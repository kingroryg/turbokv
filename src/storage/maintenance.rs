//! Shared bounded health state for flush and compaction maintenance.

use std::fmt;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use parking_lot::Mutex;

use crate::core::{
    MaintenanceFailure, MaintenanceOperationStatus, MaintenanceOrigin, MaintenanceStatus,
};

const MAX_FAILURE_MESSAGE_BYTES: usize = 512;

#[derive(Clone, Copy)]
pub(super) enum MaintenanceOperation {
    Flush,
    Compaction,
}

#[derive(Default)]
struct OperationHealth {
    attempts_since_open: AtomicU64,
    failures_since_open: AtomicU64,
    background_failures_since_open: AtomicU64,
    successful_retries_since_open: AtomicU64,
    unresolved_failure: Mutex<Option<UnresolvedFailure>>,
}

struct UnresolvedFailure {
    latest_failed_attempt_sequence: u64,
    public: MaintenanceFailure,
}

impl OperationHealth {
    fn start_attempt(&self) -> u64 {
        self.attempts_since_open
            .fetch_add(1, Ordering::Relaxed)
            .saturating_add(1)
    }

    fn record_failure(
        &self,
        attempt_sequence: u64,
        origin: MaintenanceOrigin,
        error: &dyn fmt::Display,
    ) {
        let mut unresolved = self.unresolved_failure.lock();
        let sequence_since_open = self
            .failures_since_open
            .fetch_add(1, Ordering::Relaxed)
            .saturating_add(1);
        if origin == MaintenanceOrigin::Background {
            self.background_failures_since_open
                .fetch_add(1, Ordering::Relaxed);
        }
        if let Some(failure) = unresolved.as_mut() {
            failure.latest_failed_attempt_sequence =
                failure.latest_failed_attempt_sequence.max(attempt_sequence);
        } else {
            let (message, message_truncated) = bounded_message(error);
            *unresolved = Some(UnresolvedFailure {
                latest_failed_attempt_sequence: attempt_sequence,
                public: MaintenanceFailure {
                    sequence_since_open,
                    origin,
                    message,
                    message_truncated,
                },
            });
        }
    }

    fn record_success(&self, attempt_sequence: u64, retry_work_resolved: bool) {
        if !retry_work_resolved {
            return;
        }
        let mut unresolved = self.unresolved_failure.lock();
        if unresolved
            .as_ref()
            .is_some_and(|failure| failure.latest_failed_attempt_sequence <= attempt_sequence)
        {
            unresolved.take();
            self.successful_retries_since_open
                .fetch_add(1, Ordering::Relaxed);
        }
    }

    fn status(&self) -> MaintenanceOperationStatus {
        let unresolved = self.unresolved_failure.lock();
        let unresolved_failure = unresolved.as_ref().map(|failure| failure.public.clone());
        MaintenanceOperationStatus {
            retry_pending: unresolved_failure.is_some(),
            failures_since_open: self.failures_since_open.load(Ordering::Relaxed),
            background_failures_since_open: self
                .background_failures_since_open
                .load(Ordering::Relaxed),
            successful_retries_since_open: self
                .successful_retries_since_open
                .load(Ordering::Relaxed),
            unresolved_failure,
        }
    }
}

#[derive(Default)]
pub(super) struct MaintenanceHealth {
    flush: OperationHealth,
    compaction: OperationHealth,
}

impl MaintenanceHealth {
    pub(super) fn attempt(
        self: &Arc<Self>,
        operation: MaintenanceOperation,
        origin: MaintenanceOrigin,
    ) -> MaintenanceAttempt {
        let attempt_sequence = self.operation(operation).start_attempt();
        MaintenanceAttempt {
            health: Arc::clone(self),
            operation,
            origin,
            attempt_sequence,
            completed: false,
        }
    }

    pub(super) fn record_failure(
        &self,
        operation: MaintenanceOperation,
        origin: MaintenanceOrigin,
        error: &dyn fmt::Display,
    ) {
        let operation_health = self.operation(operation);
        let attempt_sequence = operation_health.start_attempt();
        operation_health.record_failure(attempt_sequence, origin, error);
    }

    pub(super) fn status(&self) -> MaintenanceStatus {
        MaintenanceStatus {
            flush: self.flush.status(),
            compaction: self.compaction.status(),
        }
    }

    pub(super) fn status_with_wal_failure(&self, wal_failure: Option<&str>) -> MaintenanceStatus {
        let mut status = self.status();
        if let Some(error) = wal_failure {
            let sequence_since_open = status.flush.failures_since_open.saturating_add(1);
            status.flush.retry_pending = true;
            status.flush.failures_since_open = sequence_since_open;
            if status.flush.unresolved_failure.is_none() {
                let (message, message_truncated) = bounded_message(&error);
                status.flush.unresolved_failure = Some(MaintenanceFailure {
                    sequence_since_open,
                    origin: MaintenanceOrigin::Foreground,
                    message,
                    message_truncated,
                });
            }
        }
        status
    }

    pub(super) fn retry_pending(&self, operation: MaintenanceOperation) -> bool {
        self.operation(operation)
            .unresolved_failure
            .lock()
            .is_some()
    }

    fn operation(&self, operation: MaintenanceOperation) -> &OperationHealth {
        match operation {
            MaintenanceOperation::Flush => &self.flush,
            MaintenanceOperation::Compaction => &self.compaction,
        }
    }
}

/// Cancellation records a bounded unresolved failure. Callers complete the
/// guard with their result so a later successful operation clears that slot.
pub(super) struct MaintenanceAttempt {
    health: Arc<MaintenanceHealth>,
    operation: MaintenanceOperation,
    origin: MaintenanceOrigin,
    attempt_sequence: u64,
    completed: bool,
}

impl MaintenanceAttempt {
    pub(super) fn finish<T, E: fmt::Display>(
        &mut self,
        result: &Result<T, E>,
        retry_work_resolved: bool,
    ) {
        match result {
            Ok(_) => self
                .health
                .operation(self.operation)
                .record_success(self.attempt_sequence, retry_work_resolved),
            Err(error) => self.health.operation(self.operation).record_failure(
                self.attempt_sequence,
                self.origin,
                error,
            ),
        }
        self.completed = true;
    }
}

impl Drop for MaintenanceAttempt {
    fn drop(&mut self) {
        if !self.completed {
            self.health.operation(self.operation).record_failure(
                self.attempt_sequence,
                self.origin,
                &"maintenance attempt ended before completion (cancelled or panicked)",
            );
        }
    }
}

fn bounded_message(error: &dyn fmt::Display) -> (String, bool) {
    struct BoundedWriter {
        message: String,
        truncated: bool,
    }

    impl fmt::Write for BoundedWriter {
        fn write_str(&mut self, text: &str) -> fmt::Result {
            if self.truncated {
                return Ok(());
            }
            let remaining = MAX_FAILURE_MESSAGE_BYTES.saturating_sub(self.message.len());
            if text.len() <= remaining {
                self.message.push_str(text);
                return Ok(());
            }

            let mut boundary = remaining;
            while !text.is_char_boundary(boundary) {
                boundary -= 1;
            }
            self.message.push_str(&text[..boundary]);
            self.truncated = true;
            Ok(())
        }
    }

    let mut writer = BoundedWriter {
        message: String::with_capacity(MAX_FAILURE_MESSAGE_BYTES),
        truncated: false,
    };
    fmt::write(&mut writer, format_args!("{error}"))
        .expect("bounded failure-message writer cannot fail");
    (writer.message, writer.truncated)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn retained_failure_detail_is_bounded_at_a_utf8_boundary() {
        let health = MaintenanceHealth::default();
        let message = "é".repeat(MAX_FAILURE_MESSAGE_BYTES);
        health.record_failure(
            MaintenanceOperation::Flush,
            MaintenanceOrigin::Background,
            &message,
        );

        let status = health.status();
        let failure = status.flush.unresolved_failure.as_ref().unwrap();
        assert_eq!(failure.message.len(), MAX_FAILURE_MESSAGE_BYTES);
        assert!(failure.message.is_char_boundary(failure.message.len()));
        assert!(failure.message_truncated);
    }

    #[test]
    fn wal_poison_counts_without_replacing_the_first_flush_failure() {
        let health = MaintenanceHealth::default();
        health.record_failure(
            MaintenanceOperation::Flush,
            MaintenanceOrigin::Background,
            &"first background flush failure",
        );

        let status = health.status_with_wal_failure(Some("later WAL data sync failure"));
        assert!(status.flush.retry_pending);
        assert_eq!(status.flush.failures_since_open, 2);
        assert_eq!(status.flush.background_failures_since_open, 1);
        let failure = status.flush.unresolved_failure.as_ref().unwrap();
        assert_eq!(failure.sequence_since_open, 1);
        assert_eq!(failure.origin, MaintenanceOrigin::Background);
        assert_eq!(failure.message, "first background flush failure");

        assert_eq!(
            health.status_with_wal_failure(Some("later WAL data sync failure")),
            status
        );
    }

    #[test]
    fn earlier_success_cannot_clear_a_later_failure_or_replace_original_detail() {
        let health = Arc::new(MaintenanceHealth::default());
        let mut original = health.attempt(
            MaintenanceOperation::Compaction,
            MaintenanceOrigin::Background,
        );
        original.finish::<(), _>(&Err("original failure"), false);
        let mut earlier_success = health.attempt(
            MaintenanceOperation::Compaction,
            MaintenanceOrigin::Background,
        );
        let mut later_failure = health.attempt(
            MaintenanceOperation::Compaction,
            MaintenanceOrigin::Background,
        );
        later_failure.finish::<(), _>(&Err("later failure"), false);

        earlier_success.finish::<_, &str>(&Ok(()), true);
        let pending = health.status().compaction;
        assert!(pending.retry_pending);
        assert_eq!(pending.failures_since_open, 2);
        assert_eq!(
            pending.unresolved_failure.unwrap().message,
            "original failure"
        );

        let mut proven_retry = health.attempt(
            MaintenanceOperation::Compaction,
            MaintenanceOrigin::Background,
        );
        proven_retry.finish::<_, &str>(&Ok(()), true);
        assert!(!health.status().compaction.retry_pending);
    }
}

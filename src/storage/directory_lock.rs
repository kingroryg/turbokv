use std::collections::HashSet;
use std::fs::{File, OpenOptions};
use std::io;
use std::path::{Path, PathBuf};
use std::sync::{Mutex, MutexGuard, OnceLock};

const LOCK_FILE_NAME: &str = ".turbokv.lock";
pub(crate) const LOCKED_DIRECTORY_GUIDANCE: &str =
    "close or drop the existing TurboKV database before reopening it; shared multi-writer access is unsupported";

static OPEN_DIRECTORIES: OnceLock<Mutex<HashSet<PathBuf>>> = OnceLock::new();

pub(crate) enum AcquireError {
    Locked { path: PathBuf },
    Io { path: PathBuf, source: io::Error },
}

/// Process- and system-wide exclusive ownership of a database directory.
///
/// The lock file remains in place after release. Removing it could allow a new
/// inode to be locked while an existing owner still holds the old inode.
pub(crate) struct DirectoryLock {
    canonical_path: PathBuf,
    file: File,
}

impl DirectoryLock {
    pub(crate) fn acquire(data_dir: &Path) -> Result<Self, AcquireError> {
        let canonical_path =
            std::fs::canonicalize(data_dir).map_err(|source| AcquireError::Io {
                path: data_dir.to_path_buf(),
                source,
            })?;
        let lock_path = canonical_path.join(LOCK_FILE_NAME);
        let mut open_directories = lock_open_directories();

        if open_directories.contains(&canonical_path) {
            return Err(AcquireError::Locked {
                path: canonical_path,
            });
        }

        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(&lock_path)
            .map_err(|source| AcquireError::Io {
                path: lock_path.clone(),
                source,
            })?;

        match try_lock(&file).map_err(|source| AcquireError::Io {
            path: lock_path,
            source,
        })? {
            LockAttempt::Acquired => {
                open_directories.insert(canonical_path.clone());
                Ok(Self {
                    canonical_path,
                    file,
                })
            }
            LockAttempt::Contended => Err(AcquireError::Locked {
                path: canonical_path,
            }),
        }
    }

    pub(crate) fn path(&self) -> &Path {
        &self.canonical_path
    }
}

impl Drop for DirectoryLock {
    fn drop(&mut self) {
        // Serialize the OS unlock with registry updates so another opener in
        // this process cannot observe only half of the release.
        let mut open_directories = lock_open_directories();
        unlock(&self.file);
        open_directories.remove(&self.canonical_path);
    }
}

fn lock_open_directories() -> MutexGuard<'static, HashSet<PathBuf>> {
    OPEN_DIRECTORIES
        .get_or_init(|| Mutex::new(HashSet::new()))
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

enum LockAttempt {
    Acquired,
    Contended,
}

#[cfg(unix)]
fn try_lock(file: &File) -> io::Result<LockAttempt> {
    use std::os::fd::AsRawFd;

    // SAFETY: `file` owns a valid descriptor for the duration of this call.
    let result = unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) };
    if result == 0 {
        return Ok(LockAttempt::Acquired);
    }

    let error = io::Error::last_os_error();
    if error.kind() == io::ErrorKind::WouldBlock {
        Ok(LockAttempt::Contended)
    } else {
        Err(error)
    }
}

#[cfg(unix)]
fn unlock(file: &File) {
    use std::os::fd::AsRawFd;

    // SAFETY: `file` owns a valid descriptor for the duration of this call.
    let _ = unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_UN) };
}

#[cfg(windows)]
fn try_lock(file: &File) -> io::Result<LockAttempt> {
    use std::os::windows::io::AsRawHandle;

    use windows_sys::Win32::Foundation::ERROR_LOCK_VIOLATION;
    use windows_sys::Win32::Storage::FileSystem::LockFile;

    // SAFETY: `file` owns a valid handle for the duration of this call. Windows
    // permits locking a byte beyond the current end of an empty file.
    let result = unsafe { LockFile(file.as_raw_handle(), 0, 0, 1, 0) };
    if result != 0 {
        return Ok(LockAttempt::Acquired);
    }

    let error = io::Error::last_os_error();
    if error.raw_os_error() == Some(ERROR_LOCK_VIOLATION as i32) {
        Ok(LockAttempt::Contended)
    } else {
        Err(error)
    }
}

#[cfg(windows)]
fn unlock(file: &File) {
    use std::os::windows::io::AsRawHandle;

    use windows_sys::Win32::Storage::FileSystem::UnlockFile;

    // SAFETY: this unlocks the exact byte range acquired by `try_lock` while
    // `file` still owns the handle.
    let _ = unsafe { UnlockFile(file.as_raw_handle(), 0, 0, 1, 0) };
}

#[cfg(not(any(unix, windows)))]
fn try_lock(_file: &File) -> io::Result<LockAttempt> {
    Err(io::Error::new(
        io::ErrorKind::Unsupported,
        "exclusive database-directory locking is supported only on Unix and Windows",
    ))
}

#[cfg(not(any(unix, windows)))]
fn unlock(_file: &File) {}

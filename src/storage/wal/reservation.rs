//! Platform-specific physical space reservation for mapped WAL segments.

use std::fs::File;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum AllocationResult {
    Reserved,
    Unsupported,
}

#[cfg(target_os = "linux")]
pub(crate) fn reserve_file_space(file: &File, target: u64) -> std::io::Result<AllocationResult> {
    use std::os::fd::AsRawFd;

    let length = i64::try_from(target).map_err(|_| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "WAL capacity exceeds off_t",
        )
    })?;
    // SAFETY: `file` owns a valid descriptor and the requested range is
    // represented by the checked nonnegative `off_t` length.
    let fallocate_result = unsafe { libc::fallocate(file.as_raw_fd(), 0, 0, length) };
    if fallocate_result == 0 {
        return Ok(AllocationResult::Reserved);
    }
    let fallocate_error = std::io::Error::last_os_error();
    if !allocation_api_unsupported(&fallocate_error) {
        return Err(fallocate_error);
    }

    // `posix_fallocate` returns an error number directly rather than setting
    // errno. It is the portable reserving fallback where `fallocate` is absent.
    // SAFETY: descriptor and checked range are valid for the duration of the call.
    let error = unsafe { libc::posix_fallocate(file.as_raw_fd(), 0, length) };
    if error == 0 {
        Ok(AllocationResult::Reserved)
    } else {
        let error = std::io::Error::from_raw_os_error(error);
        if allocation_api_unsupported(&error) {
            Ok(AllocationResult::Unsupported)
        } else {
            Err(error)
        }
    }
}

#[cfg(target_os = "macos")]
pub(crate) fn reserve_file_space(file: &File, target: u64) -> std::io::Result<AllocationResult> {
    use std::os::fd::AsRawFd;

    let current = file.metadata()?.len();
    if target <= current {
        return Ok(AllocationResult::Reserved);
    }
    let length = i64::try_from(target - current).map_err(|_| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "WAL allocation exceeds off_t",
        )
    })?;
    let mut remaining = length;
    for flags in [libc::F_ALLOCATECONTIG, libc::F_ALLOCATEALL] {
        let mut store = libc::fstore_t {
            fst_flags: flags,
            fst_posmode: libc::F_PEOFPOSMODE,
            fst_offset: 0,
            fst_length: remaining,
            fst_bytesalloc: 0,
        };
        // SAFETY: `file` owns a valid descriptor and `store` remains writable
        // for the duration of this `F_PREALLOCATE` call.
        let result = unsafe { libc::fcntl(file.as_raw_fd(), libc::F_PREALLOCATE, &mut store) };
        if result == 0 {
            let allocated = store.fst_bytesalloc.max(0);
            if allocated >= remaining {
                return Ok(AllocationResult::Reserved);
            }
            remaining -= allocated;
            if flags == libc::F_ALLOCATECONTIG {
                continue;
            }
            return Err(std::io::Error::from_raw_os_error(libc::ENOSPC));
        }
        let error = std::io::Error::last_os_error();
        if flags == libc::F_ALLOCATECONTIG
            && (error.raw_os_error() == Some(libc::ENOSPC) || allocation_api_unsupported(&error))
        {
            continue;
        }
        if allocation_api_unsupported(&error) {
            return Ok(AllocationResult::Unsupported);
        }
        return Err(error);
    }
    Err(std::io::Error::from_raw_os_error(libc::ENOSPC))
}

#[cfg(windows)]
pub(crate) fn reserve_file_space(file: &File, target: u64) -> std::io::Result<AllocationResult> {
    use std::mem::size_of;
    use std::os::windows::io::AsRawHandle;
    use windows_sys::Win32::Storage::FileSystem::{
        FileAllocationInfo, SetFileInformationByHandle, FILE_ALLOCATION_INFO,
    };

    let allocation_size = i64::try_from(target).map_err(|_| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "WAL allocation exceeds Windows file size",
        )
    })?;
    let information = FILE_ALLOCATION_INFO {
        AllocationSize: allocation_size,
    };
    // SAFETY: `file` owns a valid handle and `information` has the exact layout
    // required by `FileAllocationInfo` for the duration of this call.
    let result = unsafe {
        SetFileInformationByHandle(
            file.as_raw_handle(),
            FileAllocationInfo,
            std::ptr::from_ref(&information).cast(),
            size_of::<FILE_ALLOCATION_INFO>() as u32,
        )
    };
    if result != 0 {
        return Ok(AllocationResult::Reserved);
    }
    let error = std::io::Error::last_os_error();
    if allocation_api_unsupported(&error) {
        Ok(AllocationResult::Unsupported)
    } else {
        Err(error)
    }
}

#[cfg(not(any(target_os = "linux", target_os = "macos", windows)))]
pub(crate) fn reserve_file_space(_file: &File, _target: u64) -> std::io::Result<AllocationResult> {
    Ok(AllocationResult::Unsupported)
}

#[cfg(unix)]
pub(crate) fn allocation_api_unsupported(error: &std::io::Error) -> bool {
    matches!(
        error.raw_os_error(),
        Some(code)
            if code == libc::EOPNOTSUPP
                || code == libc::ENOSYS
                || code == libc::EINVAL
                || code == libc::ENODEV
    )
}

#[cfg(windows)]
pub(crate) fn allocation_api_unsupported(error: &std::io::Error) -> bool {
    use windows_sys::Win32::Foundation::{
        ERROR_INVALID_FUNCTION, ERROR_INVALID_PARAMETER, ERROR_NOT_SUPPORTED,
    };
    matches!(
        error.raw_os_error(),
        Some(code)
            if code == ERROR_INVALID_FUNCTION as i32
                || code == ERROR_INVALID_PARAMETER as i32
                || code == ERROR_NOT_SUPPORTED as i32
    )
}

#[cfg(not(any(unix, windows)))]
pub(crate) fn allocation_api_unsupported(_error: &std::io::Error) -> bool {
    true
}

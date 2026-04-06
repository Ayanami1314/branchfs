use fuser::{KernelConfig, MountOption, ReplyEmpty, ReplyOpen};
use std::fs::File;
use std::sync::atomic::{AtomicU64, Ordering};

pub const FS_IOC_BRANCH_CREATE: u32 = 0x8080_6200; // _IOR('b', 0, [u8; 128])
pub const FS_IOC_BRANCH_COMMIT: u32 = 0x0000_6201; // _IO ('b', 1)
pub const FS_IOC_BRANCH_ABORT: u32 = 0x0000_6202; // _IO ('b', 2)

pub fn get_mount_options() -> Vec<MountOption> {
    vec![MountOption::DefaultPermissions]
}

pub fn setup_capabilities(_config: &mut KernelConfig, passthrough_enabled: &mut bool) {
    // Passthrough requires FUSE ABI >= 7.40 + kernel support.
    // When compiled with abi-7-31, always disable.
    if *passthrough_enabled {
        log::warn!("FUSE passthrough not available (ABI < 7.40), disabling");
        *passthrough_enabled = false;
    }
}

pub fn handle_access(reply: ReplyEmpty) {
    reply.error(libc::ENOSYS);
}

pub fn check_rename_flags(flags: u32) -> Result<(), i32> {
    if flags & libc::RENAME_EXCHANGE != 0 {
        return Err(libc::EINVAL);
    }
    Ok(())
}

pub fn check_rename_noreplace(flags: u32) -> bool {
    flags & libc::RENAME_NOREPLACE != 0
}

pub fn ctl_file_size(ino: u64) -> u64 {
    if ino == crate::fs::CTL_INO {
        256
    } else {
        0
    }
}

pub struct PassthroughState {
    pub next_fh: AtomicU64,
}

impl Default for PassthroughState {
    fn default() -> Self {
        Self {
            next_fh: AtomicU64::new(1),
        }
    }
}

impl PassthroughState {
    pub fn new() -> Self {
        Self::default()
    }
}

pub fn try_open_passthrough(_state: &mut PassthroughState, _file: File, reply: ReplyOpen) {
    // No passthrough support with abi-7-31, fall back to normal open
    reply.opened(0, 0);
}

pub fn release_passthrough(_state: &mut PassthroughState, _fh: u64) {
    // no-op without passthrough
}

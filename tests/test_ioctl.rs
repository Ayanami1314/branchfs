//! Integration tests for FS_IOC_BRANCH_* ioctls.
//!
//! These tests require FUSE support and are ignored by default.
//! Run with: cargo test --test test_ioctl -- --ignored

use std::fs;
use std::os::unix::io::AsRawFd;
use std::path::PathBuf;
use std::process::{Command, Stdio};
use std::thread;
use std::time::Duration;

use branchfs::{FS_IOC_BRANCH_ABORT, FS_IOC_BRANCH_COMMIT, FS_IOC_BRANCH_CREATE};

/// Call a data-carrying ioctl with a 128-byte buffer.
/// `input` is the null-terminated string to send.
/// Returns (ret, output_buf) where ret is the raw ioctl return value.
unsafe fn branch_ioctl_buf(fd: i32, cmd: u32, input: &str) -> (i32, [u8; 128]) {
    let mut buf = [0u8; 128];
    let bytes = input.as_bytes();
    let len = bytes.len().min(127);
    buf[..len].copy_from_slice(&bytes[..len]);
    let ret = libc::ioctl(fd, cmd as libc::c_ulong, buf.as_mut_ptr());
    (ret, buf)
}

/// Helper: CREATE a branch from `parent`, returning the new branch name.
unsafe fn ioctl_create(fd: i32, parent: &str) -> Result<String, i32> {
    let (ret, buf) = branch_ioctl_buf(fd, FS_IOC_BRANCH_CREATE, parent);
    if ret < 0 {
        return Err(*libc::__errno_location());
    }
    let end = buf.iter().position(|&b| b == 0).unwrap_or(buf.len());
    Ok(String::from_utf8_lossy(&buf[..end]).to_string())
}

/// Helper: COMMIT a branch by name.
unsafe fn ioctl_commit(fd: i32, branch: &str) -> i32 {
    let (ret, _) = branch_ioctl_buf(fd, FS_IOC_BRANCH_COMMIT, branch);
    ret
}

/// Helper: ABORT a branch by name.
unsafe fn ioctl_abort(fd: i32, branch: &str) -> i32 {
    let (ret, _) = branch_ioctl_buf(fd, FS_IOC_BRANCH_ABORT, branch);
    ret
}

struct TestFixture {
    base: PathBuf,
    storage: PathBuf,
    mnt: PathBuf,
    branchfs_bin: PathBuf,
}

impl TestFixture {
    fn new(name: &str) -> Self {
        let id = std::process::id();
        let prefix = format!("/tmp/branchfs_ioctl_{}_{}", name, id);
        let base = PathBuf::from(format!("{}_base", prefix));
        let storage = PathBuf::from(format!("{}_storage", prefix));
        let mnt = PathBuf::from(format!("{}_mnt", prefix));

        // Clean up leftovers from a previous failed run
        let _ = Command::new("fusermount3")
            .args(["-u", mnt.to_str().unwrap()])
            .stderr(Stdio::null())
            .status();
        let _ = fs::remove_dir_all(&base);
        let _ = fs::remove_dir_all(&storage);
        let _ = fs::remove_dir_all(&mnt);

        fs::create_dir_all(&base).expect("create base dir");
        fs::create_dir_all(&storage).expect("create storage dir");
        fs::create_dir_all(&mnt).expect("create mount dir");

        // Seed base directory with initial files
        fs::write(base.join("file1.txt"), "base content\n").unwrap();
        fs::write(base.join("file2.txt"), "another file\n").unwrap();

        let branchfs_bin = PathBuf::from(env!("CARGO_BIN_EXE_branchfs"));

        Self {
            base,
            storage,
            mnt,
            branchfs_bin,
        }
    }

    fn mount(&self) {
        let status = Command::new(&self.branchfs_bin)
            .args([
                "mount",
                "--base",
                self.base.to_str().unwrap(),
                "--storage",
                self.storage.to_str().unwrap(),
                self.mnt.to_str().unwrap(),
            ])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status()
            .expect("failed to run branchfs mount");

        assert!(status.success(), "mount command failed");
        thread::sleep(Duration::from_millis(500));
        assert!(
            self.mnt.join(".branchfs_ctl").exists(),
            ".branchfs_ctl should exist after mount"
        );
    }

    fn unmount(&self) {
        let _ = Command::new(&self.branchfs_bin)
            .args([
                "unmount",
                self.mnt.to_str().unwrap(),
                "--storage",
                self.storage.to_str().unwrap(),
            ])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status();
        thread::sleep(Duration::from_millis(300));
    }

    /// Open the control file; caller must keep the returned `File` alive.
    fn open_ctl(&self) -> fs::File {
        fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(self.mnt.join(".branchfs_ctl"))
            .expect("open .branchfs_ctl")
    }
}

impl Drop for TestFixture {
    fn drop(&mut self) {
        self.unmount();
        let _ = fs::remove_dir_all(&self.base);
        let _ = fs::remove_dir_all(&self.storage);
        let _ = fs::remove_dir_all(&self.mnt);
    }
}

// ── CREATE + COMMIT ─────────────────────────────────────────────────

#[test]
#[ignore]
fn test_ioctl_create_and_commit_new_file() {
    let fix = TestFixture::new("create_commit");
    fix.mount();
    let ctl = fix.open_ctl();
    let fd = ctl.as_raw_fd();

    // CREATE a branch from main
    let branch = unsafe { ioctl_create(fd, "main") }.expect("CREATE should succeed");
    assert!(!branch.is_empty(), "branch name should not be empty");

    // Write a new file on the branch via @branch virtual path
    let branch_dir = fix.mnt.join(format!("@{}", branch));
    fs::write(branch_dir.join("new_file.txt"), "branch content\n").unwrap();
    assert!(branch_dir.join("new_file.txt").exists());

    // Base should NOT have the file yet
    assert!(
        !fix.base.join("new_file.txt").exists(),
        "new file must not appear in base before commit"
    );

    // COMMIT the branch
    let ret = unsafe { ioctl_commit(fd, &branch) };
    assert_eq!(ret, 0, "COMMIT should succeed");

    // File should now be in base
    assert!(
        fix.base.join("new_file.txt").exists(),
        "new file should be in base after commit"
    );
    assert_eq!(
        fs::read_to_string(fix.base.join("new_file.txt")).unwrap(),
        "branch content\n"
    );
}

// ── CREATE + modify existing file + COMMIT ──────────────────────────

#[test]
#[ignore]
fn test_ioctl_modify_existing_and_commit() {
    let fix = TestFixture::new("modify_commit");
    fix.mount();
    let ctl = fix.open_ctl();
    let fd = ctl.as_raw_fd();

    let branch = unsafe { ioctl_create(fd, "main") }.expect("CREATE should succeed");

    // Overwrite an existing base file via @branch path
    let branch_dir = fix.mnt.join(format!("@{}", branch));
    fs::write(branch_dir.join("file1.txt"), "modified\n").unwrap();

    // Base still has the original
    assert_eq!(
        fs::read_to_string(fix.base.join("file1.txt")).unwrap(),
        "base content\n"
    );

    let ret = unsafe { ioctl_commit(fd, &branch) };
    assert_eq!(ret, 0);

    // Base should reflect the modification
    assert_eq!(
        fs::read_to_string(fix.base.join("file1.txt")).unwrap(),
        "modified\n"
    );
}

// ── CREATE + ABORT ──────────────────────────────────────────────────

#[test]
#[ignore]
fn test_ioctl_create_and_abort() {
    let fix = TestFixture::new("create_abort");
    fix.mount();
    let ctl = fix.open_ctl();
    let fd = ctl.as_raw_fd();

    let branch = unsafe { ioctl_create(fd, "main") }.expect("CREATE should succeed");

    // Write a file and modify another via @branch path
    let branch_dir = fix.mnt.join(format!("@{}", branch));
    fs::write(branch_dir.join("abort_file.txt"), "will be discarded\n").unwrap();
    fs::write(branch_dir.join("file1.txt"), "modified\n").unwrap();

    let ret = unsafe { ioctl_abort(fd, &branch) };
    assert_eq!(ret, 0, "ABORT should succeed");

    // Branch dir should be gone after abort
    assert!(
        !branch_dir.join("abort_file.txt").exists(),
        "new file should vanish after abort"
    );
    // Base untouched
    assert!(!fix.base.join("abort_file.txt").exists());
    assert_eq!(
        fs::read_to_string(fix.base.join("file1.txt")).unwrap(),
        "base content\n"
    );
}

// ── Nested CREATE + COMMIT chain ────────────────────────────────────

#[test]
#[ignore]
fn test_ioctl_nested_create_and_commit() {
    let fix = TestFixture::new("nested");
    fix.mount();
    let ctl = fix.open_ctl();
    let fd = ctl.as_raw_fd();

    // First branch (child of main)
    let branch1 = unsafe { ioctl_create(fd, "main") }.expect("first CREATE should succeed");
    let b1_dir = fix.mnt.join(format!("@{}", branch1));
    fs::write(b1_dir.join("level1.txt"), "level1\n").unwrap();

    // Second branch (child of first)
    let branch2 = unsafe { ioctl_create(fd, &branch1) }.expect("second CREATE should succeed");
    let b2_dir = fix.mnt.join(format!("@{}", branch2));
    fs::write(b2_dir.join("level2.txt"), "level2\n").unwrap();

    // Files visible through their respective branch paths
    assert!(b1_dir.join("level1.txt").exists());
    assert!(b2_dir.join("level2.txt").exists());
    // level2 branch should also see level1 (inherited from parent)
    assert!(b2_dir.join("level1.txt").exists());

    // COMMIT level-2 → merges into level-1
    let ret = unsafe { ioctl_commit(fd, &branch2) };
    assert_eq!(ret, 0, "commit level-2 should succeed");

    // Neither file should be in base yet
    assert!(!fix.base.join("level1.txt").exists());
    assert!(!fix.base.join("level2.txt").exists());

    // COMMIT level-1 → merges into main / base
    let ret = unsafe { ioctl_commit(fd, &branch1) };
    assert_eq!(ret, 0, "commit level-1 should succeed");

    // Both files in base now
    assert!(fix.base.join("level1.txt").exists());
    assert!(fix.base.join("level2.txt").exists());
}

// ── COMMIT / ABORT on main should fail ──────────────────────────────

#[test]
#[ignore]
fn test_ioctl_commit_on_main_fails() {
    let fix = TestFixture::new("commit_main");
    fix.mount();
    let ctl = fix.open_ctl();
    let fd = ctl.as_raw_fd();

    let ret = unsafe { ioctl_commit(fd, "main") };
    assert!(ret < 0, "COMMIT on main should fail (got {})", ret);
}

#[test]
#[ignore]
fn test_ioctl_abort_on_main_fails() {
    let fix = TestFixture::new("abort_main");
    fix.mount();
    let ctl = fix.open_ctl();
    let fd = ctl.as_raw_fd();

    let ret = unsafe { ioctl_abort(fd, "main") };
    assert!(ret < 0, "ABORT on main should fail (got {})", ret);
}

// ── Multiple CREATE + ABORT discards child branch ───────────────────

#[test]
#[ignore]
fn test_ioctl_abort_returns_to_parent_branch() {
    let fix = TestFixture::new("abort_parent");
    fix.mount();
    let ctl = fix.open_ctl();
    let fd = ctl.as_raw_fd();

    // CREATE first branch, write a file
    let branch1 = unsafe { ioctl_create(fd, "main") }.expect("CREATE should succeed");
    let b1_dir = fix.mnt.join(format!("@{}", branch1));
    fs::write(b1_dir.join("parent_file.txt"), "parent\n").unwrap();

    // CREATE second (nested) branch, write another file
    let branch2 = unsafe { ioctl_create(fd, &branch1) }.expect("CREATE should succeed");
    let b2_dir = fix.mnt.join(format!("@{}", branch2));
    fs::write(b2_dir.join("child_file.txt"), "child\n").unwrap();

    // ABORT second branch — child's data is discarded
    let ret = unsafe { ioctl_abort(fd, &branch2) };
    assert_eq!(ret, 0);

    // Parent branch still has its file
    assert!(b1_dir.join("parent_file.txt").exists());

    // COMMIT first branch back to main
    let ret = unsafe { ioctl_commit(fd, &branch1) };
    assert_eq!(ret, 0);

    assert!(fix.base.join("parent_file.txt").exists());
    assert!(!fix.base.join("child_file.txt").exists());
}

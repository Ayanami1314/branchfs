//! Integration tests for core filesystem operations including rename.
//!
//! These tests require FUSE support and are ignored by default.
//! Run with: cargo test --test test_integration -- --ignored

use std::fs;
use std::os::unix::io::AsRawFd;
use std::path::PathBuf;
use std::process::{Command, Stdio};
use std::thread;
use std::time::Duration;

use branchfs::{FS_IOC_BRANCH_ABORT, FS_IOC_BRANCH_COMMIT, FS_IOC_BRANCH_CREATE};

/// Helper: CREATE a branch. Returns the new branch name.
unsafe fn ioctl_create(fd: i32) -> Result<String, i32> {
    let mut buf = [0u8; 128];
    let ret = libc::ioctl(fd, FS_IOC_BRANCH_CREATE as libc::c_ulong, buf.as_mut_ptr());
    if ret < 0 {
        return Err(*libc::__errno_location());
    }
    let end = buf.iter().position(|&b| b == 0).unwrap_or(buf.len());
    Ok(String::from_utf8_lossy(&buf[..end]).to_string())
}

/// Helper: COMMIT the branch identified by the ctl fd's inode.
unsafe fn ioctl_commit(fd: i32) -> i32 {
    libc::ioctl(fd, FS_IOC_BRANCH_COMMIT as libc::c_ulong)
}

/// Helper: ABORT the branch identified by the ctl fd's inode.
unsafe fn ioctl_abort(fd: i32) -> i32 {
    libc::ioctl(fd, FS_IOC_BRANCH_ABORT as libc::c_ulong)
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
        let prefix = format!("/tmp/branchfs_integ_{}_{}", name, id);
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

        // Seed base directory
        fs::write(base.join("file1.txt"), "base content\n").unwrap();
        fs::write(base.join("file2.txt"), "another file\n").unwrap();
        fs::create_dir_all(base.join("subdir")).unwrap();
        fs::write(base.join("subdir").join("nested.txt"), "nested content\n").unwrap();

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

    fn open_ctl(&self) -> fs::File {
        fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(self.mnt.join(".branchfs_ctl"))
            .expect("open .branchfs_ctl")
    }

    fn open_branch_ctl(&self, branch: &str) -> fs::File {
        let path = self.mnt.join(format!("@{}", branch)).join(".branchfs_ctl");
        fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(&path)
            .unwrap_or_else(|e| panic!("open branch ctl for '{}' at {:?}: {}", branch, path, e))
    }

    fn branch_dir(&self, branch: &str) -> PathBuf {
        self.mnt.join(format!("@{}", branch))
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

// ── Rename tests ────────────────────────────────────────────────────

#[test]
#[ignore]
fn test_rename_file_same_dir() {
    let fix = TestFixture::new("rename_same");
    fix.mount();
    let ctl = fix.open_ctl();

    let branch = unsafe { ioctl_create(ctl.as_raw_fd()) }.expect("CREATE");
    let bdir = fix.branch_dir(&branch);

    // Rename a base file within the same directory
    fs::rename(bdir.join("file1.txt"), bdir.join("file1_renamed.txt")).expect("rename should work");

    assert!(!bdir.join("file1.txt").exists(), "old name should be gone");
    assert!(
        bdir.join("file1_renamed.txt").exists(),
        "new name should exist"
    );
    assert_eq!(
        fs::read_to_string(bdir.join("file1_renamed.txt")).unwrap(),
        "base content\n"
    );

    // Base should be untouched
    assert!(fix.base.join("file1.txt").exists());
}

#[test]
#[ignore]
fn test_rename_file_cross_dir() {
    let fix = TestFixture::new("rename_xdir");
    fix.mount();
    let ctl = fix.open_ctl();

    let branch = unsafe { ioctl_create(ctl.as_raw_fd()) }.expect("CREATE");
    let bdir = fix.branch_dir(&branch);

    // Move file into a subdirectory
    fs::rename(
        bdir.join("file1.txt"),
        bdir.join("subdir").join("moved.txt"),
    )
    .expect("cross-dir rename");

    assert!(!bdir.join("file1.txt").exists());
    assert!(bdir.join("subdir").join("moved.txt").exists());
    assert_eq!(
        fs::read_to_string(bdir.join("subdir").join("moved.txt")).unwrap(),
        "base content\n"
    );
}

#[test]
#[ignore]
fn test_rename_overwrite_existing() {
    let fix = TestFixture::new("rename_over");
    fix.mount();
    let ctl = fix.open_ctl();

    let branch = unsafe { ioctl_create(ctl.as_raw_fd()) }.expect("CREATE");
    let bdir = fix.branch_dir(&branch);

    // Create a new file, then rename it to overwrite file2.txt
    fs::write(bdir.join("new.txt"), "new content\n").unwrap();
    fs::rename(bdir.join("new.txt"), bdir.join("file2.txt")).expect("rename overwrite");

    assert!(!bdir.join("new.txt").exists());
    assert_eq!(
        fs::read_to_string(bdir.join("file2.txt")).unwrap(),
        "new content\n"
    );
}

#[test]
#[ignore]
fn test_rename_in_branch_then_commit() {
    let fix = TestFixture::new("rename_commit");
    fix.mount();
    let ctl = fix.open_ctl();

    let branch = unsafe { ioctl_create(ctl.as_raw_fd()) }.expect("CREATE");
    let bdir = fix.branch_dir(&branch);

    fs::rename(bdir.join("file1.txt"), bdir.join("renamed.txt")).expect("rename");

    // Commit
    let bctl = fix.open_branch_ctl(&branch);
    let ret = unsafe { ioctl_commit(bctl.as_raw_fd()) };
    assert_eq!(ret, 0, "commit should succeed");

    // Base should reflect the rename
    assert!(
        fix.base.join("renamed.txt").exists(),
        "renamed file should be in base"
    );
    assert_eq!(
        fs::read_to_string(fix.base.join("renamed.txt")).unwrap(),
        "base content\n"
    );
    // The original should be gone from base (tombstone applied)
    assert!(
        !fix.base.join("file1.txt").exists(),
        "original should be deleted from base after commit"
    );
}

#[test]
#[ignore]
fn test_rename_in_branch_then_abort() {
    let fix = TestFixture::new("rename_abort");
    fix.mount();
    let ctl = fix.open_ctl();

    let branch = unsafe { ioctl_create(ctl.as_raw_fd()) }.expect("CREATE");
    let bdir = fix.branch_dir(&branch);

    fs::rename(bdir.join("file1.txt"), bdir.join("renamed.txt")).expect("rename");

    // Abort
    let bctl = fix.open_branch_ctl(&branch);
    let ret = unsafe { ioctl_abort(bctl.as_raw_fd()) };
    assert_eq!(ret, 0, "abort should succeed");

    // Base should be unchanged
    assert!(fix.base.join("file1.txt").exists());
    assert!(!fix.base.join("renamed.txt").exists());
}

#[test]
#[ignore]
fn test_rename_nonexistent_fails() {
    let fix = TestFixture::new("rename_noent");
    fix.mount();
    let ctl = fix.open_ctl();

    let branch = unsafe { ioctl_create(ctl.as_raw_fd()) }.expect("CREATE");
    let bdir = fix.branch_dir(&branch);

    let result = fs::rename(bdir.join("no_such_file.txt"), bdir.join("dest.txt"));
    assert!(result.is_err(), "rename of nonexistent file should fail");
}

// ── General filesystem integration tests ────────────────────────────

#[test]
#[ignore]
fn test_create_read_write_delete() {
    let fix = TestFixture::new("crud");
    fix.mount();
    let ctl = fix.open_ctl();

    let branch = unsafe { ioctl_create(ctl.as_raw_fd()) }.expect("CREATE");
    let bdir = fix.branch_dir(&branch);

    // Create
    fs::write(bdir.join("crud.txt"), "hello\n").unwrap();
    assert!(bdir.join("crud.txt").exists());

    // Read
    assert_eq!(
        fs::read_to_string(bdir.join("crud.txt")).unwrap(),
        "hello\n"
    );

    // Update (overwrite)
    fs::write(bdir.join("crud.txt"), "updated\n").unwrap();
    assert_eq!(
        fs::read_to_string(bdir.join("crud.txt")).unwrap(),
        "updated\n"
    );

    // Delete
    fs::remove_file(bdir.join("crud.txt")).unwrap();
    assert!(!bdir.join("crud.txt").exists());
}

#[test]
#[ignore]
fn test_branch_isolation() {
    let fix = TestFixture::new("isolation");
    fix.mount();
    let ctl = fix.open_ctl();

    // Create two sibling branches from main
    let branch_a = unsafe { ioctl_create(ctl.as_raw_fd()) }.expect("CREATE A");
    let branch_b = unsafe { ioctl_create(ctl.as_raw_fd()) }.expect("CREATE B");

    let dir_a = fix.branch_dir(&branch_a);
    let dir_b = fix.branch_dir(&branch_b);

    // Write different files in each branch
    fs::write(dir_a.join("only_a.txt"), "A\n").unwrap();
    fs::write(dir_b.join("only_b.txt"), "B\n").unwrap();

    // Each branch should only see its own file, not the sibling's
    assert!(dir_a.join("only_a.txt").exists());
    assert!(!dir_a.join("only_b.txt").exists());

    assert!(dir_b.join("only_b.txt").exists());
    assert!(!dir_b.join("only_a.txt").exists());
}

#[test]
#[ignore]
fn test_nested_branch_inheritance() {
    let fix = TestFixture::new("inherit");
    fix.mount();
    let ctl = fix.open_ctl();

    // Create parent branch, write a file
    let parent = unsafe { ioctl_create(ctl.as_raw_fd()) }.expect("CREATE parent");
    let pdir = fix.branch_dir(&parent);
    fs::write(pdir.join("parent_file.txt"), "from parent\n").unwrap();

    // Create child branch from parent
    let pctl = fix.open_branch_ctl(&parent);
    let child = unsafe { ioctl_create(pctl.as_raw_fd()) }.expect("CREATE child");
    let cdir = fix.branch_dir(&child);

    // Child should see parent's file and base files
    assert!(
        cdir.join("parent_file.txt").exists(),
        "child should inherit parent's file"
    );
    assert_eq!(
        fs::read_to_string(cdir.join("parent_file.txt")).unwrap(),
        "from parent\n"
    );
    assert!(
        cdir.join("file1.txt").exists(),
        "child should see base files"
    );
}

#[test]
#[ignore]
fn test_mkdir_and_nested_files() {
    let fix = TestFixture::new("mkdir_nested");
    fix.mount();
    let ctl = fix.open_ctl();

    let branch = unsafe { ioctl_create(ctl.as_raw_fd()) }.expect("CREATE");
    let bdir = fix.branch_dir(&branch);

    // Create nested directory structure
    fs::create_dir_all(bdir.join("a").join("b").join("c")).unwrap();
    fs::write(
        bdir.join("a").join("b").join("c").join("deep.txt"),
        "deep\n",
    )
    .unwrap();
    fs::write(bdir.join("a").join("top.txt"), "top\n").unwrap();

    assert!(bdir.join("a").join("b").join("c").join("deep.txt").exists());
    assert_eq!(
        fs::read_to_string(bdir.join("a").join("b").join("c").join("deep.txt")).unwrap(),
        "deep\n"
    );
    assert_eq!(
        fs::read_to_string(bdir.join("a").join("top.txt")).unwrap(),
        "top\n"
    );

    // None of this should be in base
    assert!(!fix.base.join("a").exists());
}

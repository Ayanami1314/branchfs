use std::fs::{self, File};
use std::io::{Read, Write};
use std::path::Path;

use crate::error::Result;

pub fn ensure_parent_dirs(path: &Path) -> Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    Ok(())
}

pub fn copy_file(src: &Path, dst: &Path) -> Result<()> {
    ensure_parent_dirs(dst)?;
    fs::copy(src, dst)?;
    Ok(())
}

/// Symlink-aware copy: if `src` is a symlink, recreate the symlink at `dst`;
/// otherwise fall back to a regular file copy.
pub fn copy_entry(src: &Path, dst: &Path) -> Result<()> {
    ensure_parent_dirs(dst)?;
    let meta = src.symlink_metadata()?;
    if meta.file_type().is_symlink() {
        let target = fs::read_link(src)?;
        // Remove any pre-existing entry at dst so symlink() won't fail
        let _ = fs::remove_file(dst);
        std::os::unix::fs::symlink(&target, dst)?;
    } else {
        fs::copy(src, dst)?;
    }
    Ok(())
}

pub fn read_file(path: &Path) -> Result<Vec<u8>> {
    let mut file = File::open(path)?;
    let mut buf = Vec::new();
    file.read_to_end(&mut buf)?;
    Ok(buf)
}

pub fn write_file(path: &Path, data: &[u8]) -> Result<()> {
    ensure_parent_dirs(path)?;
    let mut file = File::create(path)?;
    file.write_all(data)?;
    Ok(())
}

pub fn file_size(path: &Path) -> Result<u64> {
    Ok(fs::metadata(path)?.len())
}

pub fn remove_file(path: &Path) -> Result<()> {
    if path.exists() {
        fs::remove_file(path)?;
    }
    Ok(())
}

pub fn remove_dir_all(path: &Path) -> Result<()> {
    if path.exists() {
        fs::remove_dir_all(path)?;
    }
    Ok(())
}

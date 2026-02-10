use crate::fs::CTL_FILE;

/// Classified path context for an inode path.
pub(crate) enum PathContext {
    /// Virtual `@branch` directory (e.g. `/@feature-a`)
    BranchDir(String),
    /// Per-branch ctl file (e.g. `/@feature-a/.branchfs_ctl`)
    BranchCtl(String),
    /// File/dir inside a branch subtree – (branch_name, relative_path)
    BranchPath(String, String),
    /// Root's control file (`/.branchfs_ctl`)
    RootCtl,
    /// Regular path resolved via root's current branch
    RootPath(String),
}

/// Classify an inode path into a PathContext.
pub(crate) fn classify_path(path: &str) -> PathContext {
    if path == "/" {
        return PathContext::RootPath("/".to_string());
    }

    // Paths under /@branch/...
    if let Some(rest) = path.strip_prefix("/@") {
        // Find the next '/' if any
        if let Some(slash_pos) = rest.find('/') {
            let branch = &rest[..slash_pos];
            let remainder = &rest[slash_pos..]; // e.g. "/.branchfs_ctl" or "/src/main.rs"

            if remainder == format!("/{}", CTL_FILE).as_str() {
                PathContext::BranchCtl(branch.to_string())
            } else {
                PathContext::BranchPath(branch.to_string(), remainder.to_string())
            }
        } else {
            // Just "/@branch" with no trailing content
            PathContext::BranchDir(rest.to_string())
        }
    } else {
        PathContext::RootPath(path.to_string())
    }
}

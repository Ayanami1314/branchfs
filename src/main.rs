use std::io::Write;
use std::path::{Path, PathBuf};
use std::process;

use anyhow::Result;
use clap::{Parser, Subcommand};

use branchfs::daemon::{self, Request, Response};

#[derive(Parser)]
#[command(name = "branchfs")]
#[command(about = "FUSE filesystem with atomic branching")]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Mount the filesystem (always starts on main branch)
    Mount {
        /// Base directory to branch from (required on first mount)
        #[arg(long)]
        base: Option<PathBuf>,

        /// Storage directory for branch data
        #[arg(long, default_value = "/var/lib/branchfs")]
        storage: PathBuf,

        /// Enable FUSE passthrough for near-native I/O performance (requires root)
        #[arg(long)]
        passthrough: bool,

        /// Mount point
        mountpoint: PathBuf,
    },

    /// Create a new branch and switch to it
    Create {
        /// Branch name
        name: String,

        /// Mount point to switch to the new branch
        mountpoint: PathBuf,

        /// Parent branch name
        #[arg(long, short, default_value = "main")]
        parent: String,

        /// Storage directory
        #[arg(long, default_value = "/var/lib/branchfs")]
        storage: PathBuf,
    },

    /// Commit branch to base
    Commit {
        /// Mount point of the branch to commit
        mountpoint: PathBuf,

        /// Storage directory
        #[arg(long, default_value = "/var/lib/branchfs")]
        storage: PathBuf,
    },

    /// Abort branch
    Abort {
        /// Mount point of the branch to abort
        mountpoint: PathBuf,

        /// Storage directory
        #[arg(long, default_value = "/var/lib/branchfs")]
        storage: PathBuf,
    },

    /// List branches
    List {
        /// Storage directory
        #[arg(long, default_value = "/var/lib/branchfs")]
        storage: PathBuf,
    },

    /// Unmount a branch (daemon auto-exits when last mount is removed)
    Unmount {
        /// Mount point to unmount
        mountpoint: PathBuf,

        /// Storage directory
        #[arg(long, default_value = "/var/lib/branchfs")]
        storage: PathBuf,
    },
}

fn get_socket_path(storage: &Path) -> PathBuf {
    storage.join("daemon.sock")
}

fn send_request(storage: &Path, request: &Request) -> Result<Response> {
    let socket_path = get_socket_path(storage);
    daemon::send_request(&socket_path, request)
        .map_err(|e| anyhow::anyhow!("Failed to communicate with daemon: {}", e))
}

/// Get the current mount branch from the daemon.
fn get_mount_branch(storage: &Path, mountpoint: &Path) -> Result<String> {
    let resp = send_request(
        storage,
        &Request::GetMountBranch {
            mountpoint: mountpoint.to_string_lossy().to_string(),
        },
    )?;
    if !resp.ok {
        anyhow::bail!("{}", resp.error.unwrap_or_else(|| "unknown error".into()));
    }
    resp.data
        .and_then(|d| d.as_str().map(|s| s.to_string()))
        .ok_or_else(|| anyhow::anyhow!("daemon returned no branch info"))
}

/// Look up the parent of a branch from the daemon's branch list.
fn get_parent_of(storage: &Path, branch: &str) -> Result<String> {
    let resp = send_request(storage, &Request::List)?;
    if !resp.ok {
        anyhow::bail!("{}", resp.error.unwrap_or_else(|| "unknown error".into()));
    }
    if let Some(data) = resp.data {
        if let Some(branches) = data.as_array() {
            for b in branches {
                if b["name"].as_str() == Some(branch) {
                    if let Some(parent) = b["parent"].as_str() {
                        return Ok(parent.to_string());
                    }
                }
            }
        }
    }
    anyhow::bail!("Could not determine parent of branch '{}'", branch)
}

fn main() -> Result<()> {
    env_logger::init();
    let cli = Cli::parse();

    match cli.command {
        Commands::Mount {
            base,
            storage,
            passthrough,
            mountpoint,
        } => {
            if passthrough && nix::unistd::geteuid().as_raw() != 0 {
                eprintln!("Error: --passthrough requires root (CAP_SYS_ADMIN)");
                process::exit(1);
            }

            std::fs::create_dir_all(&storage)?;
            let storage = storage.canonicalize()?;

            // Canonicalize base if provided
            let base = base.map(|b| b.canonicalize()).transpose()?;

            // Ensure daemon is running (auto-start if needed)
            daemon::ensure_daemon(base.as_deref(), &storage)
                .map_err(|e| anyhow::anyhow!("{}", e))?;

            // Create mountpoint
            std::fs::create_dir_all(&mountpoint)?;
            let mountpoint = mountpoint.canonicalize()?;

            // Send mount request (always mounts main branch)
            let response = send_request(
                &storage,
                &Request::Mount {
                    branch: "main".to_string(),
                    mountpoint: mountpoint.to_string_lossy().to_string(),
                    passthrough,
                },
            )?;

            if response.ok {
                println!("Mounted at {:?}", mountpoint);
            } else {
                eprintln!("Error: {}", response.error.unwrap_or_default());
                process::exit(1);
            }
        }

        Commands::Create {
            name,
            mountpoint,
            parent,
            storage,
        } => {
            let storage = storage.canonicalize()?;
            let mountpoint = mountpoint.canonicalize()?;

            let response = send_request(
                &storage,
                &Request::Create {
                    name: name.clone(),
                    parent: parent.clone(),
                },
            )?;

            if response.ok {
                // Switch to the new branch
                let ctl_path = mountpoint.join(".branchfs_ctl");

                let mut file = std::fs::OpenOptions::new()
                    .write(true)
                    .open(&ctl_path)
                    .map_err(|e| {
                        anyhow::anyhow!(
                            "Failed to open control file (is {} mounted?): {}",
                            mountpoint.display(),
                            e
                        )
                    })?;

                file.write_all(format!("switch:{}", name).as_bytes())
                    .map_err(|e| anyhow::anyhow!("Failed to switch to branch: {}", e))?;

                // Notify daemon of the switch
                let _ = send_request(
                    &storage,
                    &Request::NotifySwitch {
                        mountpoint: mountpoint.to_string_lossy().to_string(),
                        branch: name.clone(),
                    },
                );

                println!(
                    "Created and switched to branch '{}' (parent: '{}')",
                    name, parent
                );
            } else {
                eprintln!("Error: {}", response.error.unwrap_or_default());
                process::exit(1);
            }
        }

        Commands::Commit {
            mountpoint,
            storage,
        } => {
            let mountpoint = mountpoint.canonicalize()?;
            let storage = storage.canonicalize()?;

            let branch = get_mount_branch(&storage, &mountpoint)?;
            if branch == "main" {
                anyhow::bail!("Cannot commit main branch");
            }

            // Pre-compute parent before commit (FUSE handler will switch to it)
            let parent = get_parent_of(&storage, &branch)?;

            // Write to the per-branch ctl file
            let ctl_path = mountpoint.join(format!("@{}", branch)).join(".branchfs_ctl");
            let mut file = std::fs::OpenOptions::new()
                .write(true)
                .open(&ctl_path)
                .map_err(|e| anyhow::anyhow!("Failed to open branch control file: {}", e))?;

            file.write_all(b"commit")
                .map_err(|e| anyhow::anyhow!("Commit failed: {}", e))?;

            // Notify daemon that mount switched to parent
            let _ = send_request(
                &storage,
                &Request::NotifySwitch {
                    mountpoint: mountpoint.to_string_lossy().to_string(),
                    branch: parent,
                },
            );

            println!("Committed branch at {:?}", mountpoint);
        }

        Commands::Abort {
            mountpoint,
            storage,
        } => {
            let mountpoint = mountpoint.canonicalize()?;
            let storage = storage.canonicalize()?;

            let branch = get_mount_branch(&storage, &mountpoint)?;
            if branch == "main" {
                anyhow::bail!("Cannot abort main branch");
            }

            // Pre-compute parent before abort (FUSE handler will switch to it)
            let parent = get_parent_of(&storage, &branch)?;

            // Write to the per-branch ctl file
            let ctl_path = mountpoint.join(format!("@{}", branch)).join(".branchfs_ctl");
            let mut file = std::fs::OpenOptions::new()
                .write(true)
                .open(&ctl_path)
                .map_err(|e| anyhow::anyhow!("Failed to open branch control file: {}", e))?;

            file.write_all(b"abort")
                .map_err(|e| anyhow::anyhow!("Abort failed: {}", e))?;

            // Notify daemon that mount switched to parent
            let _ = send_request(
                &storage,
                &Request::NotifySwitch {
                    mountpoint: mountpoint.to_string_lossy().to_string(),
                    branch: parent,
                },
            );

            println!("Aborted branch at {:?}", mountpoint);
        }

        Commands::List { storage } => {
            let storage = storage.canonicalize()?;

            let response = send_request(&storage, &Request::List)?;

            if response.ok {
                println!("{:<20} {:<20}", "BRANCH", "PARENT");
                println!("{:<20} {:<20}", "------", "------");

                if let Some(data) = response.data {
                    if let Some(branches) = data.as_array() {
                        for branch in branches {
                            let name = branch["name"].as_str().unwrap_or("-");
                            let parent = branch["parent"].as_str().unwrap_or("-");
                            println!("{:<20} {:<20}", name, parent);
                        }
                    }
                }
            } else {
                eprintln!("Error: {}", response.error.unwrap_or_default());
                process::exit(1);
            }
        }

        Commands::Unmount {
            mountpoint,
            storage,
        } => {
            let storage = storage.canonicalize()?;
            let mountpoint = mountpoint.canonicalize()?;

            let response = send_request(
                &storage,
                &Request::Unmount {
                    mountpoint: mountpoint.to_string_lossy().to_string(),
                },
            )?;

            if response.ok {
                println!("Unmounted {:?}", mountpoint);
            } else {
                eprintln!("Error: {}", response.error.unwrap_or_default());
                process::exit(1);
            }
        }
    }

    Ok(())
}

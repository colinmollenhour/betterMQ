//! Crash-safe replacement of secret-bearing configuration files.

use std::fs::{self, OpenOptions};
use std::io::{self, Write};
use std::path::Path;

/// Create a private temporary file in the destination directory, sync its
/// contents, rename it atomically, then sync the directory entry on Unix.
/// Concurrent writers use distinct temporary files; readers see a whole value.
pub fn write_secret_atomic(path: &Path, bytes: &[u8]) -> io::Result<()> {
    let parent = path
        .parent()
        .filter(|p| !p.as_os_str().is_empty())
        .unwrap_or(Path::new("."));
    let mut dirs = fs::DirBuilder::new();
    dirs.recursive(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::DirBuilderExt;
        dirs.mode(0o700);
    }
    dirs.create(parent)?;
    let filename = path
        .file_name()
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "file name required"))?;
    let tmp = parent.join(format!(
        ".{}.{}.tmp",
        filename.to_string_lossy(),
        uuid::Uuid::new_v4()
    ));
    let mut options = OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    let mut file = options.open(&tmp)?;
    let result = (|| {
        file.write_all(bytes)?;
        file.sync_all()?;
        drop(file);
        fs::rename(&tmp, path)?;
        #[cfg(unix)]
        fs::File::open(parent)?.sync_all()?;
        Ok(())
    })();
    if result.is_err() {
        let _ = fs::remove_file(&tmp);
    }
    result
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn replaces_complete_contents_with_private_permissions() {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("new/config.json");
        write_secret_atomic(&path, b"first").unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            fs::set_permissions(&path, fs::Permissions::from_mode(0o644)).unwrap();
        }
        write_secret_atomic(&path, b"second").unwrap();
        assert_eq!(fs::read(&path).unwrap(), b"second");
        assert_eq!(fs::read_dir(path.parent().unwrap()).unwrap().count(), 1);
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            assert_eq!(
                fs::metadata(&path).unwrap().permissions().mode() & 0o777,
                0o600
            );
            assert_eq!(
                fs::metadata(path.parent().unwrap())
                    .unwrap()
                    .permissions()
                    .mode()
                    & 0o777,
                0o700
            );
        }
    }

    #[test]
    fn failed_replace_preserves_destination_and_cleans_temp() {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("directory");
        fs::create_dir(&path).unwrap();
        assert!(write_secret_atomic(&path, b"secret").is_err());
        assert!(path.is_dir());
        assert_eq!(fs::read_dir(root.path()).unwrap().count(), 1);
    }

    #[test]
    fn concurrent_replacements_do_not_share_temporary_files() {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("config.json");
        let barrier = std::sync::Arc::new(std::sync::Barrier::new(8));
        let writers: Vec<_> = (0..8)
            .map(|i| {
                let path = path.clone();
                let barrier = barrier.clone();
                std::thread::spawn(move || {
                    barrier.wait();
                    write_secret_atomic(&path, &serde_json::to_vec(&vec![i; 1024]).unwrap())
                        .unwrap();
                })
            })
            .collect();
        for writer in writers {
            writer.join().unwrap();
        }
        let value: Vec<u32> = serde_json::from_slice(&fs::read(path).unwrap()).unwrap();
        assert_eq!(value.len(), 1024);
        assert!(value.iter().all(|i| *i == value[0]));
        assert_eq!(fs::read_dir(root.path()).unwrap().count(), 1);
    }
}

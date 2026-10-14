// Copyright 2021-Present Datadog, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Conditional writes for [`LocalFileStorage`].
//!
//! A local file has no server-assigned version, so an object's version is the MD5 of its
//! content. The read-compare-write sequence is made atomic by holding an exclusive advisory lock
//! (`flock`) on a sidecar file at the storage root while it runs. The lock is per open file
//! description, so it serializes conditional writers both within one process and across
//! processes sharing the same directory.

use std::fs::File;
use std::path::Path;

use anyhow::anyhow;

use super::LocalFileStorage;
use crate::{ObjectVersion, PutPayload, Storage, StorageErrorKind, StorageResult};

/// Name of the sidecar file, at the storage root, on which conditional writers take an exclusive
/// lock. The file itself stays empty.
const LOCK_FILE_NAME: &str = ".quickwit-cas.lock";

/// Precondition attached to a conditional write.
pub(super) enum PutPrecondition<'a> {
    /// The write succeeds only if no file exists at the path.
    Absent,
    /// The write succeeds only if the current file content hashes to this version.
    MatchVersion(&'a ObjectVersion),
}

/// Two identical contents share a version. This is harmless for compare-and-swap: overwriting
/// identical bytes cannot lose an update.
pub(super) fn local_file_version(content: &[u8]) -> ObjectVersion {
    ObjectVersion::new(format!("{:x}", md5::compute(content)))
}

impl LocalFileStorage {
    /// Takes the exclusive lock on the sidecar file. The lock is released when the returned
    /// `File` is dropped.
    async fn lock_conditional_writes(&self) -> StorageResult<File> {
        let lock_file_path = self.root.join(LOCK_FILE_NAME);
        tokio::fs::create_dir_all(&self.root).await?;
        let locked_file = tokio::task::spawn_blocking(move || -> std::io::Result<File> {
            let lock_file = File::options()
                .create(true)
                .truncate(false)
                .write(true)
                .open(&lock_file_path)?;
            lock_file.lock()?;
            Ok(lock_file)
        })
        .await
        .map_err(|join_error| StorageErrorKind::Internal.with_error(join_error))??;
        Ok(locked_file)
    }

    pub(super) async fn put_conditional(
        &self,
        path: &Path,
        payload: Box<dyn PutPayload>,
        precondition: PutPrecondition<'_>,
    ) -> StorageResult<ObjectVersion> {
        // Validate the path and buffer the payload before taking the lock, so the critical
        // section only contains the read-compare-write.
        self.full_path(path)?;
        let payload_bytes = payload.read_all().await?;

        let _lock_guard = self.lock_conditional_writes().await?;

        let current_version_opt: Option<ObjectVersion> = match self.get_all(path).await {
            Ok(current_content) => Some(local_file_version(&current_content)),
            Err(error) if error.kind() == StorageErrorKind::NotFound => None,
            Err(error) => return Err(error),
        };
        match (&precondition, current_version_opt) {
            (PutPrecondition::Absent, None) => {}
            (PutPrecondition::Absent, Some(_)) => {
                return Err(StorageErrorKind::PreconditionFailed
                    .with_error(anyhow!("file `{}` already exists", path.display())));
            }
            (PutPrecondition::MatchVersion(_), None) => {
                return Err(StorageErrorKind::NotFound
                    .with_error(anyhow!("file `{}` does not exist", path.display())));
            }
            (PutPrecondition::MatchVersion(expected_version), Some(current_version)) => {
                if **expected_version != current_version {
                    return Err(StorageErrorKind::PreconditionFailed.with_error(anyhow!(
                        "file `{}` is at version {current_version}, expected {expected_version}",
                        path.display()
                    )));
                }
            }
        }
        let new_version = local_file_version(&payload_bytes);
        self.write_atomically(path, Box::new(payload_bytes.to_vec()))
            .await?;
        Ok(new_version)
    }
}

#[cfg(test)]
mod tests {
    use std::path::Path;
    use std::str::FromStr;
    use std::sync::Arc;

    use quickwit_common::uri::Uri;

    use super::*;
    use crate::storage_test_conditional_writes;

    fn storage_in(temp_dir: &Path) -> LocalFileStorage {
        let uri = Uri::from_str(&temp_dir.display().to_string()).unwrap();
        LocalFileStorage::from_uri(&uri).unwrap()
    }

    #[tokio::test]
    async fn test_local_file_storage_conditional_writes_suite() {
        let temp_dir = tempfile::tempdir().unwrap();
        storage_test_conditional_writes(Arc::new(storage_in(temp_dir.path())))
            .await
            .unwrap();
    }

    /// Two `LocalFileStorage` instances over the same directory share no in-memory state. Their
    /// conditional writes race through the same code path as two processes would: distinct open
    /// file descriptions contending for the sidecar lock. Exactly one writer per round must win.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn test_conditional_writes_are_atomic_across_storage_instances() {
        let temp_dir = tempfile::tempdir().unwrap();
        let path = Path::new("counter");
        let bootstrap_storage = storage_in(temp_dir.path());
        let initial_version = bootstrap_storage
            .put_if_absent(path, Box::new(b"0".to_vec()))
            .await
            .unwrap();

        const NUM_WRITERS: usize = 8;
        const NUM_ROUNDS: usize = 20;
        let mut tasks = Vec::with_capacity(NUM_WRITERS);
        for writer_id in 0..NUM_WRITERS {
            let storage = storage_in(temp_dir.path());
            let mut version = initial_version.clone();
            tasks.push(tokio::spawn(async move {
                let mut num_wins = 0usize;
                for _ in 0..NUM_ROUNDS {
                    // Each writer loops until it wins once per round, re-reading on conflict.
                    loop {
                        let (bytes, current_version) =
                            storage.get_all_with_version(path).await.unwrap();
                        let counter: u64 = std::str::from_utf8(&bytes).unwrap().parse().unwrap();
                        let payload = format!("{}", counter + 1).into_bytes();
                        match storage
                            .put_if_version_matches(path, Box::new(payload), &current_version)
                            .await
                        {
                            Ok(new_version) => {
                                version = new_version;
                                num_wins += 1;
                                break;
                            }
                            Err(error) => {
                                assert_eq!(
                                    error.kind(),
                                    StorageErrorKind::PreconditionFailed,
                                    "writer {writer_id}: {error}"
                                );
                            }
                        }
                    }
                }
                (num_wins, version)
            }));
        }
        let mut total_wins = 0;
        for task in tasks {
            let (num_wins, _version) = task.await.unwrap();
            total_wins += num_wins;
        }
        assert_eq!(total_wins, NUM_WRITERS * NUM_ROUNDS);

        // Every successful compare-and-swap incremented the counter exactly once: a lost update
        // would leave the counter below the number of wins.
        let final_bytes = bootstrap_storage.get_all(path).await.unwrap();
        let final_counter: usize = std::str::from_utf8(&final_bytes).unwrap().parse().unwrap();
        assert_eq!(final_counter, NUM_WRITERS * NUM_ROUNDS);
    }

    #[tokio::test]
    async fn test_version_is_derived_from_content() {
        let temp_dir = tempfile::tempdir().unwrap();
        let storage = storage_in(temp_dir.path());
        let path = Path::new("doc");
        let version = storage
            .put_if_absent(path, Box::new(b"same".to_vec()))
            .await
            .unwrap();

        // An unconditional overwrite with different bytes invalidates the version...
        storage
            .put(path, Box::new(b"other".to_vec()))
            .await
            .unwrap();
        let error = storage
            .put_if_version_matches(path, Box::new(b"next".to_vec()), &version)
            .await
            .unwrap_err();
        assert_eq!(error.kind(), StorageErrorKind::PreconditionFailed);

        // ...while restoring the same bytes restores the version. This is the documented
        // limitation of content-hash versions, shared with S3 ETags.
        storage.put(path, Box::new(b"same".to_vec())).await.unwrap();
        storage
            .put_if_version_matches(path, Box::new(b"next".to_vec()), &version)
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn test_sidecar_lock_file_lives_at_the_storage_root() {
        let temp_dir = tempfile::tempdir().unwrap();
        let storage = storage_in(temp_dir.path());
        storage
            .put_if_absent(Path::new("nested/dir/doc"), Box::new(b"x".to_vec()))
            .await
            .unwrap();
        assert!(temp_dir.path().join(LOCK_FILE_NAME).is_file());
        let nested_lock_file_path = temp_dir.path().join("nested/dir").join(LOCK_FILE_NAME);
        assert!(!nested_lock_file_path.try_exists().unwrap());
    }
}

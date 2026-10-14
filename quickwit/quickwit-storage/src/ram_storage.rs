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

use std::collections::HashMap;
use std::fmt;
use std::io::Cursor;
use std::ops::Range;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use async_trait::async_trait;
use quickwit_common::uri::{Protocol, Uri};
use quickwit_config::StorageBackend;
use tokio::io::{AsyncRead, AsyncWriteExt};
use tokio::sync::RwLock;

use crate::prefix_storage::add_prefix_to_storage;
use crate::storage::SendableAsync;
use crate::{
    BulkDeleteError, ObjectVersion, OwnedBytes, Storage, StorageErrorKind, StorageFactory,
    StorageResolverError, StorageResult,
};

/// An object held by the [`RamStorage`], together with the version conditional writes compare
/// against.
#[derive(Clone)]
struct RamObject {
    data: OwnedBytes,
    /// Unique per write (unlike an S3 ETag, which is a content hash), so that overwriting an
    /// object with identical bytes still invalidates previously read versions.
    version: u64,
}

impl RamObject {
    fn object_version(&self) -> ObjectVersion {
        ObjectVersion::new(self.version.to_string())
    }
}

/// In Ram implementation of quickwit's storage.
///
/// This implementation is mostly useful in unit tests.
#[derive(Clone)]
pub struct RamStorage {
    uri: Uri,
    files: Arc<RwLock<HashMap<PathBuf, RamObject>>>,
    /// Source of object versions, shared by all clones of a storage so that two clones never
    /// hand out the same version for two different writes.
    next_version: Arc<AtomicU64>,
}

impl fmt::Debug for RamStorage {
    fn fmt(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter
            .debug_struct("RamStorage")
            .field("uri", &self.uri)
            .finish()
    }
}

impl Default for RamStorage {
    fn default() -> Self {
        Self {
            uri: Uri::for_test("ram:///"),
            files: Arc::new(RwLock::new(HashMap::new())),
            next_version: Arc::new(AtomicU64::new(1)),
        }
    }
}

impl RamStorage {
    /// Creates a [`RamStorageBuilder`]
    pub fn builder() -> RamStorageBuilder {
        RamStorageBuilder::default()
    }

    fn new_object(&self, data: OwnedBytes) -> RamObject {
        let version = self.next_version.fetch_add(1, Ordering::Relaxed);
        RamObject { data, version }
    }

    async fn put_data(&self, path: &Path, payload: OwnedBytes) {
        let object = self.new_object(payload);
        self.files.write().await.insert(path.to_path_buf(), object);
    }

    async fn get_data(&self, path: &Path) -> Option<OwnedBytes> {
        self.files
            .read()
            .await
            .get(path)
            .map(|object| object.data.clone())
    }

    /// Returns the list of files that are present in the RamStorage.
    pub async fn list_files(&self) -> Vec<PathBuf> {
        self.files.read().await.keys().cloned().collect()
    }
}

#[async_trait]
impl Storage for RamStorage {
    async fn check_connectivity(&self) -> anyhow::Result<()> {
        Ok(())
    }

    async fn put(
        &self,
        path: &Path,
        payload: Box<dyn crate::PutPayload>,
    ) -> crate::StorageResult<()> {
        let payload_bytes = payload.read_all().await?;
        self.put_data(path, payload_bytes).await;
        Ok(())
    }

    async fn put_if_absent(
        &self,
        path: &Path,
        payload: Box<dyn crate::PutPayload>,
    ) -> StorageResult<ObjectVersion> {
        let payload_bytes = payload.read_all().await?;
        let mut files = self.files.write().await;
        if files.contains_key(path) {
            let err = anyhow::anyhow!("object `{}` already exists", path.display());
            return Err(StorageErrorKind::PreconditionFailed.with_error(err));
        }
        let object = self.new_object(payload_bytes);
        let version = object.object_version();
        files.insert(path.to_path_buf(), object);
        Ok(version)
    }

    async fn put_if_version_matches(
        &self,
        path: &Path,
        payload: Box<dyn crate::PutPayload>,
        expected_version: &ObjectVersion,
    ) -> StorageResult<ObjectVersion> {
        let payload_bytes = payload.read_all().await?;
        let mut files = self.files.write().await;
        let Some(current_object) = files.get(path) else {
            let err = anyhow::anyhow!("object `{}` does not exist", path.display());
            return Err(StorageErrorKind::NotFound.with_error(err));
        };
        let current_version = current_object.object_version();
        if current_version != *expected_version {
            let err = anyhow::anyhow!(
                "object `{}` has version `{current_version}`, expected `{expected_version}`",
                path.display(),
            );
            return Err(StorageErrorKind::PreconditionFailed.with_error(err));
        }
        let object = self.new_object(payload_bytes);
        let version = object.object_version();
        files.insert(path.to_path_buf(), object);
        Ok(version)
    }

    async fn get_all_with_version(
        &self,
        path: &Path,
    ) -> StorageResult<(OwnedBytes, ObjectVersion)> {
        let files = self.files.read().await;
        let Some(object) = files.get(path) else {
            let err = anyhow::anyhow!("failed to find dest_path {:?}", path);
            return Err(StorageErrorKind::NotFound.with_error(err));
        };
        Ok((object.data.clone(), object.object_version()))
    }

    async fn copy_to(&self, path: &Path, output: &mut dyn SendableAsync) -> StorageResult<()> {
        let payload_bytes = self.get_data(path).await.ok_or_else(|| {
            StorageErrorKind::NotFound
                .with_error(anyhow::anyhow!("failed to find dest_path {:?}", path))
        })?;
        output.write_all(&payload_bytes).await?;
        output.flush().await?;
        Ok(())
    }

    async fn get_slice(&self, path: &Path, range: Range<usize>) -> StorageResult<OwnedBytes> {
        let payload_bytes = self.get_data(path).await.ok_or_else(|| {
            StorageErrorKind::NotFound
                .with_error(anyhow::anyhow!("failed to find dest_path {:?}", path))
        })?;
        Ok(payload_bytes.slice(range.start..range.end))
    }

    async fn get_slice_stream(
        &self,
        path: &Path,
        range: Range<usize>,
    ) -> StorageResult<Box<dyn AsyncRead + Send + Unpin>> {
        let bytes = self.get_slice(path, range).await?;
        Ok(Box::new(Cursor::new(bytes)))
    }

    async fn delete(&self, path: &Path) -> StorageResult<()> {
        self.files.write().await.remove(path);
        Ok(())
    }

    async fn bulk_delete<'a>(&self, paths: &[&'a Path]) -> Result<(), BulkDeleteError> {
        let mut files = self.files.write().await;
        for &path in paths {
            files.remove(path);
        }
        Ok(())
    }

    async fn get_all(&self, path: &Path) -> StorageResult<OwnedBytes> {
        let payload_bytes = self.get_data(path).await.ok_or_else(|| {
            StorageErrorKind::NotFound
                .with_error(anyhow::anyhow!("failed to find dest_path {:?}", path))
        })?;
        Ok(payload_bytes)
    }

    fn uri(&self) -> &Uri {
        &self.uri
    }

    async fn file_num_bytes(&self, path: &Path) -> StorageResult<u64> {
        if let Some(object) = self.files.read().await.get(path) {
            Ok(object.data.len() as u64)
        } else {
            let err = anyhow::anyhow!("missing file `{}`", path.display());
            Err(StorageErrorKind::NotFound.with_error(err))
        }
    }
}

/// Builder to create a prepopulated [`RamStorage`]. This is mostly useful for tests.
#[derive(Default)]
pub struct RamStorageBuilder {
    files: HashMap<PathBuf, OwnedBytes>,
}

impl RamStorageBuilder {
    /// Adds a new file into the [`RamStorageBuilder`].
    pub fn put(mut self, path: &str, payload: &[u8]) -> Self {
        self.files
            .insert(PathBuf::from(path), OwnedBytes::new(payload.to_vec()));
        self
    }

    /// Finalizes the [`RamStorage`] creation.
    pub fn build(self) -> RamStorage {
        let storage = RamStorage::default();
        let files: HashMap<PathBuf, RamObject> = self
            .files
            .into_iter()
            .map(|(path, data)| (path, storage.new_object(data)))
            .collect();
        RamStorage {
            files: Arc::new(RwLock::new(files)),
            ..storage
        }
    }
}

/// Storage resolver for [`RamStorage`].
pub struct RamStorageFactory {
    ram_storage: Arc<dyn Storage>,
}

impl Default for RamStorageFactory {
    fn default() -> Self {
        RamStorageFactory {
            ram_storage: Arc::new(RamStorage::default()),
        }
    }
}

#[async_trait]
impl StorageFactory for RamStorageFactory {
    fn backend(&self) -> StorageBackend {
        StorageBackend::Ram
    }

    async fn resolve(&self, uri: &Uri) -> Result<Arc<dyn Storage>, StorageResolverError> {
        match uri.filepath() {
            Some(prefix) if uri.protocol() == Protocol::Ram => Ok(add_prefix_to_storage(
                self.ram_storage.clone(),
                prefix.to_path_buf(),
                uri.clone(),
            )),
            _ => {
                let message = format!("URI `{uri}` is not a valid RAM URI");
                Err(StorageResolverError::InvalidUri(message))
            }
        }
    }
}

#[cfg(test)]
mod tests {

    use super::*;
    use crate::storage_test_conditional_writes;
    use crate::test_suite::storage_test_suite;

    #[tokio::test]
    async fn test_storage() -> anyhow::Result<()> {
        let mut ram_storage = RamStorage::default();
        storage_test_suite(&mut ram_storage).await?;
        Ok(())
    }

    #[tokio::test]
    async fn test_conditional_writes() -> anyhow::Result<()> {
        storage_test_conditional_writes(Arc::new(RamStorage::default())).await
    }

    #[tokio::test]
    async fn test_ram_storage_clones_share_versions() -> anyhow::Result<()> {
        // Two clones stand for two nodes sharing one "bucket": a version read through one clone
        // must be honored, and invalidated, by writes made through the other.
        let storage_a = RamStorage::default();
        let storage_b = storage_a.clone();
        let path = Path::new("shared");
        let version_a = storage_a
            .put_if_absent(path, Box::new(b"a".to_vec()))
            .await?;
        let version_b = storage_b
            .put_if_version_matches(path, Box::new(b"b".to_vec()), &version_a)
            .await?;
        let error = storage_a
            .put_if_version_matches(path, Box::new(b"a2".to_vec()), &version_a)
            .await
            .unwrap_err();
        assert_eq!(error.kind(), StorageErrorKind::PreconditionFailed);
        assert_eq!(storage_a.get_all_with_version(path).await?.1, version_b);
        Ok(())
    }

    #[tokio::test]
    async fn test_ram_storage_builder_files_are_versioned() -> anyhow::Result<()> {
        let storage = RamStorage::builder().put("path1", b"payload").build();
        let (_, version) = storage.get_all_with_version(Path::new("path1")).await?;
        let new_version = storage
            .put_if_version_matches(Path::new("path1"), Box::new(b"payload2".to_vec()), &version)
            .await?;
        assert_ne!(new_version, version);
        // Versions allocated after `build()` never collide with the prepopulated ones.
        let other_version = storage
            .put_if_absent(Path::new("path2"), Box::new(b"payload".to_vec()))
            .await?;
        assert_ne!(other_version, version);
        assert_ne!(other_version, new_version);
        Ok(())
    }

    #[tokio::test]
    async fn test_ram_storage_factory() {
        let ram_storage_factory = RamStorageFactory::default();
        let ram_uri = Uri::for_test("s3:///foo");
        let err = ram_storage_factory.resolve(&ram_uri).await.err().unwrap();
        assert!(matches!(err, StorageResolverError::InvalidUri { .. }));

        let data_uri = Uri::for_test("ram:///data");
        let data_storage = ram_storage_factory.resolve(&data_uri).await.ok().unwrap();
        let home_uri = Uri::for_test("ram:///home");
        let home_storage = ram_storage_factory.resolve(&home_uri).await.ok().unwrap();
        assert_ne!(data_storage.uri(), home_storage.uri());

        let data_storage_two = ram_storage_factory.resolve(&data_uri).await.ok().unwrap();
        assert_eq!(data_storage.uri(), data_storage_two.uri());
    }

    #[tokio::test]
    async fn test_ram_storage_builder() -> anyhow::Result<()> {
        let storage = RamStorage::builder()
            .put("path1", b"path1_payload")
            .put("path2", b"path2_payload")
            .put("path1", b"path1_payloadb")
            .build();
        assert_eq!(
            &storage.get_all(Path::new("path1")).await?,
            &b"path1_payloadb"[..]
        );
        assert_eq!(
            &storage.get_all(Path::new("path2")).await?,
            &b"path2_payload"[..]
        );
        Ok(())
    }
}

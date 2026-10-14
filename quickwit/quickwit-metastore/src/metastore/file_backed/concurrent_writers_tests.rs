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

//! Tests for several `FileBackedMetastore` instances writing to the same storage in
//! [`WriteMode::Conditional`], as happens when several nodes host the metastore role on one
//! storage prefix.

use std::fmt;
use std::ops::Range;
use std::path::Path;
use std::sync::Arc;

use async_trait::async_trait;
use quickwit_common::uri::Uri;
use quickwit_config::IndexConfig;
use quickwit_proto::metastore::{
    CreateIndexRequest, EntityKind, ListSplitsRequest, MetastoreError, MetastoreService,
    StageSplitsRequest,
};
use quickwit_proto::types::IndexUid;
use quickwit_storage::{
    BulkDeleteError, GatedStorage, ObjectVersion, OwnedBytes, PutPayload, RamStorage,
    SendableAsync, Storage, StorageErrorKind, StorageResult,
};
use tokio::io::AsyncRead;

use super::store_operations::metastore_filepath;
use super::{FileBackedMetastore, WriteMode};
use crate::metastore::MetastoreServiceStreamSplitsExt;
use crate::{CreateIndexRequestExt, ListSplitsRequestExt, SplitMetadata, StageSplitsRequestExt};

async fn open_metastore(storage: Arc<dyn Storage>) -> FileBackedMetastore {
    FileBackedMetastore::try_new(storage, None, WriteMode::Conditional)
        .await
        .unwrap()
}

fn create_index_request(index_id: &str) -> CreateIndexRequest {
    let index_config = IndexConfig::for_test(index_id, &format!("ram:///indexes/{index_id}"));
    CreateIndexRequest::try_from_index_config(&index_config).unwrap()
}

fn spawn_stage_split(
    metastore: &FileBackedMetastore,
    index_uid: &IndexUid,
    split_id: &str,
) -> tokio::task::JoinHandle<()> {
    let split_metadata = SplitMetadata::for_test(split_id.into());
    let stage_splits_request =
        StageSplitsRequest::try_from_split_metadata(index_uid.clone(), &split_metadata).unwrap();
    let metastore = metastore.clone();
    tokio::spawn(async move {
        metastore.stage_splits(stage_splits_request).await.unwrap();
    })
}

async fn persisted_split_ids(storage: Arc<dyn Storage>, index_uid: &IndexUid) -> Vec<String> {
    let fresh_metastore = open_metastore(storage).await;
    let list_splits_request = ListSplitsRequest::try_from_index_uid(index_uid.clone()).unwrap();
    let mut split_ids: Vec<String> = fresh_metastore
        .list_splits(list_splits_request)
        .await
        .unwrap()
        .collect_split_ids()
        .await
        .unwrap()
        .into_iter()
        .map(|split_id| split_id.to_string())
        .collect();
    split_ids.sort();
    split_ids
}

/// Two metastores stage a split on the same index from the same version of its index file. The
/// one whose write lands second must notice the file changed, reload it and stage its split
/// again on top of the other's, so that both splits are persisted.
///
/// The gate holds both writes until they were both prepared from the same version, then lets
/// them land one after the other.
#[tokio::test]
async fn test_concurrent_index_mutations_from_two_metastores_are_not_lost() {
    let (gated_storage, gate) = GatedStorage::new(Arc::new(RamStorage::default()));
    let storage: Arc<dyn Storage> = Arc::new(gated_storage);
    let metastore_a = open_metastore(storage.clone()).await;
    let index_uid: IndexUid = metastore_a
        .create_index(create_index_request("test-index"))
        .await
        .unwrap()
        .index_uid()
        .clone();
    let metastore_b = open_metastore(storage.clone()).await;

    let index_filepath = metastore_filepath("test-index");
    gate.close(&index_filepath);

    let stage_split_a = spawn_stage_split(&metastore_a, &index_uid, "split-a");
    let stage_split_b = spawn_stage_split(&metastore_b, &index_uid, "split-b");
    gate.wait_until_num_blocked(&index_filepath, 2).await;

    gate.open(&index_filepath);
    stage_split_a.await.unwrap();
    stage_split_b.await.unwrap();

    assert_eq!(
        persisted_split_ids(storage, &index_uid).await,
        ["split-a", "split-b"]
    );
}

/// Two metastores race to create the same index: exactly one creation succeeds and the other
/// fails with `AlreadyExists` instead of silently overwriting the index file of the winner.
#[tokio::test]
async fn test_concurrent_creations_of_the_same_index_from_two_metastores() {
    let (gated_storage, gate) = GatedStorage::new(Arc::new(RamStorage::default()));
    let storage: Arc<dyn Storage> = Arc::new(gated_storage);
    let metastore_a = open_metastore(storage.clone()).await;
    let metastore_b = open_metastore(storage.clone()).await;

    let index_filepath = metastore_filepath("test-index");
    gate.close(&index_filepath);

    let create_index_a = tokio::spawn({
        let metastore_a = metastore_a.clone();
        async move {
            metastore_a
                .create_index(create_index_request("test-index"))
                .await
        }
    });
    let create_index_b = tokio::spawn({
        let metastore_b = metastore_b.clone();
        async move {
            metastore_b
                .create_index(create_index_request("test-index"))
                .await
        }
    });
    gate.wait_until_num_blocked(&index_filepath, 2).await;

    gate.open(&index_filepath);
    let create_index_results = [create_index_a.await.unwrap(), create_index_b.await.unwrap()];

    let (successes, failures): (Vec<_>, Vec<_>) = create_index_results
        .into_iter()
        .partition(|create_index_result| create_index_result.is_ok());
    let [winner_response] = successes
        .try_into()
        .expect("exactly one index creation should succeed");
    let [loser_error] = failures
        .try_into()
        .expect("exactly one index creation should fail");
    assert!(matches!(
        loser_error,
        Err(MetastoreError::AlreadyExists(EntityKind::Index { index_id })) if index_id == "test-index"
    ));

    // The persisted index is the winner's.
    let winner_index_uid = winner_response.unwrap().index_uid().clone();
    let fresh_metastore = open_metastore(storage).await;
    let list_splits_request =
        ListSplitsRequest::try_from_index_uid(winner_index_uid.clone()).unwrap();
    fresh_metastore
        .list_splits(list_splits_request)
        .await
        .expect("the index should exist under the winner's index UID");
}

/// Storage proxy whose conditional writes always report that the object changed, as if another
/// writer always got there first.
struct AlwaysConflictingStorage {
    inner: RamStorage,
}

impl fmt::Debug for AlwaysConflictingStorage {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("AlwaysConflictingStorage").finish()
    }
}

#[async_trait]
impl Storage for AlwaysConflictingStorage {
    async fn check_connectivity(&self) -> anyhow::Result<()> {
        self.inner.check_connectivity().await
    }

    async fn put(&self, path: &Path, payload: Box<dyn PutPayload>) -> StorageResult<()> {
        self.inner.put(path, payload).await
    }

    async fn put_if_absent(
        &self,
        path: &Path,
        payload: Box<dyn PutPayload>,
    ) -> StorageResult<ObjectVersion> {
        self.inner.put_if_absent(path, payload).await
    }

    async fn put_if_version_matches(
        &self,
        path: &Path,
        _payload: Box<dyn PutPayload>,
        _expected_version: &ObjectVersion,
    ) -> StorageResult<ObjectVersion> {
        Err(StorageErrorKind::PreconditionFailed
            .with_error(anyhow::anyhow!("`{}` changed", path.display())))
    }

    async fn get_all_with_version(
        &self,
        path: &Path,
    ) -> StorageResult<(OwnedBytes, ObjectVersion)> {
        self.inner.get_all_with_version(path).await
    }

    async fn copy_to(&self, path: &Path, output: &mut dyn SendableAsync) -> StorageResult<()> {
        self.inner.copy_to(path, output).await
    }

    async fn get_slice(&self, path: &Path, range: Range<usize>) -> StorageResult<OwnedBytes> {
        self.inner.get_slice(path, range).await
    }

    async fn get_slice_stream(
        &self,
        path: &Path,
        range: Range<usize>,
    ) -> StorageResult<Box<dyn AsyncRead + Send + Unpin>> {
        self.inner.get_slice_stream(path, range).await
    }

    async fn get_all(&self, path: &Path) -> StorageResult<OwnedBytes> {
        self.inner.get_all(path).await
    }

    async fn delete(&self, path: &Path) -> StorageResult<()> {
        self.inner.delete(path).await
    }

    async fn bulk_delete<'a>(&self, paths: &[&'a Path]) -> Result<(), BulkDeleteError> {
        self.inner.bulk_delete(paths).await
    }

    async fn exists(&self, path: &Path) -> StorageResult<bool> {
        self.inner.exists(path).await
    }

    async fn file_num_bytes(&self, path: &Path) -> StorageResult<u64> {
        self.inner.file_num_bytes(path).await
    }

    fn uri(&self) -> &Uri {
        self.inner.uri()
    }
}

/// A mutation whose write keeps losing races gives up with a retryable error instead of retrying
/// forever. Tokio's paused clock skips the backoff delays.
#[tokio::test(start_paused = true)]
async fn test_index_mutation_gives_up_after_too_many_conflicts() {
    let storage = Arc::new(AlwaysConflictingStorage {
        inner: RamStorage::default(),
    });
    let metastore = open_metastore(storage).await;
    let index_uid: IndexUid = metastore
        .create_index(create_index_request("test-index"))
        .await
        .unwrap()
        .index_uid()
        .clone();

    let split_metadata = SplitMetadata::for_test("split".into());
    let stage_splits_request =
        StageSplitsRequest::try_from_split_metadata(index_uid, &split_metadata).unwrap();
    let stage_splits_error = metastore
        .stage_splits(stage_splits_request)
        .await
        .unwrap_err();
    assert!(
        matches!(stage_splits_error, MetastoreError::Unavailable(_)),
        "{stage_splits_error:?}"
    );
}

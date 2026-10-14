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

//! Periodic reload of the manifest written by other metastores sharing the storage prefix.

use std::sync::{Arc, Weak};
use std::time::Duration;

use quickwit_proto::metastore::MetastoreResult;
use quickwit_storage::Storage;
use tokio::sync::{Mutex, RwLock};
use tracing::error;

use super::WriteMode;
use super::manifest::load_manifest;
use super::state::MetastoreState;

/// Spawns a task that reloads the manifest every `polling_interval` and adopts it when its
/// version changed, so that this metastore learns about the indexes, templates and identity
/// written by the other metastores sharing its storage prefix.
///
/// The task exits once the metastore owning `state_weak` is dropped.
///
/// Hidden contracts:
/// - the metastore is in [`WriteMode::Conditional`], the only mode in which manifest versions are
///   tracked;
/// - `manifest_lock` is the metastore's, so that a poll never replaces a state holding an unsaved
///   change of an in-flight operation. The lock is held across the manifest load because object
///   versions are not ordered: a manifest loaded before a local save could otherwise be mistaken
///   for a newer one.
pub(super) fn spawn_manifest_polling_task(
    storage: Arc<dyn Storage>,
    state_weak: Weak<RwLock<MetastoreState>>,
    manifest_lock: Arc<Mutex<()>>,
    polling_interval: Duration,
) {
    tokio::task::spawn(async move {
        let mut interval = tokio::time::interval(polling_interval);
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        // The manifest was just loaded: skip the immediate first tick.
        interval.tick().await;

        loop {
            interval.tick().await;
            let Some(state) = state_weak.upgrade() else {
                return;
            };
            let _manifest_guard = manifest_lock.lock().await;
            let poll_result = poll_manifest_once(&storage, &state, polling_interval).await;
            if let Err(metastore_error) = poll_result {
                error!(
                    error=%metastore_error,
                    "failed to poll manifest file located at `{}/{}`",
                    storage.uri(), super::manifest::MANIFEST_FILE_NAME
                );
            }
        }
    });
}

async fn poll_manifest_once(
    storage: &Arc<dyn Storage>,
    state: &RwLock<MetastoreState>,
    polling_interval: Duration,
) -> MetastoreResult<()> {
    let loaded_manifest = load_manifest(&**storage, WriteMode::Conditional).await?;
    let mut state_wlock_guard = state.write().await;
    if loaded_manifest.version == state_wlock_guard.manifest_version {
        return Ok(());
    }
    state_wlock_guard.reset_from_manifest(
        storage,
        loaded_manifest,
        Some(polling_interval),
        WriteMode::Conditional,
    )
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::Duration;

    use quickwit_proto::metastore::{
        DeleteIndexRequest, DeleteIndexTemplatesRequest, ListIndexesMetadataRequest,
        MetastoreService,
    };
    use quickwit_storage::{RamStorage, Storage};

    use super::super::concurrent_writers_tests::{
        create_index_request, create_index_template_request, open_metastore, template_ids,
    };
    use super::super::{FileBackedMetastore, WriteMode};
    use crate::ListIndexesMetadataResponseExt;

    const POLLING_INTERVAL: Duration = Duration::from_secs(1);

    async fn index_ids(metastore: &FileBackedMetastore) -> Vec<String> {
        let mut index_ids: Vec<String> = metastore
            .list_indexes_metadata(ListIndexesMetadataRequest::all())
            .await
            .unwrap()
            .deserialize_indexes_metadata()
            .await
            .unwrap()
            .into_iter()
            .map(|index_metadata| index_metadata.index_id().to_string())
            .collect();
        index_ids.sort();
        index_ids
    }

    /// Lets the polling task run at least once. The test clock is paused, so this returns as
    /// soon as the poll has completed rather than after wall-clock time.
    async fn wait_for_next_poll() {
        tokio::time::sleep(POLLING_INTERVAL * 2).await;
    }

    /// A metastore that polls the manifest sees the indexes and templates another metastore
    /// creates and deletes on the same storage, without being asked for them by ID. A metastore
    /// that does not poll keeps its view from startup, as a control.
    #[tokio::test(start_paused = true)]
    async fn test_polling_metastore_follows_manifest_changes_of_another_metastore() {
        let storage: Arc<dyn Storage> = Arc::new(RamStorage::default());
        let writer = open_metastore(storage.clone()).await;
        let non_polling_metastore = open_metastore(storage.clone()).await;
        let poller =
            FileBackedMetastore::try_new(storage, Some(POLLING_INTERVAL), WriteMode::Conditional)
                .await
                .unwrap();

        let index_uid = writer
            .create_index(create_index_request("test-index"))
            .await
            .unwrap()
            .index_uid()
            .clone();
        writer
            .create_index_template(create_index_template_request("test-template"))
            .await
            .unwrap();

        wait_for_next_poll().await;
        assert_eq!(index_ids(&poller).await, ["test-index"]);
        assert_eq!(template_ids(&poller).await, ["test-template"]);
        assert!(index_ids(&non_polling_metastore).await.is_empty());
        assert!(template_ids(&non_polling_metastore).await.is_empty());

        writer
            .delete_index(DeleteIndexRequest {
                index_uid: Some(index_uid),
            })
            .await
            .unwrap();
        writer
            .delete_index_templates(DeleteIndexTemplatesRequest {
                template_ids: vec!["test-template".to_string()],
            })
            .await
            .unwrap();

        wait_for_next_poll().await;
        assert!(index_ids(&poller).await.is_empty());
        assert!(template_ids(&poller).await.is_empty());
    }
}

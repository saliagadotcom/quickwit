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

//! Tests for how concurrent operations on one `FileBackedMetastore` interact.

use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use quickwit_config::{IndexConfig, IndexTemplate};
use quickwit_proto::metastore::{
    CreateIndexRequest, CreateIndexTemplateRequest, IndexMetadataRequest,
    ListIndexTemplatesRequest, ListIndexesMetadataRequest, MetastoreService,
};
use quickwit_proto::types::IndexUid;
use quickwit_storage::{GatedStorage, RamStorage, Storage};

use super::FileBackedMetastore;
use super::manifest::MANIFEST_FILE_NAME;
use crate::{CreateIndexRequestExt, IndexMetadataResponseExt, ListIndexesMetadataResponseExt};

const READ_TIMEOUT: Duration = Duration::from_secs(2);

async fn create_index(metastore: &FileBackedMetastore, index_id: &str) -> IndexUid {
    let index_config = IndexConfig::for_test(index_id, &format!("ram:///indexes/{index_id}"));
    let create_index_request = CreateIndexRequest::try_from_index_config(&index_config).unwrap();
    metastore
        .create_index(create_index_request)
        .await
        .unwrap()
        .index_uid()
        .clone()
}

fn spawn_create_index_template(
    metastore: &FileBackedMetastore,
    template_id: &str,
) -> tokio::task::JoinHandle<()> {
    let index_template = IndexTemplate::for_test(template_id, &[&format!("{template_id}-*")], 100);
    let create_index_template_request = CreateIndexTemplateRequest {
        index_template_json: serde_json::to_string(&index_template).unwrap(),
        overwrite: false,
    };
    let metastore = metastore.clone();
    tokio::spawn(async move {
        metastore
            .create_index_template(create_index_template_request)
            .await
            .unwrap();
    })
}

async fn index_exists(metastore: &FileBackedMetastore, index_id: &str) -> bool {
    let request = IndexMetadataRequest::for_index_id(index_id.to_string());
    metastore
        .index_metadata(request)
        .await
        .unwrap()
        .deserialize_index_metadata()
        .is_ok()
}

/// A slow manifest write, as happens with a remote storage, must not stall readers of other
/// indexes. Readers of the index being created wait for the creation to complete and then see
/// the index.
#[tokio::test]
async fn test_index_creation_blocked_on_manifest_write_does_not_stall_other_indexes() {
    let (gated_storage, gate) = GatedStorage::new(Arc::new(RamStorage::default()));
    let metastore = FileBackedMetastore::try_new(Arc::new(gated_storage), None)
        .await
        .unwrap();
    create_index(&metastore, "index-a").await;

    let manifest_path = Path::new(MANIFEST_FILE_NAME);
    gate.close(manifest_path);

    let create_index_b = tokio::spawn({
        let metastore = metastore.clone();
        async move { create_index(&metastore, "index-b").await }
    });
    gate.wait_until_blocked(manifest_path).await;

    let is_index_a_readable =
        tokio::time::timeout(READ_TIMEOUT, index_exists(&metastore, "index-a"))
            .await
            .expect("reading index-a should not wait for the manifest write of index-b");
    assert!(is_index_a_readable);

    let list_indexes_result = tokio::time::timeout(
        READ_TIMEOUT,
        metastore.list_indexes_metadata(ListIndexesMetadataRequest::all()),
    )
    .await
    .expect("listing indexes should not wait for the manifest write of index-b");
    let listed_index_ids: Vec<String> = list_indexes_result
        .unwrap()
        .deserialize_indexes_metadata()
        .await
        .unwrap()
        .into_iter()
        .map(|index_metadata| index_metadata.index_id().to_string())
        .collect();
    assert_eq!(listed_index_ids, ["index-a"]);

    // A reader of the index being created waits for the creation instead of failing.
    let read_index_b = tokio::spawn({
        let metastore = metastore.clone();
        async move { index_exists(&metastore, "index-b").await }
    });
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert!(!read_index_b.is_finished());

    gate.open(manifest_path);
    create_index_b.await.unwrap();
    assert!(read_index_b.await.unwrap());
}

/// Two concurrent manifest updates must both end up in the manifest written to storage, which
/// is what a metastore started later on the same storage reads.
///
/// The gate lands the second template's manifest write first and the first template's write
/// last. Were the two operations allowed to interleave, the first write would be based on a
/// manifest snapshot without the second template and would erase it.
#[tokio::test]
async fn test_concurrent_manifest_updates_are_not_lost() {
    let (gated_storage, gate) = GatedStorage::new(Arc::new(RamStorage::default()));
    let gated_storage: Arc<dyn Storage> = Arc::new(gated_storage);
    let metastore = FileBackedMetastore::try_new(gated_storage.clone(), None)
        .await
        .unwrap();

    let manifest_path = Path::new(MANIFEST_FILE_NAME);
    gate.close(manifest_path);

    let create_template_1 = spawn_create_index_template(&metastore, "template-1");
    gate.wait_until_blocked(manifest_path).await;
    let create_template_2 = spawn_create_index_template(&metastore, "template-2");
    // Lets the second creation run until it blocks, either on the gate or on the first creation.
    tokio::time::sleep(Duration::from_millis(100)).await;

    gate.release_one(manifest_path).await;
    gate.wait_until_blocked(manifest_path).await;
    gate.release_one(manifest_path).await;

    create_template_1.await.unwrap();
    create_template_2.await.unwrap();

    let reopened_metastore = FileBackedMetastore::try_new(gated_storage, None)
        .await
        .unwrap();
    let mut persisted_template_ids: Vec<String> = reopened_metastore
        .list_index_templates(ListIndexTemplatesRequest {})
        .await
        .unwrap()
        .index_templates_json
        .iter()
        .map(|index_template_json| {
            let index_template: IndexTemplate = serde_json::from_str(index_template_json).unwrap();
            index_template.template_id
        })
        .collect();
    persisted_template_ids.sort();
    assert_eq!(persisted_template_ids, ["template-1", "template-2"]);
}

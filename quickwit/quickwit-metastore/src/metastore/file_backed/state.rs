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
use std::sync::Arc;
use std::time::Duration;

use quickwit_config::{IndexTemplate, IndexTemplateId};
use quickwit_proto::metastore::MetastoreResult;
use quickwit_proto::types::IndexId;
use quickwit_storage::{ObjectVersion, Storage};
use uuid::Uuid;

use super::index_template_matcher::IndexTemplateMatcher;
use super::lazy_file_backed_index::LazyFileBackedIndex;
use super::manifest::{IndexStatus, LoadedManifest, Manifest};
use super::{LazyIndexStatus, WriteMode};

#[derive(Default)]
pub(super) struct MetastoreState {
    pub indexes: HashMap<IndexId, LazyIndexStatus>,
    pub templates: HashMap<IndexTemplateId, IndexTemplate>,
    pub template_matcher: IndexTemplateMatcher,
    pub identity: Uuid,
    /// Version of the manifest file this state was last loaded from or saved to.
    ///
    /// Hidden contract: `Some` in [`WriteMode::Conditional`] once the manifest was loaded, where
    /// the next manifest save is conditioned on it; `None` in [`WriteMode::Unconditional`].
    pub manifest_version: Option<ObjectVersion>,
}

impl MetastoreState {
    pub fn try_from_manifest(
        storage: &Arc<dyn Storage>,
        loaded_manifest: LoadedManifest,
        polling_interval_opt: Option<Duration>,
        write_mode: WriteMode,
    ) -> MetastoreResult<Self> {
        let mut state = Self::default();
        state.reset_from_manifest(storage, loaded_manifest, polling_interval_opt, write_mode)?;
        Ok(state)
    }

    /// Replaces the whole state with the content of `loaded_manifest`, dropping any local change
    /// that was not saved to the storage.
    ///
    /// Indexes that are active in both the current state and the manifest keep their cached
    /// copy, so that adopting a manifest written by another metastore does not force a reload of
    /// every index file.
    pub fn reset_from_manifest(
        &mut self,
        storage: &Arc<dyn Storage>,
        loaded_manifest: LoadedManifest,
        polling_interval_opt: Option<Duration>,
        write_mode: WriteMode,
    ) -> MetastoreResult<()> {
        let LoadedManifest { manifest, version } = loaded_manifest;
        let template_matcher =
            IndexTemplateMatcher::try_from_index_templates(manifest.templates.values())?;

        let mut previous_indexes = std::mem::take(&mut self.indexes);
        self.indexes = manifest
            .indexes
            .into_iter()
            .map(|(index_id, index_status)| {
                let lazy_index_status = match (index_status, previous_indexes.remove(&index_id)) {
                    (IndexStatus::Active, Some(LazyIndexStatus::Active(lazy_index))) => {
                        LazyIndexStatus::Active(lazy_index)
                    }
                    (IndexStatus::Active, _) => {
                        let lazy_index = LazyFileBackedIndex::new(
                            storage.clone(),
                            index_id.clone(),
                            polling_interval_opt,
                            write_mode,
                            None,
                        );
                        LazyIndexStatus::Active(lazy_index)
                    }
                    (IndexStatus::Creating, _) => LazyIndexStatus::Creating,
                    (IndexStatus::Deleting, _) => LazyIndexStatus::Deleting,
                };
                (index_id, lazy_index_status)
            })
            .collect();
        self.templates = manifest.templates;
        self.template_matcher = template_matcher;
        self.identity = manifest.identity;
        self.manifest_version = version;
        Ok(())
    }

    pub fn as_manifest(&self) -> Manifest {
        let indexes = self
            .indexes
            .iter()
            .map(|(index_id, index_state)| {
                let index_status = match index_state {
                    LazyIndexStatus::Creating => IndexStatus::Creating,
                    LazyIndexStatus::Active(_) => IndexStatus::Active,
                    LazyIndexStatus::Deleting => IndexStatus::Deleting,
                };
                (index_id.clone(), index_status)
            })
            .collect();
        let templates = self.templates.clone();
        Manifest {
            indexes,
            templates,
            identity: self.identity,
        }
    }
}

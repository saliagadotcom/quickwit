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

use std::path::{Path, PathBuf};

use quickwit_proto::metastore::{EntityKind, MetastoreError, MetastoreResult, serde_utils};
use quickwit_storage::{OwnedBytes, Storage, StorageError, StorageErrorKind};

use super::WriteMode;
use crate::metastore::file_backed::file_backed_index::FileBackedIndex;

/// Index metastore file managed by [`FileBackedMetastore`](crate::FileBackedMetastore).
pub(super) const METASTORE_FILE_NAME: &str = "metastore.json";

/// Path to the metadata file from the given index ID.
pub(super) fn metastore_filepath(index_id: &str) -> PathBuf {
    Path::new(index_id).join(METASTORE_FILE_NAME)
}

fn convert_error(index_id: &str, storage_error: StorageError) -> MetastoreError {
    match storage_error.kind() {
        StorageErrorKind::NotFound => MetastoreError::NotFound(EntityKind::Index {
            index_id: index_id.to_string(),
        }),
        StorageErrorKind::Unauthorized => MetastoreError::Forbidden {
            message: "the request credentials do not allow for this operation".to_string(),
        },
        StorageErrorKind::Unsupported => MetastoreError::Internal {
            message: format!(
                "the storage does not support the conditional writes required to write the index \
                 `{index_id}` concurrently"
            ),
            cause: storage_error.to_string(),
        },
        _ => MetastoreError::Internal {
            message: "failed to get index files".to_string(),
            cause: storage_error.to_string(),
        },
    }
}

/// Loads the index file of `index_id`.
///
/// In [`WriteMode::Conditional`], the version of the file is recorded on the returned index so
/// that its next write can be conditioned on it.
pub(super) async fn load_index(
    storage: &dyn Storage,
    index_id: &str,
    write_mode: WriteMode,
) -> MetastoreResult<FileBackedIndex> {
    let metastore_filepath = metastore_filepath(index_id);

    let (content, storage_version_opt): (OwnedBytes, _) = match write_mode {
        WriteMode::Unconditional => {
            let content = storage
                .get_all(&metastore_filepath)
                .await
                .map_err(|storage_err| convert_error(index_id, storage_err))?;
            (content, None)
        }
        WriteMode::Conditional => {
            let (content, storage_version) = storage
                .get_all_with_version(&metastore_filepath)
                .await
                .map_err(|storage_err| convert_error(index_id, storage_err))?;
            (content, Some(storage_version))
        }
    };
    let mut index: FileBackedIndex = serde_utils::from_json_bytes(&content)?;
    index.set_storage_version(storage_version_opt);

    if index.index_id() != index_id {
        return Err(MetastoreError::Internal {
            message: "inconsistent manifest: index_id mismatch".to_string(),
            cause: format!(
                "expected index_id `{}`, but found `{}`",
                index_id,
                index.index_id()
            ),
        });
    }
    Ok(index)
}

pub(super) async fn index_exists(storage: &dyn Storage, index_id: &str) -> MetastoreResult<bool> {
    let metastore_filepath = metastore_filepath(index_id);
    let exists = storage
        .exists(&metastore_filepath)
        .await
        .map_err(|storage_error| convert_error(index_id, storage_error))?;
    Ok(exists)
}

/// Serializes the `Index` object and stores the data on the storage, unconditionally.
///
/// Do not call this method. Instead, call `put_index` or `create_index_file`.
/// This method only exists to let unit tests write index files under arbitrary paths.
#[cfg(test)]
pub(super) async fn put_index_given_index_id(
    storage: &dyn Storage,
    index: &FileBackedIndex,
    index_id: &str,
) -> MetastoreResult<()> {
    // Serialize Index.
    let content: Vec<u8> = serde_utils::to_json_bytes_pretty(index)?;
    let metastore_filepath = metastore_filepath(index_id);
    // Put data back into storage.
    storage
        .put(&metastore_filepath, Box::new(content))
        .await
        .map_err(|storage_err| convert_error(index_id, storage_err))?;
    Ok(())
}

/// Outcome of a conditional index file write.
#[derive(Debug)]
pub(super) enum PutIndexOutcome {
    /// The index file was written. In [`WriteMode::Conditional`], the index's storage version
    /// was updated to the version that was written.
    Written,
    /// The index file on the storage is not at the version recorded on the index anymore:
    /// another writer modified it. Nothing was written.
    Conflict,
}

/// Serializes the `Index` object and stores it on the storage.
///
/// In [`WriteMode::Conditional`], the write only succeeds if the index file is still at the
/// version recorded on `index`, see [`FileBackedIndex::storage_version`]. Otherwise, the method
/// returns [`PutIndexOutcome::Conflict`] and the caller is expected to reload the index and retry.
///
/// Hidden contract: in [`WriteMode::Conditional`], `index` carries a storage version, i.e. it was
/// obtained from `load_index` or `create_index_file` in that same mode.
pub(super) async fn put_index(
    storage: &dyn Storage,
    index: &mut FileBackedIndex,
    write_mode: WriteMode,
) -> MetastoreResult<PutIndexOutcome> {
    let index_id = index.index_id();
    let content: Vec<u8> = serde_utils::to_json_bytes_pretty(&*index)?;
    let metastore_filepath = metastore_filepath(index_id);

    let expected_version = match write_mode {
        WriteMode::Unconditional => {
            storage
                .put(&metastore_filepath, Box::new(content))
                .await
                .map_err(|storage_err| convert_error(index_id, storage_err))?;
            return Ok(PutIndexOutcome::Written);
        }
        WriteMode::Conditional => {
            index
                .storage_version()
                .ok_or_else(|| MetastoreError::Internal {
                    message: format!(
                                "index `{index_id}` has no storage version but the metastore \
                                 writes                          conditionally"
                            ),
                    cause: "".to_string(),
                })?
        }
    };
    let put_result = storage
        .put_if_version_matches(&metastore_filepath, Box::new(content), expected_version)
        .await;
    match put_result {
        Ok(new_version) => {
            index.set_storage_version(Some(new_version));
            Ok(PutIndexOutcome::Written)
        }
        Err(storage_error) if storage_error.kind() == StorageErrorKind::PreconditionFailed => {
            Ok(PutIndexOutcome::Conflict)
        }
        Err(storage_error) => Err(convert_error(index_id, storage_error)),
    }
}

/// Serializes the `Index` object of a newly created index and stores it on the storage.
///
/// In [`WriteMode::Conditional`], the write only succeeds if no index file exists yet, so two
/// metastores racing to create the same index cannot both succeed: the loser gets a
/// [`MetastoreError::AlreadyExists`] error. In [`WriteMode::Unconditional`], an existing file is
/// overwritten.
pub(super) async fn create_index_file(
    storage: &dyn Storage,
    index: &mut FileBackedIndex,
    write_mode: WriteMode,
) -> MetastoreResult<()> {
    let index_id = index.index_id();
    let content: Vec<u8> = serde_utils::to_json_bytes_pretty(&*index)?;
    let metastore_filepath = metastore_filepath(index_id);

    match write_mode {
        WriteMode::Unconditional => {
            storage
                .put(&metastore_filepath, Box::new(content))
                .await
                .map_err(|storage_err| convert_error(index_id, storage_err))?;
            Ok(())
        }
        WriteMode::Conditional => {
            let put_result = storage
                .put_if_absent(&metastore_filepath, Box::new(content))
                .await;
            match put_result {
                Ok(new_version) => {
                    index.set_storage_version(Some(new_version));
                    Ok(())
                }
                Err(storage_error)
                    if storage_error.kind() == StorageErrorKind::PreconditionFailed =>
                {
                    Err(MetastoreError::AlreadyExists(EntityKind::Index {
                        index_id: index_id.to_string(),
                    }))
                }
                Err(storage_error) => Err(convert_error(index_id, storage_error)),
            }
        }
    }
}

/// Serializes the Index and stores the data on the storage.
pub(super) async fn delete_index(storage: &dyn Storage, index_id: &str) -> MetastoreResult<()> {
    let metastore_filepath = metastore_filepath(index_id);

    let file_exists = storage
        .exists(&metastore_filepath)
        .await
        .map_err(|storage_err| convert_error(index_id, storage_err))?;

    if !file_exists {
        return Err(MetastoreError::NotFound(EntityKind::Index {
            index_id: index_id.to_string(),
        }));
    }
    // Put data back into storage.
    storage
        .delete(&metastore_filepath)
        .await
        .map_err(|storage_error| match storage_error.kind() {
            StorageErrorKind::Unauthorized => MetastoreError::Forbidden {
                message: "the request credentials do not allow for this operation".to_string(),
            },
            _ => MetastoreError::Internal {
                message: format!(
                    "failed to delete metastore file located at `{}/{}`",
                    storage.uri(),
                    metastore_filepath.display()
                ),
                cause: storage_error.to_string(),
            },
        })?;
    Ok(())
}

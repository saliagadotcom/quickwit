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

//! Shared test suite for the conditional write operations of [`Storage`]:
//! [`Storage::put_if_absent`], [`Storage::put_if_version_matches`] and
//! [`Storage::get_all_with_version`].
//!
//! Every backend that implements conditional writes runs [`storage_test_conditional_writes`];
//! every backend (or wrapper) that does not runs [`storage_test_conditional_writes_unsupported`].

use std::path::Path;
use std::sync::Arc;

use anyhow::Context;
use futures::future::join_all;

use crate::{ObjectVersion, OwnedBytes, Storage, StorageError, StorageErrorKind, StorageResult};

const NUM_CONCURRENT_WRITERS: usize = 8;

fn assert_error_kind<T: std::fmt::Debug>(
    result: StorageResult<T>,
    expected_kind: StorageErrorKind,
) -> anyhow::Result<StorageError> {
    match result {
        Ok(value) => anyhow::bail!("expected `{expected_kind:?}` error, got `Ok({value:?})`"),
        Err(error) if error.kind() == expected_kind => Ok(error),
        Err(error) => anyhow::bail!("expected `{expected_kind:?}` error, got `{error}`"),
    }
}

async fn assert_object_is(
    storage: &dyn Storage,
    path: &Path,
    expected_bytes: &[u8],
    expected_version: &ObjectVersion,
) -> anyhow::Result<()> {
    let (bytes, version) = storage.get_all_with_version(path).await?;
    anyhow::ensure!(
        bytes.as_slice() == expected_bytes,
        "unexpected object content: `{bytes:?}`"
    );
    anyhow::ensure!(
        version == *expected_version,
        "unexpected object version: `{version}`, expected `{expected_version}`"
    );
    // `get_all` and `get_all_with_version` must agree.
    let bytes_without_version = storage.get_all(path).await?;
    anyhow::ensure!(bytes_without_version.as_slice() == expected_bytes);
    Ok(())
}

async fn test_put_if_absent_creates_once(storage: &dyn Storage) -> anyhow::Result<()> {
    let path = Path::new("conditional_writes/put_if_absent");
    let version = storage
        .put_if_absent(path, Box::new(b"first".to_vec()))
        .await?;
    assert_object_is(storage, path, b"first", &version).await?;

    assert_error_kind(
        storage
            .put_if_absent(path, Box::new(b"second".to_vec()))
            .await,
        StorageErrorKind::PreconditionFailed,
    )?;
    // The losing write must not have touched the object.
    assert_object_is(storage, path, b"first", &version).await?;

    storage.delete(path).await?;
    Ok(())
}

async fn test_put_if_version_matches_is_compare_and_swap(
    storage: &dyn Storage,
) -> anyhow::Result<()> {
    let path = Path::new("conditional_writes/put_if_version_matches");
    storage.put(path, Box::new(b"v1".to_vec())).await?;
    let (bytes, version_1) = storage.get_all_with_version(path).await?;
    anyhow::ensure!(bytes.as_slice() == b"v1");

    let version_2 = storage
        .put_if_version_matches(path, Box::new(b"v2".to_vec()), &version_1)
        .await?;
    anyhow::ensure!(version_2 != version_1, "version did not change on write");
    assert_object_is(storage, path, b"v2", &version_2).await?;

    // The stale version loses, and loses without side effects.
    assert_error_kind(
        storage
            .put_if_version_matches(path, Box::new(b"v3".to_vec()), &version_1)
            .await,
        StorageErrorKind::PreconditionFailed,
    )?;
    assert_object_is(storage, path, b"v2", &version_2).await?;

    // A version returned by a successful write is usable for the next write.
    let version_3 = storage
        .put_if_version_matches(path, Box::new(b"v3".to_vec()), &version_2)
        .await?;
    assert_object_is(storage, path, b"v3", &version_3).await?;

    storage.delete(path).await?;
    Ok(())
}

async fn test_unconditional_put_invalidates_version(storage: &dyn Storage) -> anyhow::Result<()> {
    let path = Path::new("conditional_writes/unconditional_put");
    storage.put(path, Box::new(b"original".to_vec())).await?;
    let (_, stale_version) = storage.get_all_with_version(path).await?;

    storage.put(path, Box::new(b"overwritten".to_vec())).await?;

    assert_error_kind(
        storage
            .put_if_version_matches(path, Box::new(b"lost update".to_vec()), &stale_version)
            .await,
        StorageErrorKind::PreconditionFailed,
    )?;
    let (bytes, current_version) = storage.get_all_with_version(path).await?;
    anyhow::ensure!(bytes.as_slice() == b"overwritten");
    anyhow::ensure!(current_version != stale_version);

    storage.delete(path).await?;
    Ok(())
}

async fn test_put_if_version_matches_on_deleted_object(
    storage: &dyn Storage,
) -> anyhow::Result<()> {
    let path = Path::new("conditional_writes/deleted_object");
    let version = storage
        .put_if_absent(path, Box::new(b"short-lived".to_vec()))
        .await?;
    storage.delete(path).await?;

    assert_error_kind(
        storage
            .put_if_version_matches(path, Box::new(b"resurrected".to_vec()), &version)
            .await,
        StorageErrorKind::NotFound,
    )?;
    anyhow::ensure!(!storage.exists(path).await?, "object was resurrected");

    // Once deleted, the path is absent again and can be re-created.
    let new_version = storage
        .put_if_absent(path, Box::new(b"recreated".to_vec()))
        .await?;
    assert_object_is(storage, path, b"recreated", &new_version).await?;

    storage.delete(path).await?;
    Ok(())
}

async fn test_get_all_with_version_missing_object(storage: &dyn Storage) -> anyhow::Result<()> {
    let path = Path::new("conditional_writes/missing_object");
    assert_error_kind(
        storage.get_all_with_version(path).await,
        StorageErrorKind::NotFound,
    )?;
    Ok(())
}

/// Checks that exactly one of `results` succeeded and that the object holds that writer's
/// payload, while every other writer failed with `PreconditionFailed`.
async fn assert_exactly_one_winner(
    storage: &dyn Storage,
    path: &Path,
    results: Vec<(OwnedBytes, StorageResult<ObjectVersion>)>,
) -> anyhow::Result<()> {
    let mut winner_opt: Option<(OwnedBytes, ObjectVersion)> = None;
    for (payload, result) in results {
        match result {
            Ok(version) => {
                anyhow::ensure!(
                    winner_opt.is_none(),
                    "more than one concurrent conditional write succeeded"
                );
                winner_opt = Some((payload, version));
            }
            Err(error) => {
                anyhow::ensure!(
                    error.kind() == StorageErrorKind::PreconditionFailed,
                    "losing writer failed with unexpected error: `{error}`"
                );
            }
        }
    }
    let (winner_payload, winner_version) =
        winner_opt.context("no concurrent conditional write succeeded")?;
    assert_object_is(storage, path, &winner_payload, &winner_version).await?;
    Ok(())
}

async fn test_concurrent_put_if_absent_has_one_winner(
    storage: Arc<dyn Storage>,
) -> anyhow::Result<()> {
    let path = Path::new("conditional_writes/concurrent_put_if_absent");
    let writes = (0..NUM_CONCURRENT_WRITERS).map(|writer_id| {
        let storage = storage.clone();
        let payload = OwnedBytes::new(format!("writer-{writer_id}").into_bytes());
        async move {
            let result = storage
                .put_if_absent(path, Box::new(payload.to_vec()))
                .await;
            (payload, result)
        }
    });
    let results = join_all(writes).await;
    assert_exactly_one_winner(&*storage, path, results).await?;

    storage.delete(path).await?;
    Ok(())
}

async fn test_concurrent_put_if_version_matches_has_one_winner(
    storage: Arc<dyn Storage>,
) -> anyhow::Result<()> {
    let path = Path::new("conditional_writes/concurrent_put_if_version_matches");
    storage.put(path, Box::new(b"initial".to_vec())).await?;
    let (_, initial_version) = storage.get_all_with_version(path).await?;

    let writes = (0..NUM_CONCURRENT_WRITERS).map(|writer_id| {
        let storage = storage.clone();
        let payload = OwnedBytes::new(format!("writer-{writer_id}").into_bytes());
        let initial_version = initial_version.clone();
        async move {
            let result = storage
                .put_if_version_matches(path, Box::new(payload.to_vec()), &initial_version)
                .await;
            (payload, result)
        }
    });
    let results = join_all(writes).await;
    assert_exactly_one_winner(&*storage, path, results).await?;

    storage.delete(path).await?;
    Ok(())
}

/// Test suite for storages that support conditional writes.
pub async fn storage_test_conditional_writes(storage: Arc<dyn Storage>) -> anyhow::Result<()> {
    test_put_if_absent_creates_once(&*storage)
        .await
        .context("put_if_absent_creates_once")?;
    test_put_if_version_matches_is_compare_and_swap(&*storage)
        .await
        .context("put_if_version_matches_is_compare_and_swap")?;
    test_unconditional_put_invalidates_version(&*storage)
        .await
        .context("unconditional_put_invalidates_version")?;
    test_put_if_version_matches_on_deleted_object(&*storage)
        .await
        .context("put_if_version_matches_on_deleted_object")?;
    test_get_all_with_version_missing_object(&*storage)
        .await
        .context("get_all_with_version_missing_object")?;
    test_concurrent_put_if_absent_has_one_winner(storage.clone())
        .await
        .context("concurrent_put_if_absent_has_one_winner")?;
    test_concurrent_put_if_version_matches_has_one_winner(storage)
        .await
        .context("concurrent_put_if_version_matches_has_one_winner")?;
    Ok(())
}

/// Test suite for storages that do not support conditional writes.
///
/// Such a storage must fail every conditional operation with
/// [`StorageErrorKind::Unsupported`], without writing anything: a caller relying on
/// compare-and-swap must learn that it does not get one, rather than silently getting an
/// unconditional write.
pub async fn storage_test_conditional_writes_unsupported(
    storage: &dyn Storage,
) -> anyhow::Result<()> {
    let path = Path::new("conditional_writes/unsupported");
    assert_error_kind(
        storage
            .put_if_absent(path, Box::new(b"payload".to_vec()))
            .await,
        StorageErrorKind::Unsupported,
    )
    .context("put_if_absent")?;
    anyhow::ensure!(
        !storage.exists(path).await?,
        "unsupported put_if_absent wrote the object anyway"
    );
    assert_error_kind(
        storage
            .put_if_version_matches(
                path,
                Box::new(b"payload".to_vec()),
                &ObjectVersion::new("fabricated"),
            )
            .await,
        StorageErrorKind::Unsupported,
    )
    .context("put_if_version_matches")?;
    anyhow::ensure!(
        !storage.exists(path).await?,
        "unsupported put_if_version_matches wrote the object anyway"
    );
    assert_error_kind(
        storage.get_all_with_version(path).await,
        StorageErrorKind::Unsupported,
    )
    .context("get_all_with_version")?;
    Ok(())
}

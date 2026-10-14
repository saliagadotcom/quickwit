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

//! Startup probe run before opening a file-backed metastore with `concurrent_writers: true`.
//!
//! The metastore relies on the storage rejecting conditional writes whose precondition does not
//! hold. A backend that merely accepts the conditional headers and ignores them (for instance
//! LocalStack before 4.14) would silently turn compare-and-swap into last-writer-wins, which is
//! exactly the data loss the mode is meant to prevent. Checking the trait implementation is
//! therefore not enough: the probe performs real conditional writes and verifies that the
//! storage enforces them.

use std::path::{Path, PathBuf};

use anyhow::{Context, bail};
use quickwit_storage::{Storage, StorageErrorKind};
use ulid::Ulid;

/// Returns a path for a probe object that no other node is writing to.
///
/// The object is written directly under the metastore root, next to the manifest, because that
/// is where the metastore needs conditional writes to work.
pub(super) fn new_probe_path() -> PathBuf {
    PathBuf::from(format!(".conditional-writes-probe-{}", Ulid::new()))
}

/// Verifies that `storage` supports and enforces conditional writes, using `probe_path` as
/// scratch object. The probe object is deleted on success.
///
/// The probe exercises both primitives used by the metastore:
/// 1. `put_if_absent` on a new path must succeed, and a second `put_if_absent` on the same path
///    must be rejected with `PreconditionFailed`.
/// 2. `put_if_version_matches` with the version returned by the first write must succeed and
///    produce a new version, after which the stale version must be rejected with
///    `PreconditionFailed`.
pub(super) async fn probe_conditional_writes(
    storage: &dyn Storage,
    probe_path: &Path,
) -> anyhow::Result<()> {
    let storage_uri = storage.uri();
    let first_version = match storage
        .put_if_absent(probe_path, Box::new(b"probe-1".to_vec()))
        .await
    {
        Ok(version) => version,
        Err(error) if error.kind() == StorageErrorKind::Unsupported => {
            bail!(
                "`concurrent_writers` requires a storage supporting conditional writes, but \
                 `{storage_uri}` does not: {error}"
            );
        }
        Err(error) => {
            return Err(error).with_context(|| {
                format!("failed to write conditional-writes probe object to `{storage_uri}`")
            });
        }
    };
    let probe_result = check_enforcement(storage, probe_path, first_version).await;
    let delete_result = storage
        .delete(probe_path)
        .await
        .with_context(|| format!("failed to delete probe object `{}`", probe_path.display()));
    probe_result?;
    delete_result
}

/// Runs the checks that need the probe object to exist. Split out so the caller can always
/// delete the probe object, whichever check fails.
async fn check_enforcement(
    storage: &dyn Storage,
    probe_path: &Path,
    first_version: quickwit_storage::ObjectVersion,
) -> anyhow::Result<()> {
    let storage_uri = storage.uri();
    match storage
        .put_if_absent(probe_path, Box::new(b"probe-2".to_vec()))
        .await
    {
        Ok(_) => bail!(
            "`concurrent_writers` requires a storage enforcing conditional writes, but \
             `{storage_uri}` overwrote an existing object on `put_if_absent`"
        ),
        Err(error) if error.kind() == StorageErrorKind::PreconditionFailed => {}
        Err(error) => {
            return Err(error).with_context(|| {
                format!("unexpected error from `put_if_absent` probe on `{storage_uri}`")
            });
        }
    }
    let second_version = storage
        .put_if_version_matches(probe_path, Box::new(b"probe-3".to_vec()), &first_version)
        .await
        .with_context(|| {
            format!(
                "`put_if_version_matches` probe with the current version failed on `{storage_uri}`"
            )
        })?;
    if second_version == first_version {
        bail!(
            "`concurrent_writers` requires a storage versioning objects, but `{storage_uri}` \
             returned the same version for two different contents"
        );
    }
    match storage
        .put_if_version_matches(probe_path, Box::new(b"probe-4".to_vec()), &first_version)
        .await
    {
        Ok(_) => bail!(
            "`concurrent_writers` requires a storage enforcing conditional writes, but \
             `{storage_uri}` accepted a `put_if_version_matches` with a stale version"
        ),
        Err(error) if error.kind() == StorageErrorKind::PreconditionFailed => Ok(()),
        Err(error) => Err(error).with_context(|| {
            format!("unexpected error from `put_if_version_matches` probe on `{storage_uri}`")
        }),
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use quickwit_common::uri::Uri;
    use quickwit_storage::{MockStorage, ObjectVersion, RamStorage};

    use super::*;

    #[tokio::test]
    async fn test_probe_passes_on_enforcing_storage_and_leaves_no_object_behind() {
        let storage = RamStorage::default();
        let probe_path = new_probe_path();
        probe_conditional_writes(&storage, &probe_path)
            .await
            .unwrap();
        assert!(!storage.exists(&probe_path).await.unwrap());
    }

    #[tokio::test]
    async fn test_probe_fails_when_conditional_writes_are_unsupported() {
        let mut mock_storage = MockStorage::default();
        mock_storage
            .expect_uri()
            .return_const(Uri::for_test("ram:///indexes"));
        // The default `Storage` implementation of the conditional primitives returns
        // `Unsupported`; the mock mirrors it. No `delete` is expected: nothing was written.
        mock_storage.expect_put_if_absent().returning(|_, _| {
            Err(StorageErrorKind::Unsupported.with_error(anyhow::anyhow!("not supported")))
        });
        let error = probe_conditional_writes(&mock_storage, Path::new("probe"))
            .await
            .unwrap_err();
        assert!(
            error.to_string().contains("supporting conditional writes"),
            "{error}"
        );
    }

    #[tokio::test]
    async fn test_probe_fails_when_storage_ignores_put_if_absent_precondition() {
        // Models a backend that accepts `If-None-Match` but does not enforce it: every
        // `put_if_absent` succeeds.
        let mut mock_storage = MockStorage::default();
        mock_storage
            .expect_uri()
            .return_const(Uri::for_test("s3://bucket/indexes"));
        mock_storage
            .expect_put_if_absent()
            .times(2)
            .returning(|_, _| Ok(ObjectVersion::new("etag")));
        mock_storage.expect_delete().times(1).returning(|_| Ok(()));
        let error = probe_conditional_writes(&mock_storage, Path::new("probe"))
            .await
            .unwrap_err();
        assert!(
            error.to_string().contains("overwrote an existing object"),
            "{error}"
        );
    }

    #[tokio::test]
    async fn test_probe_fails_when_storage_ignores_stale_version() {
        // Models a backend that enforces `If-None-Match` but ignores `If-Match`.
        let mut mock_storage = MockStorage::default();
        mock_storage
            .expect_uri()
            .return_const(Uri::for_test("s3://bucket/indexes"));
        let mut num_put_if_absent_calls = 0;
        mock_storage.expect_put_if_absent().returning(move |_, _| {
            num_put_if_absent_calls += 1;
            if num_put_if_absent_calls == 1 {
                Ok(ObjectVersion::new("v1"))
            } else {
                Err(StorageErrorKind::PreconditionFailed.with_error(anyhow::anyhow!("exists")))
            }
        });
        let mut num_cas_calls = 0;
        mock_storage
            .expect_put_if_version_matches()
            .returning(move |_, _, _| {
                num_cas_calls += 1;
                Ok(ObjectVersion::new(format!("v{}", num_cas_calls + 1)))
            });
        mock_storage.expect_delete().times(1).returning(|_| Ok(()));
        let error = probe_conditional_writes(&mock_storage, Path::new("probe"))
            .await
            .unwrap_err();
        assert!(error.to_string().contains("stale version"), "{error}");
    }

    #[tokio::test]
    async fn test_probe_deletes_object_even_when_a_check_fails() {
        // Wraps a RAM storage so that the stale-version check fails while deletion still runs
        // against the real object.
        let ram_storage = Arc::new(RamStorage::default());
        let mut mock_storage = MockStorage::default();
        mock_storage
            .expect_uri()
            .return_const(Uri::for_test("ram:///indexes"));
        let ram_storage_clone = ram_storage.clone();
        mock_storage
            .expect_put_if_absent()
            .returning(move |path, payload| {
                futures::executor::block_on(ram_storage_clone.put_if_absent(path, payload))
            });
        mock_storage
            .expect_put_if_version_matches()
            .returning(|_, _, _| Ok(ObjectVersion::new("always-new")));
        let ram_storage_clone = ram_storage.clone();
        mock_storage
            .expect_delete()
            .returning(move |path| futures::executor::block_on(ram_storage_clone.delete(path)));
        let probe_path = new_probe_path();
        probe_conditional_writes(&mock_storage, &probe_path)
            .await
            .unwrap_err();
        assert!(!ram_storage.exists(&probe_path).await.unwrap());
    }
}

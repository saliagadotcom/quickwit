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
use std::str::FromStr;
use std::sync::{Arc, LazyLock};
use std::time::Duration;

use async_trait::async_trait;
use quickwit_common::uri::Uri;
use quickwit_config::{FileMetastoreConfig, MetastoreBackend, MetastoreConfig};
use quickwit_proto::metastore::{MetastoreError, MetastoreServiceClient};
use quickwit_storage::{Storage, StorageResolver, StorageResolverError};
use regex::Regex;
use tokio::sync::Mutex;
use tracing::{debug, info};

use super::WriteMode;
use super::conditional_writes_probe::{new_probe_path, probe_conditional_writes};
use crate::{
    FileBackedMetastore, MetastoreFactory, MetastoreFactoryOptions, MetastoreResolverError,
};

/// A file-backed metastore factory.
///
/// The implementation ensures that there is only
/// one living instance of `FileBasedMetastore` per metastore URI.
/// As a result, within a same process as long as we keep a single
/// FileBasedMetastoreFactory, it is safe to use the file based
/// metastore, even from different threads.
#[derive(Clone)]
pub struct FileBackedMetastoreFactory {
    storage_resolver: StorageResolver,
    // We never garbage collect unused metastore client instances. This should not be a problem
    // because during normal use this cache will hold at most a single instance.
    cache: Arc<Mutex<HashMap<Uri, MetastoreServiceClient>>>,
}

fn extract_polling_interval_from_uri(uri: &str) -> (String, Option<Duration>) {
    static URI_FRAGMENT_PATTERN: LazyLock<Regex> =
        LazyLock::new(|| Regex::new("(.*)#polling_interval=([1-9][0-9]{0,8})s").unwrap());
    if let Some(captures) = URI_FRAGMENT_PATTERN.captures(uri) {
        let uri_without_fragment = captures.get(1).unwrap().as_str().to_string();
        let polling_interval_in_secs: u64 =
            captures.get(2).unwrap().as_str().parse::<u64>().unwrap();
        (
            uri_without_fragment,
            Some(Duration::from_secs(polling_interval_in_secs)),
        )
    } else {
        (uri.to_string(), None)
    }
}

/// Reconciles the polling interval given in the metastore URI fragment with the one given in the
/// node config. Both are accepted for backward compatibility, but they must agree.
fn resolve_polling_interval(
    polling_interval_from_uri: Option<Duration>,
    file_metastore_config: &FileMetastoreConfig,
) -> Result<Option<Duration>, MetastoreResolverError> {
    let polling_interval_from_config: Option<Duration> = file_metastore_config
        .polling_interval
        .as_ref()
        .map(|polling_interval| **polling_interval);
    match (polling_interval_from_uri, polling_interval_from_config) {
        (Some(from_uri), Some(from_config)) if from_uri != from_config => {
            Err(MetastoreResolverError::InvalidConfig(format!(
                "the metastore URI sets `polling_interval={}s` but the `metastore.file` config \
                 sets `polling_interval: {}`: remove one of them or make them agree",
                from_uri.as_secs(),
                file_metastore_config
                    .polling_interval
                    .as_ref()
                    .expect("checked above")
            )))
        }
        (from_uri, from_config) => Ok(from_uri.or(from_config)),
    }
}

impl FileBackedMetastoreFactory {
    /// Creates a new [`FileBackedMetastoreFactory`].
    pub fn new(storage_resolver: StorageResolver) -> Self {
        Self {
            storage_resolver,
            cache: Default::default(),
        }
    }

    async fn get_from_cache(&self, uri: &Uri) -> Option<MetastoreServiceClient> {
        self.cache.lock().await.get(uri).cloned()
    }

    /// If there is a valid entry in the cache to begin with, we ignore the new
    /// metastore and return the old one.
    ///
    /// This way we make sure that we keep only one instance associated
    /// to the key `uri` outside of this struct.
    async fn cache_metastore(
        &self,
        uri: Uri,
        metastore: MetastoreServiceClient,
    ) -> MetastoreServiceClient {
        self.cache
            .lock()
            .await
            .entry(uri)
            .or_insert(metastore)
            .clone()
    }
}

#[async_trait]
impl MetastoreFactory for FileBackedMetastoreFactory {
    fn backend(&self) -> MetastoreBackend {
        MetastoreBackend::File
    }

    async fn resolve(
        &self,
        metastore_config: &MetastoreConfig,
        uri: &Uri,
        _options: MetastoreFactoryOptions,
    ) -> Result<MetastoreServiceClient, MetastoreResolverError> {
        let file_metastore_config = metastore_config.as_file().ok_or_else(|| {
            MetastoreResolverError::InvalidConfig(format!(
                "expected a file metastore config, got `{:?}`",
                metastore_config.backend()
            ))
        })?;
        let (uri_stripped, polling_interval_from_uri) =
            extract_polling_interval_from_uri(uri.as_str());
        let polling_interval_opt =
            resolve_polling_interval(polling_interval_from_uri, file_metastore_config)?;
        let write_mode = if file_metastore_config.concurrent_writers {
            WriteMode::Conditional
        } else {
            WriteMode::Unconditional
        };
        let uri = Uri::from_str(&uri_stripped).map_err(|_| {
            MetastoreResolverError::InvalidConfig(format!("invalid URI: `{uri_stripped}`"))
        })?;
        if let Some(metastore) = self.get_from_cache(&uri).await {
            debug!("using metastore from cache");
            return Ok(metastore);
        }
        debug!("metastore not found in cache");
        let storage = self
            .storage_resolver
            .resolve(&uri)
            .await
            .map_err(|err| match err {
                StorageResolverError::InvalidConfig(message) => {
                    MetastoreResolverError::InvalidConfig(message)
                }
                StorageResolverError::InvalidUri(message) => {
                    MetastoreResolverError::InvalidUri(message)
                }
                StorageResolverError::UnsupportedBackend(message) => {
                    MetastoreResolverError::UnsupportedBackend(message)
                }
                StorageResolverError::FailedToOpenStorage { kind, message } => {
                    MetastoreResolverError::Initialization(MetastoreError::Internal {
                        message: format!("failed to open metastore file `{uri}`"),
                        cause: format!("StorageError {kind:?}: {message}"),
                    })
                }
            })?;
        if write_mode == WriteMode::Conditional {
            ensure_conditional_writes_enforced(&*storage).await?;
            info!(metastore_uri=%uri, "file-backed metastore opened with concurrent writers enabled");
        }
        let file_backed_metastore =
            FileBackedMetastore::try_new(storage, polling_interval_opt, write_mode)
                .await
                .map(MetastoreServiceClient::new)
                .map_err(MetastoreResolverError::Initialization)?;
        let unique_metastore_for_uri = self.cache_metastore(uri, file_backed_metastore).await;
        Ok(unique_metastore_for_uri)
    }
}

/// Fails with [`MetastoreResolverError::InvalidConfig`] if `storage` does not support or does not
/// enforce conditional writes, so that a node configured with `concurrent_writers: true` on an
/// unsuitable storage refuses to start instead of silently losing metastore updates.
async fn ensure_conditional_writes_enforced(
    storage: &dyn Storage,
) -> Result<(), MetastoreResolverError> {
    probe_conditional_writes(storage, &new_probe_path())
        .await
        .map_err(|error| MetastoreResolverError::InvalidConfig(format!("{error:#}")))
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;
    use std::time::Duration;

    use quickwit_common::uri::Uri;
    use quickwit_config::{
        HumanDuration, MetastoreConfig, PostgresMetastoreConfig, StorageBackend,
    };
    use quickwit_storage::{MockStorage, MockStorageFactory, RamStorage, StorageErrorKind};

    use super::*;

    fn file_config(
        concurrent_writers: bool,
        polling_interval: Option<&str>,
    ) -> FileMetastoreConfig {
        FileMetastoreConfig {
            concurrent_writers,
            polling_interval: polling_interval.map(|polling_interval| {
                HumanDuration::try_from(polling_interval.to_string()).unwrap()
            }),
        }
    }

    #[test]
    fn test_resolve_polling_interval() {
        let thirty_secs = Some(Duration::from_secs(30));
        assert_eq!(
            resolve_polling_interval(None, &file_config(false, None)).unwrap(),
            None
        );
        assert_eq!(
            resolve_polling_interval(thirty_secs, &file_config(false, None)).unwrap(),
            thirty_secs
        );
        assert_eq!(
            resolve_polling_interval(None, &file_config(false, Some("30s"))).unwrap(),
            thirty_secs
        );
        assert_eq!(
            resolve_polling_interval(thirty_secs, &file_config(false, Some("30s"))).unwrap(),
            thirty_secs
        );
        let error =
            resolve_polling_interval(thirty_secs, &file_config(false, Some("1min"))).unwrap_err();
        assert!(
            matches!(error, MetastoreResolverError::InvalidConfig(message) if message.contains("polling_interval"))
        );
    }

    #[tokio::test]
    async fn test_resolve_rejects_non_file_config() {
        let factory = FileBackedMetastoreFactory::new(StorageResolver::for_test());
        let error = factory
            .resolve(
                &MetastoreConfig::PostgreSQL(PostgresMetastoreConfig::default()),
                &Uri::for_test("ram:///indexes"),
                MetastoreFactoryOptions::default(),
            )
            .await
            .unwrap_err();
        assert!(matches!(error, MetastoreResolverError::InvalidConfig(_)));
    }

    /// Builds a storage resolver that hands out `storage` for every `ram://` URI.
    fn storage_resolver_for(storage: Arc<dyn Storage>) -> StorageResolver {
        let mut mock_storage_factory = MockStorageFactory::new();
        mock_storage_factory
            .expect_backend()
            .returning(|| StorageBackend::Ram);
        mock_storage_factory
            .expect_resolve()
            .returning(move |_| Ok(storage.clone()));
        StorageResolver::builder()
            .register(mock_storage_factory)
            .build()
            .unwrap()
    }

    #[tokio::test]
    async fn test_resolve_with_concurrent_writers_on_enforcing_storage() {
        let ram_storage = Arc::new(RamStorage::default());
        let factory = FileBackedMetastoreFactory::new(storage_resolver_for(ram_storage.clone()));
        factory
            .resolve(
                &file_config(true, Some("10s")).into(),
                &Uri::for_test("ram:///indexes"),
                MetastoreFactoryOptions::default(),
            )
            .await
            .unwrap();
        // The metastore was initialized, and the probe object did not pollute the metastore root.
        assert_eq!(
            ram_storage.list_files().await,
            vec![PathBuf::from("manifest.json")]
        );
    }

    #[tokio::test]
    async fn test_resolve_with_concurrent_writers_fails_when_storage_is_unsupported() {
        let mut mock_storage = MockStorage::default();
        mock_storage
            .expect_uri()
            .return_const(Uri::for_test("ram:///indexes"));
        mock_storage.expect_put_if_absent().returning(|_, _| {
            Err(StorageErrorKind::Unsupported.with_error(anyhow::anyhow!("unsupported")))
        });
        let factory = FileBackedMetastoreFactory::new(storage_resolver_for(Arc::new(mock_storage)));
        let error = factory
            .resolve(
                &file_config(true, None).into(),
                &Uri::for_test("ram:///indexes"),
                MetastoreFactoryOptions::default(),
            )
            .await
            .unwrap_err();
        assert!(
            matches!(&error, MetastoreResolverError::InvalidConfig(message) if message.contains("conditional writes")),
            "{error:?}"
        );
    }

    #[test]
    fn test_extract_polling_interval_from_uri() {
        assert_eq!(
            extract_polling_interval_from_uri("file://some-uri#polling_interval=23s"),
            ("file://some-uri".to_string(), Some(Duration::from_secs(23)))
        );
        assert_eq!(
            extract_polling_interval_from_uri(
                "file://some-uri#polling_interval=18446744073709551616s"
            ),
            (
                "file://some-uri#polling_interval=18446744073709551616s".to_string(),
                None
            )
        );
        assert_eq!(
            extract_polling_interval_from_uri("file://some-uri#polling_interval=0s"),
            ("file://some-uri#polling_interval=0s".to_string(), None)
        );
        assert_eq!(
            extract_polling_interval_from_uri("file://some-uri#otherfragment#polling_interval=10s"),
            (
                "file://some-uri#otherfragment".to_string(),
                Some(Duration::from_secs(10))
            )
        );
    }
}

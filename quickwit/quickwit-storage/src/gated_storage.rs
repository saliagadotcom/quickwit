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

//! Test-only storage wrapper that lets a test park writes to chosen paths.
//!
//! This is used to reproduce interleavings deterministically: a test closes the gate for a path,
//! starts operations that write to it, waits until their writes are parked, and then decides
//! what happens in the meantime and in which order the parked writes land.

use std::collections::{HashMap, HashSet};
use std::fmt;
use std::ops::Range;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use quickwit_common::uri::Uri;
use tokio::io::AsyncRead;
use tokio::sync::watch;

use crate::storage::SendableAsync;
use crate::{
    BulkDeleteError, ListObjectsStream, ObjectVersion, OwnedBytes, PutPayload, Storage,
    StorageResult,
};

type WriterId = u64;

#[derive(Default)]
struct GateState {
    closed_paths: HashSet<PathBuf>,
    /// Writers parked on each closed path, in parking order.
    parked_writers: HashMap<PathBuf, Vec<WriterId>>,
    /// Writers let through individually by [`Gate::release_one`] whose write has not landed yet.
    released_writers: HashSet<WriterId>,
    next_writer_id: WriterId,
}

struct GateInner {
    state: Mutex<GateState>,
    /// Bumped on every state change so that waiters re-check their condition.
    generation_tx: watch::Sender<u64>,
}

impl GateInner {
    fn update<T>(&self, update_fn: impl FnOnce(&mut GateState) -> T) -> T {
        let result = update_fn(&mut self.state.lock().unwrap());
        self.generation_tx
            .send_modify(|generation| *generation += 1);
        result
    }

    /// Waits until `condition` holds. The receiver is subscribed before the first check so a
    /// state change between the check and the wait cannot be missed.
    async fn wait_until(&self, condition: impl Fn(&GateState) -> bool) {
        let mut generation_rx = self.generation_tx.subscribe();
        loop {
            if condition(&self.state.lock().unwrap()) {
                return;
            }
            generation_rx
                .changed()
                .await
                .expect("the gate sender lives as long as the gate inner");
        }
    }
}

/// Controls which paths of a [`GatedStorage`] accept writes.
#[derive(Clone)]
pub struct Gate {
    inner: Arc<GateInner>,
}

impl Gate {
    /// Parks every subsequent write to `path` until it is opened or released.
    pub fn close(&self, path: &Path) {
        self.inner.update(|state| {
            state.closed_paths.insert(path.to_path_buf());
        });
    }

    /// Lets all parked and subsequent writes to `path` through.
    pub fn open(&self, path: &Path) {
        self.inner.update(|state| {
            state.closed_paths.remove(path);
        });
    }

    /// Resolves once at least one writer is parked on `path`.
    pub async fn wait_until_blocked(&self, path: &Path) {
        self.wait_until_num_blocked(path, 1).await;
    }

    /// Resolves once at least `num_writers` writers are parked on `path`.
    pub async fn wait_until_num_blocked(&self, path: &Path, num_writers: usize) {
        self.inner
            .wait_until(|state| match state.parked_writers.get(path) {
                Some(writers) => writers.len() >= num_writers,
                None => num_writers == 0,
            })
            .await;
    }

    /// Lets the most recently parked writer on `path` through and resolves once its write has
    /// landed in the underlying storage. `path` stays closed for the other writers.
    ///
    /// Releasing in reverse parking order is what lets a test land a write that was prepared
    /// earlier, from staler data, after a write prepared later.
    ///
    /// # Panics
    ///
    /// Panics if no writer is parked on `path`.
    pub async fn release_one(&self, path: &Path) {
        let writer_id = self.inner.update(|state| {
            let writer_id = state
                .parked_writers
                .get_mut(path)
                .and_then(|writers| writers.pop())
                .unwrap_or_else(|| panic!("no writer is parked on `{}`", path.display()));
            state.released_writers.insert(writer_id);
            writer_id
        });
        self.inner
            .wait_until(|state| !state.released_writers.contains(&writer_id))
            .await;
    }
}

/// Proof that a writer went through the gate; marks its write as landed when dropped.
struct GatePass {
    gate: Arc<GateInner>,
    writer_id: WriterId,
}

impl Drop for GatePass {
    fn drop(&mut self) {
        self.gate.update(|state| {
            state.released_writers.remove(&self.writer_id);
        });
    }
}

/// Storage proxy whose writes to closed paths park until they are let through.
///
/// Reads and deletes are never gated.
pub struct GatedStorage {
    inner: Arc<dyn Storage>,
    gate: Gate,
}

impl GatedStorage {
    /// Wraps `inner`. All paths start open.
    pub fn new(inner: Arc<dyn Storage>) -> (GatedStorage, Gate) {
        let (generation_tx, _generation_rx) = watch::channel(0u64);
        let gate = Gate {
            inner: Arc::new(GateInner {
                state: Mutex::default(),
                generation_tx,
            }),
        };
        let storage = GatedStorage {
            inner,
            gate: gate.clone(),
        };
        (storage, gate)
    }

    /// Returns immediately if `path` is open, otherwise parks until the path is opened or this
    /// writer is released. The returned pass must be held until the write has landed.
    async fn pass_gate(&self, path: &Path) -> GatePass {
        let gate = &self.gate.inner;
        let writer_id = gate.update(|state| {
            let writer_id = state.next_writer_id;
            state.next_writer_id += 1;
            if state.closed_paths.contains(path) {
                state
                    .parked_writers
                    .entry(path.to_path_buf())
                    .or_default()
                    .push(writer_id);
            }
            writer_id
        });
        gate.wait_until(|state| {
            !state.closed_paths.contains(path) || state.released_writers.contains(&writer_id)
        })
        .await;
        gate.update(|state| {
            if let Some(writers) = state.parked_writers.get_mut(path) {
                writers.retain(|parked_writer_id| *parked_writer_id != writer_id);
            }
        });
        GatePass {
            gate: gate.clone(),
            writer_id,
        }
    }
}

impl fmt::Debug for GatedStorage {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("GatedStorage")
            .field("inner", &self.inner)
            .finish()
    }
}

#[async_trait]
impl Storage for GatedStorage {
    async fn check_connectivity(&self) -> anyhow::Result<()> {
        self.inner.check_connectivity().await
    }

    async fn put(&self, path: &Path, payload: Box<dyn PutPayload>) -> StorageResult<()> {
        let _pass = self.pass_gate(path).await;
        self.inner.put(path, payload).await
    }

    async fn put_if_absent(
        &self,
        path: &Path,
        payload: Box<dyn PutPayload>,
    ) -> StorageResult<ObjectVersion> {
        let _pass = self.pass_gate(path).await;
        self.inner.put_if_absent(path, payload).await
    }

    async fn put_if_version_matches(
        &self,
        path: &Path,
        payload: Box<dyn PutPayload>,
        expected_version: &ObjectVersion,
    ) -> StorageResult<ObjectVersion> {
        let _pass = self.pass_gate(path).await;
        self.inner
            .put_if_version_matches(path, payload, expected_version)
            .await
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

    fn list(&self, prefix: &Path) -> ListObjectsStream {
        self.inner.list(prefix)
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

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;
    use crate::RamStorage;

    fn gated_ram_storage() -> (Arc<GatedStorage>, Gate) {
        let (storage, gate) = GatedStorage::new(Arc::new(RamStorage::default()));
        (Arc::new(storage), gate)
    }

    fn spawn_put(
        storage: &Arc<GatedStorage>,
        path: &'static Path,
        content: &'static [u8],
    ) -> tokio::task::JoinHandle<StorageResult<()>> {
        let storage = storage.clone();
        tokio::spawn(async move { storage.put(path, Box::new(content.to_vec())).await })
    }

    #[tokio::test]
    async fn test_gated_storage_parks_only_closed_paths() {
        let (storage, gate) = gated_ram_storage();
        let closed = Path::new("closed");
        gate.close(closed);

        storage
            .put(Path::new("open"), Box::new(b"ok".to_vec()))
            .await
            .unwrap();

        let parked_put = spawn_put(&storage, closed, b"late");
        gate.wait_until_blocked(closed).await;
        assert!(!storage.exists(closed).await.unwrap());

        gate.open(closed);
        tokio::time::timeout(Duration::from_secs(5), parked_put)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert_eq!(&storage.get_all(closed).await.unwrap()[..], b"late");

        // Once open, writes go straight through.
        storage
            .put(closed, Box::new(b"again".to_vec()))
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn test_gated_storage_release_one_lands_most_recent_writer_first() {
        let (storage, gate) = gated_ram_storage();
        let path = Path::new("contended");
        gate.close(path);

        let first_put = spawn_put(&storage, path, b"first");
        gate.wait_until_blocked(path).await;
        let second_put = spawn_put(&storage, path, b"second");
        tokio::time::sleep(Duration::from_millis(50)).await;

        gate.release_one(path).await;
        assert_eq!(&storage.get_all(path).await.unwrap()[..], b"second");
        assert!(!first_put.is_finished());

        gate.release_one(path).await;
        assert_eq!(&storage.get_all(path).await.unwrap()[..], b"first");
        first_put.await.unwrap().unwrap();
        second_put.await.unwrap().unwrap();
    }
}

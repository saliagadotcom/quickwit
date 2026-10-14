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

//! Retry policy and metrics for conditional writes that lost a race against another writer.

use std::time::Duration;

use quickwit_common::retry::RetryParams;
use quickwit_metrics::{LazyCounter, counter, lazy_counter};
use quickwit_proto::metastore::{MetastoreError, MetastoreResult};
use tracing::debug;

/// Maximum number of times one operation retries after its conditional write lost a race.
///
/// With the backoff below, an operation that keeps losing gives up after roughly three seconds
/// of waiting, plus the time of its reloads and writes. This stays under the request timeout of
/// the metastore client, whose retry layer then takes over.
const MAX_CONFLICT_RETRIES: usize = 16;

/// Jittered exponential backoff between two attempts: 2.5–5 ms after the first conflict, capped
/// at 125–250 ms.
const CONFLICT_BACKOFF: RetryParams = RetryParams {
    base_delay: Duration::from_millis(5),
    max_delay: Duration::from_millis(250),
    max_attempts: MAX_CONFLICT_RETRIES,
};

static CONFLICTS_TOTAL: LazyCounter = lazy_counter!(
        name: "conditional_write_conflicts_total",
        description: "Number of conditional writes of the file-backed metastore that lost a race against another writer, by object.",
        subsystem: "metastore",
);

static CONFLICT_RETRIES_EXHAUSTED_TOTAL: LazyCounter = lazy_counter!(
        name: "conditional_write_retries_exhausted_total",
        description: "Number of file-backed metastore operations that gave up after their conditional write lost too many races in a row, by object.",
        subsystem: "metastore",
);

/// Kind of file the file-backed metastore writes conditionally.
#[derive(Clone, Copy, Debug)]
pub(super) enum ConditionalObject {
    Index,
    Manifest,
}

impl ConditionalObject {
    fn as_str(self) -> &'static str {
        match self {
            ConditionalObject::Index => "index",
            ConditionalObject::Manifest => "manifest",
        }
    }
}

/// Tracks the conflicts of one operation and decides whether and how long it retries.
pub(super) struct ConflictRetry {
    object: ConditionalObject,
    /// Human-readable name of the object being written, for logs and errors.
    object_name: String,
    num_conflicts: usize,
}

impl ConflictRetry {
    pub fn new(object: ConditionalObject, object_name: impl Into<String>) -> Self {
        Self {
            object,
            object_name: object_name.into(),
            num_conflicts: 0,
        }
    }

    /// Records that the conditional write lost a race, then waits for a jittered, exponentially
    /// growing delay so that the caller can reload the object and retry.
    ///
    /// Fails with [`MetastoreError::Unavailable`] once the retry budget is spent. Nothing was
    /// written in that case, and the error is retryable by the metastore client.
    pub async fn wait_before_retry(&mut self) -> MetastoreResult<()> {
        self.num_conflicts += 1;
        counter!(parent: CONFLICTS_TOTAL, "object" => self.object.as_str()).inc();

        if self.num_conflicts > MAX_CONFLICT_RETRIES {
            counter!(parent: CONFLICT_RETRIES_EXHAUSTED_TOTAL, "object" => self.object.as_str())
                .inc();
            return Err(MetastoreError::Unavailable(format!(
                "{} was modified concurrently {} times in a row, giving up",
                self.object_name, self.num_conflicts
            )));
        }
        let delay = CONFLICT_BACKOFF.compute_delay(self.num_conflicts);
        debug!(
            object = %self.object_name,
            num_conflicts = self.num_conflicts,
            delay_ms = delay.as_millis(),
            "conditional write lost a race, retrying"
        );
        tokio::time::sleep(delay).await;
        Ok(())
    }
}

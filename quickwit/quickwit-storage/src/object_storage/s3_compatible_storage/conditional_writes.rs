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

//! Conditional writes (`If-None-Match: *` / `If-Match: <etag>`) and versioned reads for
//! [`S3CompatibleObjectStorage`].
//!
//! See <https://docs.aws.amazon.com/AmazonS3/latest/userguide/conditional-writes.html>.

use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};

use anyhow::anyhow;
use quickwit_aws::retry::aws_retry;
use quickwit_common::retry::Retry;
use tracing::info;

use super::{REQUEST_SEMAPHORE, S3CompatibleObjectStorage, download_all};
use crate::stable_deref_bytes::into_owned_bytes;
use crate::{ObjectVersion, OwnedBytes, PutPayload, StorageError, StorageErrorKind, StorageResult};

/// Precondition attached to a `PutObject` request.
pub(super) enum PutPrecondition<'a> {
    /// `If-None-Match: *`: the write succeeds only if no object exists at the key.
    Absent,
    /// `If-Match: <etag>`: the write succeeds only if the current object has this ETag.
    MatchVersion(&'a ObjectVersion),
}

impl S3CompatibleObjectStorage {
    /// Conditional writes are only implemented for single-part uploads. Payloads large enough
    /// to require a multipart upload are rejected with `Unsupported` rather than silently
    /// written without the precondition.
    ///
    /// A `PreconditionFailed` outcome is not counted in `OBJECT_STORAGE_PUT_ERRORS_TOTAL`: it is
    /// the expected way for a compare-and-swap to lose a race, not a storage failure.
    pub(super) async fn put_conditional(
        &self,
        path: &Path,
        payload: Box<dyn PutPayload>,
        precondition: PutPrecondition<'_>,
    ) -> StorageResult<ObjectVersion> {
        crate::metrics::OBJECT_STORAGE_PUT_TOTAL.inc();
        let _permit = REQUEST_SEMAPHORE.acquire().await;
        let key = self.key(path);
        let total_len = payload.len();
        let part_num_bytes = self.multipart_policy.part_num_bytes(total_len);
        if !self.disable_multipart_upload && part_num_bytes < total_len {
            return Err(StorageErrorKind::Unsupported.with_error(anyhow!(
                "conditional write of `{key}` ({total_len} bytes) would require a multipart \
                 upload, which is not supported"
            )));
        }
        // Set when at least one attempt failed with a transient error. In that case, the server
        // may have applied a write whose response we never saw, so a subsequent
        // `PreconditionFailed` is ambiguous and must be disambiguated by reading back the object.
        let had_transient_failure = AtomicBool::new(false);
        let put_result: StorageResult<Option<ObjectVersion>> =
            aws_retry(&self.retry_params, || async {
                let attempt_result = self
                    .put_single_part_single_try(
                        &self.bucket,
                        &key,
                        payload.clone(),
                        total_len,
                        Some(&precondition),
                    )
                    .await;
                if matches!(attempt_result, Err(Retry::Transient(_))) {
                    had_transient_failure.store(true, Ordering::Relaxed);
                }
                attempt_result
            })
            .await
            .map_err(Retry::into_inner);

        let version_opt = match put_result {
            Ok(version_opt) => version_opt,
            Err(error) if error.kind() == StorageErrorKind::PreconditionFailed => {
                if !had_transient_failure.load(Ordering::Relaxed) {
                    return Err(error);
                }
                return self
                    .recover_from_possible_lost_response(path, &*payload, error)
                    .await;
            }
            Err(error) => {
                crate::metrics::OBJECT_STORAGE_PUT_ERRORS_TOTAL.inc();
                return Err(error);
            }
        };
        version_opt.ok_or_else(|| {
            StorageErrorKind::Internal.with_error(anyhow!(
                "PutObject response for `{}/{}` did not include an ETag, which is required for \
                 conditional writes",
                self.uri,
                path.display()
            ))
        })
    }

    /// Called when a conditional PUT was rejected with `PreconditionFailed` after an earlier
    /// attempt failed transiently. If the object now holds exactly the bytes we tried to write,
    /// our lost write did go through and the operation is reported as a success with the
    /// object's current version. Otherwise, the original `PreconditionFailed` error is
    /// returned.
    ///
    /// If the read-back itself fails, that failure is returned instead: the caller cannot know
    /// whether its write was applied, and must not treat the outcome as a plain conflict.
    async fn recover_from_possible_lost_response(
        &self,
        path: &Path,
        payload: &dyn PutPayload,
        precondition_failed_error: StorageError,
    ) -> StorageResult<ObjectVersion> {
        let (current_bytes, current_version) = self
            .get_all_with_version_inner(path)
            .await
            .map_err(|error| {
                error.add_context(format!(
                    "conditional write of `{}/{}` was rejected after a transient failure, and \
                     reading the object back to check whether the write was applied failed too",
                    self.uri,
                    path.display()
                ))
            })?;
        let expected_bytes = payload.read_all().await?;
        if current_bytes.as_slice() != expected_bytes.as_slice() {
            return Err(precondition_failed_error);
        }
        info!(
            path = %path.display(),
            version = %current_version,
            "conditional write rejected after a transient failure, but the object holds the \
             written bytes: treating the lost response as a success"
        );
        Ok(current_version)
    }

    /// Fetches the whole object together with its ETag. The caller is responsible for holding a
    /// `REQUEST_SEMAPHORE` permit.
    pub(super) async fn get_all_with_version_inner(
        &self,
        path: &Path,
    ) -> StorageResult<(OwnedBytes, ObjectVersion)> {
        let get_object_output = aws_retry(&self.retry_params, || self.get_object(path, None))
            .await
            .map_err(|error| {
                StorageError::from(error).add_context(format!(
                    "failed to fetch object: {}/{}",
                    self.uri,
                    path.display()
                ))
            })?;
        let e_tag = get_object_output.e_tag().ok_or_else(|| {
            StorageErrorKind::Internal.with_error(anyhow!(
                "GetObject response for `{}/{}` did not include an ETag, which is required for \
                 versioned reads",
                self.uri,
                path.display()
            ))
        })?;
        let version = ObjectVersion::new(e_tag);
        let bytes = download_all(get_object_output.body)
            .await
            .map(into_owned_bytes)?;
        Ok((bytes, version))
    }
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use aws_sdk_s3::primitives::SdkBody;
    use aws_smithy_runtime::client::http::test_util::{ReplayEvent, StaticReplayClient};
    use bytesize::ByteSize;
    use hyper::http;
    use quickwit_config::ChecksumAlgorithm;

    use super::super::tests::{make_s3_storage_with_replay, ok_put_response};
    use crate::{MultiPartPolicy, ObjectVersion, Storage, StorageErrorKind};

    const ETAG: &str = "\"d41d8cd98f00b204e9800998ecf8427e\"";

    fn replay_event(response: http::Response<SdkBody>) -> ReplayEvent {
        // The replay client does not validate the recorded request against the actual one, so
        // an empty request is enough. Assertions on the actual requests are made through
        // `StaticReplayClient::actual_requests`.
        ReplayEvent::new(
            http::Request::builder().body(SdkBody::empty()).unwrap(),
            response,
        )
    }

    fn ok_put_response_with_etag(e_tag: &str) -> ReplayEvent {
        replay_event(
            http::Response::builder()
                .status(200)
                .header("ETag", e_tag)
                .body(SdkBody::empty())
                .unwrap(),
        )
    }

    fn ok_get_response(e_tag_opt: Option<&str>, body: &[u8]) -> ReplayEvent {
        let mut response_builder = http::Response::builder()
            .status(200)
            .header("Content-Length", body.len().to_string());
        if let Some(e_tag) = e_tag_opt {
            response_builder = response_builder.header("ETag", e_tag);
        }
        replay_event(response_builder.body(SdkBody::from(body.to_vec())).unwrap())
    }

    fn error_response(status: u16, code: &str) -> ReplayEvent {
        replay_event(
            http::Response::builder()
                .status(status)
                .body(SdkBody::from(format!(
                    r#"<?xml version="1.0" encoding="UTF-8"?>
                    <Error>
                      <Code>{code}</Code>
                      <Message>message</Message>
                      <Resource>/my-path</Resource>
                      <RequestId>4442587FB7D0A2F9</RequestId>
                    </Error>"#
                )))
                .unwrap(),
        )
    }

    #[tokio::test]
    async fn test_put_if_absent_sends_if_none_match_and_returns_etag() {
        let client = StaticReplayClient::new(vec![ok_put_response_with_etag(ETAG)]);
        let s3_storage = make_s3_storage_with_replay(client.clone(), ChecksumAlgorithm::Crc32c);

        let version = s3_storage
            .put_if_absent(Path::new("my-path"), Box::new(b"payload".to_vec()))
            .await
            .unwrap();
        assert_eq!(version, ObjectVersion::new(ETAG));

        let requests: Vec<_> = client.actual_requests().collect();
        assert_eq!(requests.len(), 1);
        let headers = requests[0].headers();
        assert_eq!(headers.get("if-none-match"), Some("*"));
        assert!(headers.get("if-match").is_none());
    }

    #[tokio::test]
    async fn test_put_if_version_matches_sends_if_match() {
        let new_etag = "\"new-etag\"";
        let client = StaticReplayClient::new(vec![ok_put_response_with_etag(new_etag)]);
        let s3_storage = make_s3_storage_with_replay(client.clone(), ChecksumAlgorithm::Crc32c);

        let version = s3_storage
            .put_if_version_matches(
                Path::new("my-path"),
                Box::new(b"payload".to_vec()),
                &ObjectVersion::new(ETAG),
            )
            .await
            .unwrap();
        assert_eq!(version, ObjectVersion::new(new_etag));

        let requests: Vec<_> = client.actual_requests().collect();
        assert_eq!(requests.len(), 1);
        let headers = requests[0].headers();
        assert_eq!(headers.get("if-match"), Some(ETAG));
        assert!(headers.get("if-none-match").is_none());
    }

    #[tokio::test]
    async fn test_precondition_failed_is_not_retried() {
        let client = StaticReplayClient::new(vec![error_response(412, "PreconditionFailed")]);
        let s3_storage = make_s3_storage_with_replay(client.clone(), ChecksumAlgorithm::Crc32c);

        let error = s3_storage
            .put_if_absent(Path::new("my-path"), Box::new(b"payload".to_vec()))
            .await
            .unwrap_err();
        assert_eq!(error.kind(), StorageErrorKind::PreconditionFailed);
        // No retry, and no read-back since no attempt failed transiently.
        assert_eq!(client.actual_requests().count(), 1);
    }

    #[tokio::test]
    async fn test_conditional_request_conflict_maps_to_precondition_failed() {
        let client =
            StaticReplayClient::new(vec![error_response(409, "ConditionalRequestConflict")]);
        let s3_storage = make_s3_storage_with_replay(client.clone(), ChecksumAlgorithm::Crc32c);

        let error = s3_storage
            .put_if_version_matches(
                Path::new("my-path"),
                Box::new(b"payload".to_vec()),
                &ObjectVersion::new(ETAG),
            )
            .await
            .unwrap_err();
        assert_eq!(error.kind(), StorageErrorKind::PreconditionFailed);
        assert_eq!(client.actual_requests().count(), 1);
    }

    #[tokio::test]
    async fn test_put_if_version_matches_on_deleted_object_is_not_found() {
        let client = StaticReplayClient::new(vec![error_response(404, "NoSuchKey")]);
        let s3_storage = make_s3_storage_with_replay(client.clone(), ChecksumAlgorithm::Crc32c);

        let error = s3_storage
            .put_if_version_matches(
                Path::new("my-path"),
                Box::new(b"payload".to_vec()),
                &ObjectVersion::new(ETAG),
            )
            .await
            .unwrap_err();
        assert_eq!(error.kind(), StorageErrorKind::NotFound);
    }

    #[tokio::test]
    async fn test_conditional_put_rejects_multipart_sized_payload_without_any_request() {
        let client = StaticReplayClient::new(vec![ok_put_response()]);
        let mut s3_storage = make_s3_storage_with_replay(client.clone(), ChecksumAlgorithm::Crc32c);
        s3_storage.disable_multipart_upload = false;
        s3_storage.multipart_policy = MultiPartPolicy {
            target_part_num_bytes: ByteSize::b(4),
            max_num_parts: 10_000,
            multipart_threshold_num_bytes: ByteSize::b(8),
            max_object_num_bytes: ByteSize::tb(5),
            max_concurrent_uploads: 100,
        };

        let error = s3_storage
            .put_if_absent(Path::new("my-path"), Box::new(vec![0u8; 64]))
            .await
            .unwrap_err();
        assert_eq!(error.kind(), StorageErrorKind::Unsupported);
        assert_eq!(client.actual_requests().count(), 0);

        // The same payload goes through the unconditional path, which does support multipart.
        // Guard against the test passing only because the policy rejects everything.
        assert!(s3_storage.multipart_policy.part_num_bytes(64) < 64);
    }

    #[tokio::test]
    async fn test_lost_response_is_recovered_when_object_holds_written_bytes() {
        let payload = b"payload".to_vec();
        let client = StaticReplayClient::new(vec![
            // The write is applied server-side but the response is lost.
            error_response(500, "InternalError"),
            // The retry is rejected because the object now exists.
            error_response(412, "PreconditionFailed"),
            // Read-back: the object holds exactly what we wrote.
            ok_get_response(Some(ETAG), &payload),
        ]);
        let s3_storage = make_s3_storage_with_replay(client.clone(), ChecksumAlgorithm::Crc32c);

        let version = s3_storage
            .put_if_absent(Path::new("my-path"), Box::new(payload))
            .await
            .unwrap();
        assert_eq!(version, ObjectVersion::new(ETAG));

        let requests: Vec<_> = client.actual_requests().collect();
        assert_eq!(requests.len(), 3);
        assert_eq!(requests[0].method(), http::Method::PUT);
        assert_eq!(requests[1].method(), http::Method::PUT);
        assert_eq!(requests[2].method(), http::Method::GET);
    }

    #[tokio::test]
    async fn test_lost_response_with_different_bytes_is_a_real_conflict() {
        let client = StaticReplayClient::new(vec![
            error_response(500, "InternalError"),
            error_response(412, "PreconditionFailed"),
            ok_get_response(Some(ETAG), b"someone else's payload"),
        ]);
        let s3_storage = make_s3_storage_with_replay(client.clone(), ChecksumAlgorithm::Crc32c);

        let error = s3_storage
            .put_if_absent(Path::new("my-path"), Box::new(b"payload".to_vec()))
            .await
            .unwrap_err();
        assert_eq!(error.kind(), StorageErrorKind::PreconditionFailed);
        assert_eq!(client.actual_requests().count(), 3);
    }

    #[tokio::test]
    async fn test_lost_response_read_back_failure_is_propagated() {
        let client = StaticReplayClient::new(vec![
            error_response(500, "InternalError"),
            error_response(412, "PreconditionFailed"),
            // `RetryParams::for_test()` allows 3 attempts for the read-back.
            error_response(500, "InternalError"),
            error_response(500, "InternalError"),
            error_response(500, "InternalError"),
        ]);
        let s3_storage = make_s3_storage_with_replay(client.clone(), ChecksumAlgorithm::Crc32c);

        let error = s3_storage
            .put_if_absent(Path::new("my-path"), Box::new(b"payload".to_vec()))
            .await
            .unwrap_err();
        // The outcome of the write is unknown; it must not be reported as a plain conflict.
        assert_eq!(error.kind(), StorageErrorKind::Service);
        assert!(format!("{error:?}").contains("reading the object back"));
    }

    #[tokio::test]
    async fn test_conditional_put_without_etag_in_response_is_internal_error() {
        let client = StaticReplayClient::new(vec![ok_put_response()]);
        let s3_storage = make_s3_storage_with_replay(client.clone(), ChecksumAlgorithm::Crc32c);

        let error = s3_storage
            .put_if_absent(Path::new("my-path"), Box::new(b"payload".to_vec()))
            .await
            .unwrap_err();
        assert_eq!(error.kind(), StorageErrorKind::Internal);
        assert!(error.to_string().contains("ETag"));
    }

    #[tokio::test]
    async fn test_get_all_with_version_returns_bytes_and_etag() {
        let client = StaticReplayClient::new(vec![ok_get_response(Some(ETAG), b"content")]);
        let s3_storage = make_s3_storage_with_replay(client.clone(), ChecksumAlgorithm::Crc32c);

        let (bytes, version) = s3_storage
            .get_all_with_version(Path::new("my-path"))
            .await
            .unwrap();
        assert_eq!(bytes.as_slice(), b"content");
        assert_eq!(version, ObjectVersion::new(ETAG));
    }

    #[tokio::test]
    async fn test_get_all_with_version_without_etag_is_internal_error() {
        let client = StaticReplayClient::new(vec![ok_get_response(None, b"content")]);
        let s3_storage = make_s3_storage_with_replay(client.clone(), ChecksumAlgorithm::Crc32c);

        let error = s3_storage
            .get_all_with_version(Path::new("my-path"))
            .await
            .unwrap_err();
        assert_eq!(error.kind(), StorageErrorKind::Internal);
    }

    #[tokio::test]
    async fn test_get_all_with_version_missing_object_is_not_found() {
        let client = StaticReplayClient::new(vec![error_response(404, "NoSuchKey")]);
        let s3_storage = make_s3_storage_with_replay(client.clone(), ChecksumAlgorithm::Crc32c);

        let error = s3_storage
            .get_all_with_version(Path::new("my-path"))
            .await
            .unwrap_err();
        assert_eq!(error.kind(), StorageErrorKind::NotFound);
    }
}

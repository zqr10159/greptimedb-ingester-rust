// Copyright 2023 Greptime Team
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

use crate::api::v1::auth_header::AuthScheme;
use crate::api::v1::greptime_request::Request;
use crate::api::v1::{
    greptime_response, AffectedRows, AuthHeader, DeleteRequests, GreptimeRequest, InsertRequest,
    InsertRequests, RequestHeader, RowInsertRequests,
};
use crate::stream_insert::StreamInserter;

use crate::error::{IllegalDatabaseResponseSnafu, InvalidAsciiSnafu};
use crate::{Client, Result};
use snafu::OptionExt;
use tonic::metadata::MetadataValue;

const DEFAULT_STREAMING_INSERTER_BUFFER_SIZE: usize = 1024;

/// The Client for GreptimeDB Database API.
#[derive(Clone, Debug, Default)]
pub struct Database {
    // The dbname follows naming rule as out mysql, postgres and http
    // protocol. The server treat dbname in priority of catalog/schema.
    dbname: String,

    client: Client,
    auth_header: Option<AuthHeader>,
}

impl Database {
    /// Create database service client using dbname.
    ///
    /// This API is designed for external usage. `dbname` is:
    ///
    /// - the name of database when using GreptimeDB standalone or cluster
    /// - the name provided by GreptimeCloud or other multi-tenant GreptimeDB
    ///   environment
    pub fn new_with_dbname(dbname: impl Into<String>, client: Client) -> Self {
        Self {
            dbname: dbname.into(),
            client,
            auth_header: None,
        }
    }

    /// Get associated dbname of this client
    pub fn dbname(&self) -> &String {
        &self.dbname
    }

    /// Update dbname of this client
    pub fn set_dbname(&mut self, dbname: impl Into<String>) {
        self.dbname = dbname.into();
    }

    /// Set authentication information
    pub fn set_auth(&mut self, auth: AuthScheme) {
        self.auth_header = Some(AuthHeader {
            auth_scheme: Some(auth),
        });
    }

    /// Write insert requests to GreptimeDB and get rows written
    #[deprecated(note = "Use row_insert instead.")]
    pub async fn insert(&self, requests: Vec<InsertRequest>) -> Result<u32> {
        self.handle(Request::Inserts(InsertRequests { inserts: requests }), None)
            .await
    }

    /// Write Row based insert requests to GreptimeDB and get rows written
    pub async fn row_insert(&self, requests: RowInsertRequests) -> Result<u32> {
        self.handle(Request::RowInserts(requests), None).await
    }

    /// Write Row based insert requests with hint to GreptimeDB and get rows written
    pub async fn row_insert_with_hint(
        &self,
        requests: RowInsertRequests,
        hint: &str,
    ) -> Result<u32> {
        self.handle(Request::RowInserts(requests), Some(hint)).await
    }

    /// Initialise a streaming insert handle, using default buffer size `1024`
    pub fn default_streaming_inserter(&self) -> Result<StreamInserter> {
        self.streaming_inserter(DEFAULT_STREAMING_INSERTER_BUFFER_SIZE, None)
    }

    /// Initialise a streaming insert handle, using custom buffer size and hint
    pub fn streaming_inserter(
        &self,
        channel_size: usize,
        hint: Option<&str>,
    ) -> Result<StreamInserter> {
        let client = self.client.make_database_client()?.inner;
        let hint = hint
            .map(|value| {
                MetadataValue::try_from(value).map_err(|_| InvalidAsciiSnafu { value }.build())
            })
            .transpose()?;

        StreamInserter::new(
            client,
            self.dbname().to_string(),
            self.auth_header.clone(),
            channel_size,
            hint,
        )
    }

    /// Issue a delete to database
    pub async fn delete(&self, request: DeleteRequests) -> Result<u32> {
        self.handle(Request::Deletes(request), None).await
    }

    async fn handle(&self, request: Request, hint: Option<&str>) -> Result<u32> {
        let mut client = self.client.make_database_client()?.inner;
        let request = self.to_rpc_request(request);
        let mut request = tonic::Request::new(request);
        if let Some(hint) = hint {
            let hint = MetadataValue::try_from(hint).map_err(|_| {
                InvalidAsciiSnafu {
                    value: hint.to_string(),
                }
                .build()
            })?;
            request.metadata_mut().insert("x-greptime-hints", hint);
        }
        let response = client
            .handle(request)
            .await?
            .into_inner()
            .response
            .context(IllegalDatabaseResponseSnafu {
                err_msg: "GreptimeResponse is empty",
            })?;
        let greptime_response::Response::AffectedRows(AffectedRows { value }) = response;
        Ok(value)
    }

    #[inline]
    fn to_rpc_request(&self, request: Request) -> GreptimeRequest {
        GreptimeRequest {
            header: Some(RequestHeader {
                authorization: self.auth_header.clone(),
                dbname: self.dbname.clone(),
                ..Default::default()
            }),
            request: Some(request),
        }
    }
}

#[cfg(test)]
mod tests {}

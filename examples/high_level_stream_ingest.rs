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

#![allow(clippy::print_stderr)]
#![allow(clippy::print_stdout)]

use derive_new::new;

mod config_utils;
use config_utils::DbConfig;

use greptimedb_ingester::api::v1::*;
use greptimedb_ingester::helpers::schema::{field, tag, timestamp};
use greptimedb_ingester::helpers::values::{
    f32_value, i32_value, string_value, timestamp_millisecond_value,
};
use greptimedb_ingester::{ClientBuilder, Database};

#[tokio::main]
async fn main() {
    let config = DbConfig::from_env();

    let grpc_client = ClientBuilder::default()
        .peers(vec![&config.endpoint])
        .build();

    let client = Database::new_with_dbname(config.database, grpc_client);

    let stream_inserter = client.streaming_inserter(1024, Some("ttl=1d")).unwrap();

    if let Err(e) = stream_inserter
        .row_insert(to_insert_requests(weather_records_1()))
        .await
    {
        eprintln!("Error: {e}");
    }

    if let Err(e) = stream_inserter
        .row_insert(to_insert_requests(weather_records_2()))
        .await
    {
        eprintln!("Error: {e}");
    }

    let result = stream_inserter.finish().await;

    match result {
        Ok(rows) => {
            println!("Rows written: {rows}");
        }
        Err(e) => {
            eprintln!("Error: {e}");
        }
    };
}

#[derive(new)]
struct WeatherRecord {
    timestamp_millis: i64,
    collector: String,
    temperature: f32,
    humidity: i32,
}

fn weather_records_1() -> Vec<WeatherRecord> {
    vec![
        WeatherRecord::new(1686109527000, "c1".to_owned(), 26.4, 15),
        WeatherRecord::new(1686023127000, "c1".to_owned(), 29.3, 20),
        WeatherRecord::new(1685936727000, "c1".to_owned(), 31.8, 13),
        WeatherRecord::new(1686109527000, "c2".to_owned(), 20.4, 67),
        WeatherRecord::new(1686023127000, "c2".to_owned(), 18.0, 74),
        WeatherRecord::new(1685936727000, "c2".to_owned(), 19.2, 81),
    ]
}

fn weather_records_2() -> Vec<WeatherRecord> {
    vec![
        WeatherRecord::new(1686109527001, "c3".to_owned(), 26.4, 15),
        WeatherRecord::new(1686023127002, "c3".to_owned(), 29.3, 20),
        WeatherRecord::new(1685936727003, "c3".to_owned(), 31.8, 13),
        WeatherRecord::new(1686109527004, "c4".to_owned(), 20.4, 67),
        WeatherRecord::new(1686023127005, "c4".to_owned(), 18.0, 74),
        WeatherRecord::new(1685936727006, "c4".to_owned(), 19.2, 81),
    ]
}

fn weather_schema() -> Vec<ColumnSchema> {
    vec![
        timestamp("ts", ColumnDataType::TimestampMillisecond),
        tag("collector", ColumnDataType::String),
        field("temperature", ColumnDataType::Float32),
        field("humidity", ColumnDataType::Int32),
    ]
}

/// This function generates some random data and bundle them into a
/// `InsertRequest`.
///
/// Data structure:
///
/// - `ts`: a timestamp column
/// - `collector`: a tag column
/// - `temperature`: a value field of f32
/// - `humidity`: a value field of i32
///
fn to_insert_requests(records: Vec<WeatherRecord>) -> RowInsertRequests {
    let rows = records
        .into_iter()
        .map(|record| Row {
            values: vec![
                timestamp_millisecond_value(record.timestamp_millis),
                string_value(record.collector),
                f32_value(record.temperature),
                i32_value(record.humidity),
            ],
        })
        .collect();

    RowInsertRequests {
        inserts: vec![RowInsertRequest {
            table_name: "weather_demo".to_owned(),
            rows: Some(Rows {
                schema: weather_schema(),
                rows,
            }),
        }],
    }
}

//
// Copyright (c) 2017, 2020 ADLINK Technology Inc.
//
// This program and the accompanying materials are made available under the
// terms of the Eclipse Public License 2.0 which is available at
// http://www.eclipse.org/legal/epl-2.0, or the Apache License, Version 2.0
// which is available at https://www.apache.org/licenses/LICENSE-2.0.
//
// SPDX-License-Identifier: EPL-2.0 OR Apache-2.0
//
// Contributors:
//   ADLINK zenoh team, <zenoh@adlink-labs.tech>
//

use async_std::task;
use async_trait::async_trait;
use influxdb::{
    Client, Query as InfluxQuery, Timestamp as InfluxTimestamp, WriteQuery as InfluxWQuery,
};
use log::{debug, error, warn};
use regex::Regex;
use serde::Deserialize;
use std::borrow::Cow;
use std::convert::{TryFrom, TryInto};
use std::str::FromStr;
use std::sync::Arc;
use std::time::{Duration, Instant};
use uuid::Uuid;
use zenoh::buf::ZBuf;
use zenoh::prelude::*;
use zenoh::time::new_reception_timestamp;
use zenoh::time::Timestamp;
use zenoh::Result as ZResult;
use zenoh_backend_traits::config::{
    PrivacyGetResult, PrivacyTransparentGet, StorageConfig, VolumeConfig,
};
use zenoh_backend_traits::*;
use zenoh_buffers::SplitBuffer;
use zenoh_collections::{Timed, TimedEvent, TimedHandle, Timer};
use zenoh_core::{bail, zerror};

// Properies used by the Backend
pub const PROP_BACKEND_URL: &str = "url";
pub const PROP_BACKEND_USERNAME: &str = "username";
pub const PROP_BACKEND_PASSWORD: &str = "password";

// Properies used by the Storage
pub const PROP_STORAGE_DB: &str = "db";
pub const PROP_STORAGE_CREATE_DB: &str = "create_db";
pub const PROP_STORAGE_ON_CLOSURE: &str = "on_closure";
pub const PROP_STORAGE_USERNAME: &str = PROP_BACKEND_USERNAME;
pub const PROP_STORAGE_PASSWORD: &str = PROP_BACKEND_PASSWORD;

// delay after deletion to drop a measurement
const DROP_MEASUREMENT_TIMEOUT_MS: u64 = 5000;

const GIT_VERSION: &str = git_version::git_version!(prefix = "v", cargo_prefix = "v");
lazy_static::lazy_static!(
    static ref LONG_VERSION: String = format!("{} built with {}", GIT_VERSION, env!("RUSTC_VERSION"));
);

#[allow(dead_code)]
const CREATE_BACKEND_TYPECHECK: CreateVolume = create_volume;

fn get_private_conf<'a>(
    config: &'a serde_json::Map<String, serde_json::Value>,
    credit: &str,
) -> ZResult<Option<&'a String>> {
    match config.get_private(credit) {
        PrivacyGetResult::NotFound => Ok(None),
        PrivacyGetResult::Private(serde_json::Value::String(v)) => Ok(Some(v)),
        PrivacyGetResult::Public(serde_json::Value::String(v)) => {
            log::warn!(
                r#"Value "{}" is given for `{}` publicly (i.e. is visible by anyone who can fetch the router configuration). You may want to replace `{}: "{}"` with `private: {{{}: "{}"}}`"#,
                v,
                credit,
                credit,
                v,
                credit,
                v
            );
            Ok(Some(v))
        }
        PrivacyGetResult::Both {
            public: serde_json::Value::String(public),
            private: serde_json::Value::String(private),
        } => {
            log::warn!(
                r#"Value "{}" is given for `{}` publicly, but a private value also exists. The private value will be used, but the public value, which is {} the same as the private one, will still be visible in configurations."#,
                public,
                credit,
                if public == private { "" } else { "not " }
            );
            Ok(Some(private))
        }
        _ => {
            bail!("Optional property `{}` must be a string", credit)
        }
    }
}

#[no_mangle]
pub fn create_volume(mut config: VolumeConfig) -> ZResult<Box<dyn Volume>> {
    // For some reasons env_logger is sometime not active in a loaded library.
    // Try to activate it here, ignoring failures.
    let _ = env_logger::try_init();
    debug!("InfluxDB backend {}", LONG_VERSION.as_str());

    config
        .rest
        .insert("version".into(), LONG_VERSION.clone().into());

    let url = match config.rest.get(PROP_BACKEND_URL) {
        Some(serde_json::Value::String(url)) => url.clone(),
        _ => {
            bail!(
                "Mandatory property `{}` for InfluxDb Backend must be a string",
                PROP_BACKEND_URL
            )
        }
    };

    // The InfluxDB client used for administration purposes (show/create/drop databases)
    let mut admin_client = Client::new(&url, "");

    // Note: remove username/password from properties to not re-expose them in admin_status
    let credentials = match (
        get_private_conf(&config.rest, PROP_BACKEND_USERNAME)?,
        get_private_conf(&config.rest, PROP_BACKEND_PASSWORD)?,
    ) {
        (Some(username), Some(password)) => {
            admin_client = admin_client.with_auth(username, password);
            Some((username.clone(), password.clone()))
        }
        (None, None) => None,
        _ => {
            bail!(
                "Optional properties `{}` and `{}` must coexist",
                PROP_BACKEND_USERNAME,
                PROP_BACKEND_PASSWORD
            )
        }
    };

    // Check connectivity to InfluxDB, trying to list databases
    match async_std::task::block_on(async { show_databases(&admin_client).await }) {
        Ok(dbs) => {
            // trick: if "_internal" db is not shown, it means the credentials are not for an admin
            if !dbs.iter().any(|e| e == "_internal") {
                warn!("The InfluxDB credentials are not for an admin user; the volume won't be able to create or drop any database")
            }
        }
        Err(e) => bail!("Failed to create InfluxDb Volume : {}", e),
    }

    Ok(Box::new(InfluxDbBackend {
        admin_status: config,
        admin_client,
        credentials,
    }))
}

pub struct InfluxDbBackend {
    admin_status: VolumeConfig,
    admin_client: Client,
    credentials: Option<(String, String)>,
}

#[async_trait]
impl Volume for InfluxDbBackend {
    fn get_admin_status(&self) -> serde_json::Value {
        self.admin_status.to_json_value()
    }

    async fn create_storage(&mut self, mut config: StorageConfig) -> ZResult<Box<dyn Storage>> {
        let path_expr = config.key_expr.clone();
        let path_prefix = if config.strip_prefix.is_empty() {
            None
        } else if path_expr.starts_with(&config.strip_prefix) {
            Some(config.strip_prefix.clone())
        } else {
            bail!(
                "The specified strip_prefix={} is not a prefix of key_expr={}",
                &config.strip_prefix,
                &path_expr
            )
        };
        let volume_cfg = match config.volume_cfg.as_object() {
            Some(v) => v,
            None => bail!("influxdb backed storages need some volume-specific configuration"),
        };
        let on_closure = match volume_cfg.get(PROP_STORAGE_ON_CLOSURE) {
            Some(serde_json::Value::String(x)) if x == "drop_series" => OnClosure::DropSeries,
            Some(serde_json::Value::String(x)) if x == "drop_db" => OnClosure::DropDb,
            Some(serde_json::Value::String(x)) if x == "do_nothing" => OnClosure::DoNothing,
            None => OnClosure::DoNothing,
            Some(_) => {
                bail!(
                    r#"`{}` property of storage `{}` must be one of "do_nothing" (default), "drop_db" and "drop_series""#,
                    PROP_STORAGE_ON_CLOSURE,
                    &config.name
                )
            }
        };
        let (db, createdb) = match volume_cfg.get(PROP_STORAGE_DB) {
            Some(serde_json::Value::String(s)) => (
                s.clone(),
                match volume_cfg.get(PROP_STORAGE_CREATE_DB) {
                    None | Some(serde_json::Value::Bool(false)) => false,
                    Some(serde_json::Value::Bool(true)) => true,
                    Some(_) => todo!(),
                },
            ),
            None => (generate_db_name(), true),
            _ => bail!(""),
        };

        // The Influx client on database used to write/query on this storage
        // (using the same URL than backend's admin_client, but with storage credentials)
        let mut client = Client::new(self.admin_client.database_url(), &db);

        // Use credentials if specified in storage's volume config
        let storage_username = match (
            get_private_conf(volume_cfg, PROP_STORAGE_USERNAME)?,
            get_private_conf(volume_cfg, PROP_STORAGE_PASSWORD)?,
        ) {
            (Some(username), Some(password)) => {
                client = client.with_auth(username, password);
                Some(username.clone())
            }
            (None, None) => None,
            _ => {
                bail!(
                    "Optional properties `{}` and `{}` must coexist",
                    PROP_STORAGE_USERNAME,
                    PROP_STORAGE_PASSWORD
                )
            }
        };

        // Check if the database exists (using storages credentials)
        if !is_db_existing(&client, &db).await? {
            if createdb {
                // create db using backend's credentials
                create_db(&self.admin_client, &db, storage_username).await?;
            } else {
                bail!("Database '{}' doesn't exist in InfluxDb", db)
            }
        }

        // re-insert the actual name of database (in case it has been generated)
        config
            .volume_cfg
            .as_object_mut()
            .unwrap()
            .entry(PROP_STORAGE_DB)
            .or_insert(db.clone().into());

        // The Influx client on database with backend's credentials (admin), to drop measurements and database
        let mut admin_client = Client::new(self.admin_client.database_url(), db);
        if let Some((username, password)) = &self.credentials {
            admin_client = admin_client.with_auth(username, password);
        }

        Ok(Box::new(InfluxDbStorage {
            admin_status: config,
            admin_client,
            client,
            path_prefix,
            on_closure,
            timer: Timer::default(),
        }))
    }

    fn incoming_data_interceptor(&self) -> Option<Arc<dyn Fn(Sample) -> Sample + Send + Sync>> {
        None
    }

    fn outgoing_data_interceptor(&self) -> Option<Arc<dyn Fn(Sample) -> Sample + Send + Sync>> {
        None
    }
}

enum OnClosure {
    DropDb,
    DropSeries,
    DoNothing,
}

impl TryFrom<&Properties> for OnClosure {
    type Error = zenoh_core::Error;
    fn try_from(p: &Properties) -> ZResult<OnClosure> {
        match p.get(PROP_STORAGE_ON_CLOSURE) {
            Some(s) => {
                if s == "drop_db" {
                    Ok(OnClosure::DropDb)
                } else if s == "drop_series" {
                    Ok(OnClosure::DropSeries)
                } else {
                    bail!("Unsupported value for 'on_closure' property: {}", s)
                }
            }
            None => Ok(OnClosure::DoNothing),
        }
    }
}

struct InfluxDbStorage {
    admin_status: StorageConfig,
    admin_client: Client,
    client: Client,
    path_prefix: Option<String>,
    on_closure: OnClosure,
    timer: Timer,
}

impl InfluxDbStorage {
    async fn get_deletion_timestamp(&self, measurement: &str) -> ZResult<Option<Timestamp>> {
        #[derive(Deserialize, Debug, PartialEq)]
        struct QueryResult {
            timestamp: String,
        }

        let query = <dyn InfluxQuery>::raw_read_query(format!(
            r#"SELECT "timestamp" FROM "{}" WHERE kind='DEL' ORDER BY time DESC LIMIT 1"#,
            measurement
        ));
        match self.client.json_query(query).await {
            Ok(mut result) => match result.deserialize_next::<QueryResult>() {
                Ok(qr) => {
                    if !qr.series.is_empty() && !qr.series[0].values.is_empty() {
                        let ts = qr.series[0].values[0]
                            .timestamp
                            .parse::<Timestamp>()
                            .map_err(|err| {
                                zerror!(
                                "Failed to parse the latest timestamp for deletion of measurement {} : {}",
                                measurement, err.cause)
                            })?;
                        Ok(Some(ts))
                    } else {
                        Ok(None)
                    }
                }
                Err(err) => bail!(
                    "Failed to get latest timestamp for deletion of measurement {} : {}",
                    measurement,
                    err
                ),
            },
            Err(err) => bail!(
                "Failed to get latest timestamp for deletion of measurement {} : {}",
                measurement,
                err
            ),
        }
    }

    async fn schedule_measurement_drop(&self, measurement: &str) -> TimedHandle {
        let event = TimedEvent::once(
            Instant::now() + Duration::from_millis(DROP_MEASUREMENT_TIMEOUT_MS),
            TimedMeasurementDrop {
                client: self.admin_client.clone(),
                measurement: measurement.to_string(),
            },
        );
        let handle = event.get_handle();
        self.timer.add_async(event).await;
        handle
    }
}

#[async_trait]
impl Storage for InfluxDbStorage {
    fn get_admin_status(&self) -> serde_json::Value {
        // TODO: possibly add more properties in returned Value for more information about this storage
        self.admin_status.to_json_value()
    }

    // When receiving a Sample (i.e. on PUT or DELETE operations)
    async fn on_sample(&mut self, sample: Sample) -> ZResult<()> {
        // measurement is the path, stripped of the path_prefix if any
        let mut measurement = sample.key_expr.try_as_str()?;
        if let Some(prefix) = &self.path_prefix {
            measurement = measurement.strip_prefix(prefix).ok_or_else(|| {
                zerror!(
                    "Received a Sample not starting with path_prefix '{}'",
                    prefix
                )
            })?;
        }
        // Note: assume that uhlc timestamp was generated by a clock using UNIX_EPOCH (that's the case by default)
        let sample_ts = sample.timestamp.unwrap_or_else(new_reception_timestamp);
        let influx_time = sample_ts.get_time().to_duration().as_nanos();

        // Store or delete the sample depending the ChangeKind
        match sample.kind {
            SampleKind::Put => {
                // get timestamp of deletion of this measurement, if any
                if let Some(del_time) = self.get_deletion_timestamp(measurement).await? {
                    // ignore sample if oldest than the deletion
                    if sample_ts < del_time {
                        debug!("Received a Sample for {} with timestamp older than its deletion; ignore it", sample.key_expr);
                        return Ok(());
                    }
                }

                // encode the value as a string to be stored in InfluxDB, converting to base64 if the buffer is not a UTF-8 string
                let (base64, strvalue) =
                    match String::from_utf8(sample.value.payload.contiguous().into_owned()) {
                        Ok(s) => (false, s),
                        Err(err) => (true, base64::encode(err.into_bytes())),
                    };

                // Note: tags are stored as strings in InfluxDB, while fileds are typed.
                // For simpler/faster deserialization, we store encoding, timestamp and base64 as fields.
                // while the kind is stored as a tag to be indexed by InfluxDB and have faster queries on it.
                let query =
                    InfluxWQuery::new(InfluxTimestamp::Nanoseconds(influx_time), measurement)
                        .add_tag("kind", "PUT")
                        .add_field("timestamp", sample_ts.to_string())
                        .add_field("encoding_prefix", u8::from(*sample.value.encoding.prefix()))
                        .add_field("encoding_suffix", sample.value.encoding.suffix())
                        .add_field("base64", base64)
                        .add_field("value", strvalue);
                debug!("Put {} with Influx query: {:?}", sample.key_expr, query);
                if let Err(e) = self.client.query(&query).await {
                    bail!(
                        "Failed to put Value for {} in InfluxDb storage : {}",
                        sample.key_expr,
                        e
                    )
                }
            }
            SampleKind::Delete => {
                // delete all points from the measurement that are older than this DELETE message
                // (in case more recent PUT have been recevived un-ordered)
                let query = <dyn InfluxQuery>::raw_read_query(format!(
                    r#"DELETE FROM "{}" WHERE time < {}"#,
                    measurement, influx_time
                ));
                debug!("Delete {} with Influx query: {:?}", sample.key_expr, query);
                if let Err(e) = self.client.query(&query).await {
                    bail!(
                        "Failed to delete points for measurement '{}' from InfluxDb storage : {}",
                        measurement,
                        e
                    )
                }
                // store a point (with timestamp) with "delete" tag, thus we don't re-introduce an older point later
                let query =
                    InfluxWQuery::new(InfluxTimestamp::Nanoseconds(influx_time), measurement)
                        .add_field("timestamp", sample_ts.to_string())
                        .add_tag("kind", "DEL");
                debug!(
                    "Mark measurement {} as deleted at time {}",
                    measurement, influx_time
                );
                if let Err(e) = self.client.query(&query).await {
                    bail!(
                        "Failed to mark measurement {} as deleted : {}",
                        sample.key_expr,
                        e
                    )
                }
                // schedule the drop of measurement later in the future, if it's empty
                let _ = self.schedule_measurement_drop(measurement).await;
            }
            SampleKind::Patch => {
                println!("Received PATCH for {}: not yet supported", sample.key_expr);
            }
        }
        Ok(())
    }

    // When receiving a Query (i.e. on GET operations)
    async fn on_query(&mut self, query: Query) -> ZResult<()> {
        // get the query's Selector
        let selector = query.selector();
        let selector_str = selector.key_selector.try_as_str()?;
        // if a path_prefix is used
        let regex = if let Some(prefix) = &self.path_prefix {
            // get the list of sub-path expressions that will match the same stored keys than
            // the selector, if those keys had the path_prefix.
            let path_exprs = utils::get_sub_key_selectors(selector_str, prefix);
            debug!(
                "Query on {} with path_prefix={} => sub_key_selectors = {:?}",
                selector, prefix, path_exprs
            );
            // convert the sub-path expressions into an Influx regex
            path_exprs_to_influx_regex(&path_exprs)
        } else {
            // convert the Selector's path expression into an Influx regex
            path_exprs_to_influx_regex(&[selector_str])
        };

        // construct the Influx query clauses from the Selector
        let clauses = clauses_from_selector(&selector)?;

        // the Influx query
        let influx_query_str = format!("SELECT * FROM {} {}", regex, clauses);
        let influx_query = <dyn InfluxQuery>::raw_read_query(&influx_query_str);

        // the expected JSon type resulting from the query
        #[derive(Deserialize, Debug)]
        struct ZenohPoint {
            #[allow(dead_code)]
            // NOTE: "kind" is present within InfluxDB and used in query clauses, but not read in Rust...
            kind: String,
            timestamp: String,
            encoding_prefix: ZInt,
            encoding_suffix: String,
            base64: bool,
            value: String,
        }
        debug!("Get {} with Influx query: {}", selector, influx_query_str);
        match self.client.json_query(influx_query).await {
            Ok(mut query_result) => {
                while !query_result.results.is_empty() {
                    match query_result.deserialize_next::<ZenohPoint>() {
                        Ok(retn) => {
                            for serie in retn.series {
                                // reconstruct the path from the measurement name (same as serie.name)
                                let mut res_name = String::with_capacity(serie.name.len());
                                if let Some(p) = &self.path_prefix {
                                    res_name.push_str(p);
                                }
                                res_name.push_str(&serie.name);
                                debug!("Replying {} values for {}", serie.values.len(), res_name);
                                for zpoint in serie.values {
                                    let encoding_prefix =
                                        zpoint.encoding_prefix.try_into().map_err(|_| {
                                            zerror!("Unknown encoding {}", zpoint.encoding_prefix)
                                        })?;
                                    let encoding = if zpoint.encoding_suffix.is_empty() {
                                        Encoding::Exact(encoding_prefix)
                                    } else {
                                        Encoding::WithSuffix(
                                            encoding_prefix,
                                            zpoint.encoding_suffix.into(),
                                        )
                                    };
                                    let payload = if zpoint.base64 {
                                        match base64::decode(zpoint.value) {
                                            Ok(v) => ZBuf::from(v),
                                            Err(e) => {
                                                warn!(
                                                    r#"Failed to decode zenoh base64 Value from Influx point {} with timestamp="{}": {}"#,
                                                    serie.name, zpoint.timestamp, e
                                                );
                                                continue;
                                            }
                                        }
                                    } else {
                                        ZBuf::from(zpoint.value.into_bytes())
                                    };
                                    let timestamp = match Timestamp::from_str(&zpoint.timestamp) {
                                        Ok(t) => t,
                                        Err(e) => {
                                            warn!(
                                                r#"Failed to decode zenoh Timestamp from Influx point {} with timestamp="{}": {:?}"#,
                                                serie.name, zpoint.timestamp, e
                                            );
                                            continue;
                                        }
                                    };
                                    let value = Value { payload, encoding };
                                    query
                                        .reply(
                                            Sample::new(res_name.clone(), value)
                                                .with_timestamp(timestamp),
                                        )
                                        .await;
                                }
                            }
                        }
                        Err(e) => {
                            bail!(
                                "Failed to parse result of InfluxDB query '{}': {}",
                                influx_query_str,
                                e
                            )
                        }
                    }
                }
                Ok(())
            }
            Err(e) => bail!(
                "Failed to query InfluxDb with '{}' : {}",
                influx_query_str,
                e
            ),
        }
    }
}

impl Drop for InfluxDbStorage {
    fn drop(&mut self) {
        debug!("Closing InfluxDB storage");
        match self.on_closure {
            OnClosure::DropDb => {
                let _ = task::block_on(async move {
                    let db = self.admin_client.database_name();
                    debug!("Close InfluxDB storage, dropping database {}", db);
                    let query =
                        <dyn InfluxQuery>::raw_read_query(format!(r#"DROP DATABASE "{}""#, db));
                    if let Err(e) = self.admin_client.query(&query).await {
                        error!("Failed to drop InfluxDb database '{}' : {}", db, e)
                    }
                });
            }
            OnClosure::DropSeries => {
                let _ = task::block_on(async move {
                    let db = self.client.database_name();
                    debug!(
                        "Close InfluxDB storage, dropping all series from database {}",
                        db
                    );
                    let query = <dyn InfluxQuery>::raw_read_query("DROP SERIES FROM /.*/");
                    if let Err(e) = self.client.query(&query).await {
                        error!(
                            "Failed to drop all series from InfluxDb database '{}' : {}",
                            db, e
                        )
                    }
                });
            }
            OnClosure::DoNothing => {
                debug!(
                    "Close InfluxDB storage, keeping database {} as it is",
                    self.client.database_name()
                );
            }
        }
    }
}

// Scheduled dropping of a measurement after a timeout, if it's empty
struct TimedMeasurementDrop {
    client: Client,
    measurement: String,
}

#[async_trait]
impl Timed for TimedMeasurementDrop {
    async fn run(&mut self) {
        #[derive(Deserialize, Debug, PartialEq)]
        struct QueryResult {
            kind: String,
        }

        // check if there is at least 1 point without "DEL" kind in the measurement
        let query = <dyn InfluxQuery>::raw_read_query(format!(
            r#"SELECT "kind" FROM "{}" WHERE kind!='DEL' LIMIT 1"#,
            self.measurement
        ));
        match self.client.json_query(query).await {
            Ok(mut result) => match result.deserialize_next::<QueryResult>() {
                Ok(qr) => {
                    if !qr.series.is_empty() {
                        debug!("Measurement {} contains new values inserted after deletion; don't drop it", self.measurement);
                        return;
                    }
                }
                Err(e) => {
                    warn!(
                        "Failed to check if measurement '{}' is empty (can't drop it) : {}",
                        self.measurement, e
                    );
                }
            },
            Err(e) => {
                warn!(
                    "Failed to check if measurement '{}' is empty (can't drop it) : {}",
                    self.measurement, e
                );
                return;
            }
        }

        // drop the measurement
        let query = <dyn InfluxQuery>::raw_read_query(format!(
            r#"DROP MEASUREMENT "{}""#,
            self.measurement
        ));
        debug!(
            "Drop measurement {} after timeout with Influx query: {:?}",
            self.measurement, query
        );
        if let Err(e) = self.client.query(&query).await {
            warn!(
                "Failed to drop measurement '{}' from InfluxDb storage : {}",
                self.measurement, e
            );
        }
    }
}

fn generate_db_name() -> String {
    format!("zenoh_db_{}", Uuid::new_v4().to_simple())
}

async fn show_databases(client: &Client) -> ZResult<Vec<String>> {
    #[derive(Deserialize)]
    struct Database {
        name: String,
    }
    let query = <dyn InfluxQuery>::raw_read_query("SHOW DATABASES");
    debug!("List databases with Influx query: {:?}", query);
    match client.json_query(query).await {
        Ok(mut result) => match result.deserialize_next::<Database>() {
            Ok(dbs) => {
                let mut result: Vec<String> = Vec::new();
                for serie in dbs.series {
                    for db in serie.values {
                        result.push(db.name);
                    }
                }
                Ok(result)
            }
            Err(e) => bail!(
                "Failed to parse list of existing InfluxDb databases : {}",
                e
            ),
        },
        Err(e) => bail!("Failed to list existing InfluxDb databases : {}", e),
    }
}

async fn is_db_existing(client: &Client, db_name: &str) -> ZResult<bool> {
    let dbs = show_databases(client).await?;
    Ok(dbs.iter().any(|e| e == db_name))
}

async fn create_db(
    client: &Client,
    db_name: &str,
    storage_username: Option<String>,
) -> ZResult<()> {
    let query = <dyn InfluxQuery>::raw_read_query(format!(r#"CREATE DATABASE "{}""#, db_name));
    debug!("Create Influx database: {}", db_name);
    if let Err(e) = client.query(&query).await {
        bail!(
            "Failed to create new InfluxDb database '{}' : {}",
            db_name,
            e
        )
    }

    // is a username is specified for storage access, grant him access to the database
    if let Some(username) = storage_username {
        let query = <dyn InfluxQuery>::raw_read_query(format!(
            r#"GRANT ALL ON "{}" TO "{}""#,
            db_name, username
        ));
        debug!(
            "Grant access to {} on Influx database: {}",
            username, db_name
        );
        if let Err(e) = client.query(&query).await {
            bail!(
                "Failed grant access to {} on Influx database '{}' : {}",
                username,
                db_name,
                e
            )
        }
    }
    Ok(())
}

// Returns an InfluxDB regex (see https://docs.influxdata.com/influxdb/v1.8/query_language/explore-data/#regular-expressions)
// corresponding to the list of path expressions. I.e.:
// Replace "**" with ".*", "*" with "[^\/]*"  and "/" with "\/".
// Concat each with "|", and surround the result with '/^' and '$/'.
fn path_exprs_to_influx_regex(path_exprs: &[&str]) -> String {
    let mut result = String::with_capacity(2 * path_exprs[0].len());
    result.push_str("/^");
    for (i, path_expr) in path_exprs.iter().enumerate() {
        if i != 0 {
            result.push('|');
        }
        let mut chars = path_expr.chars().peekable();
        while let Some(c) = chars.next() {
            match c {
                '*' => {
                    if let Some(c2) = chars.peek() {
                        if c2 == &'*' {
                            result.push_str(".*");
                            chars.next();
                        } else {
                            result.push_str(".*")
                        }
                    }
                }
                '/' => result.push_str(r#"\/"#),
                _ => result.push(c),
            }
        }
    }
    result.push_str("$/");
    result
}

fn clauses_from_selector(s: &Selector) -> ZResult<String> {
    let value_selector = s.parse_value_selector()?;
    let mut result = String::with_capacity(256);
    result.push_str("WHERE kind!='DEL'");
    match (
        value_selector.properties.get("starttime"),
        value_selector.properties.get("stoptime"),
    ) {
        (Some(start), Some(stop)) => {
            result.push_str(" AND time >= ");
            result.push_str(&normalize_rfc3339(start));
            result.push_str(" AND time <= ");
            result.push_str(&normalize_rfc3339(stop));
        }
        (Some(start), None) => {
            result.push_str(" AND time >= ");
            result.push_str(&normalize_rfc3339(start));
        }
        (None, Some(stop)) => {
            result.push_str(" AND time <= ");
            result.push_str(&normalize_rfc3339(stop));
        }
        _ => {
            //No time selection, return only latest values
            result.push_str(" ORDER BY time DESC LIMIT 1");
        }
    }
    Ok(result)
}

// Surrounds with `''` all parts of `time` matching a RFC3339 time representation
// to comply with InfluxDB time clauses.
fn normalize_rfc3339(time: &str) -> Cow<str> {
    lazy_static::lazy_static! {
        static ref RE: Regex = Regex::new(
            "(?:'?(?P<rfc3339>[0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9][ T]?[0-9:.]*Z?)'?)"
        )
        .unwrap();
    }

    RE.replace_all(time, "'$rfc3339'")
}

#[test]
fn test_normalize_rfc3339() {
    // test no surrounding with '' if not rfc3339 time
    assert_eq!("now()", normalize_rfc3339("now()"));
    assert_eq!("now()-1h", normalize_rfc3339("now()-1h"));

    // test surrounding with ''
    assert_eq!(
        "'2020-11-05T16:31:42.226942997Z'",
        normalize_rfc3339("2020-11-05T16:31:42.226942997Z")
    );
    assert_eq!(
        "'2020-11-05T16:31:42Z'",
        normalize_rfc3339("2020-11-05T16:31:42Z")
    );
    assert_eq!(
        "'2020-11-05 16:31:42.226942997'",
        normalize_rfc3339("2020-11-05 16:31:42.226942997")
    );
    assert_eq!("'2020-11-05'", normalize_rfc3339("2020-11-05"));

    // test no surrounding with '' if already done
    assert_eq!(
        "'2020-11-05T16:31:42.226942997Z'",
        normalize_rfc3339("'2020-11-05T16:31:42.226942997Z'")
    );

    // test surrounding with '' only the rfc3339 time
    assert_eq!(
        "'2020-11-05T16:31:42.226942997Z'-1h",
        normalize_rfc3339("2020-11-05T16:31:42.226942997Z-1h")
    );
    assert_eq!(
        "'2020-11-05T16:31:42Z'-1h",
        normalize_rfc3339("2020-11-05T16:31:42Z-1h")
    );
    assert_eq!(
        "'2020-11-05 16:31:42.226942997'-1h",
        normalize_rfc3339("2020-11-05 16:31:42.226942997-1h")
    );
    assert_eq!("'2020-11-05'-1h", normalize_rfc3339("2020-11-05-1h"));
}

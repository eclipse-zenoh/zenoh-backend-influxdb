//
// Copyright (c) 2022 ZettaScale Technology
//
// This program and the accompanying materials are made available under the
// terms of the Eclipse Public License 2.0 which is available at
// http://www.eclipse.org/legal/epl-2.0, or the Apache License, Version 2.0
// which is available at https://www.apache.org/licenses/LICENSE-2.0.
//
// SPDX-License-Identifier: EPL-2.0 OR Apache-2.0
//
// Contributors:
//   ZettaScale Zenoh Team, <zenoh@zettascale.tech>
//

use async_std::task;
use async_trait::async_trait;
use base64::{engine::general_purpose::STANDARD as b64_std_engine, Engine};
use chrono::{NaiveDateTime, SecondsFormat};
use futures::prelude::*;
use influxdb2::api::buckets::ListBucketsRequest;
use influxdb2::models::Query;
use influxdb2::models::{DataPoint, PostBucketRequest};
use influxdb2::Client;
use influxdb2::FromDataPoint;
use std::convert::{TryFrom, TryInto};
use std::str::FromStr;
use std::time::{Duration, Instant, UNIX_EPOCH};
use uuid::Uuid;
use zenoh::encoding::Encoding;
use zenoh::internal::{
    bail,
    buffers::{SplitBuffer, ZBuf},
    zerror, Timed, TimedEvent, TimedHandle, Timer, Value,
};
use zenoh::key_expr::{keyexpr, OwnedKeyExpr};
use zenoh::selector::{Parameters, TimeExpr};
use zenoh::time::{new_timestamp, Timestamp};
use zenoh::{try_init_log_from_env, Error, Result as ZResult};
use zenoh_backend_traits::config::{
    PrivacyGetResult, PrivacyTransparentGet, StorageConfig, VolumeConfig,
};
use zenoh_backend_traits::StorageInsertionResult;
use zenoh_backend_traits::*;
use zenoh_plugin_trait::{plugin_long_version, plugin_version, Plugin};

// Properties used by the Backend
pub const PROP_BACKEND_URL: &str = "url";
pub const PROP_BACKEND_ORG_ID: &str = "org_id";
pub const PROP_TOKEN: &str = "token";

// Properties used by the Storage
pub const PROP_STORAGE_DB: &str = "db";
pub const PROP_STORAGE_CREATE_DB: &str = "create_db";
pub const PROP_STORAGE_ON_CLOSURE: &str = "on_closure";

// Special key for None (when the prefix being stripped exactly matches the key)
pub const NONE_KEY: &str = "@@none_key@@";

// delay after deletion to drop a measurement
const DROP_MEASUREMENT_TIMEOUT_MS: u64 = 5000;

lazy_static::lazy_static!(
    static ref INFLUX_REGEX_ALL: String = key_exprs_to_influx_regex(&["**".try_into().unwrap()]);
);

type Config<'a> = &'a serde_json::Map<String, serde_json::Value>;

fn get_private_conf<'a>(config: Config<'a>, credit: &str) -> ZResult<Option<&'a String>> {
    match config.get_private(credit) {
        PrivacyGetResult::NotFound => Ok(None),
        PrivacyGetResult::Private(serde_json::Value::String(v)) => Ok(Some(v)),
        PrivacyGetResult::Public(serde_json::Value::String(v)) => {
            tracing::warn!(
                r#"Value "{}" is given for `{}` publicly (i.e. is visible by anyone who can fetch the router configuration). 
                You may want to replace `{}: "{}"` with `private: {{{}: "{}"}}`"#,
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
            tracing::warn!(
                r#"Value "{}" is given for `{}` publicly, but a private value also exists. 
                The private value will be used, 
                but the public value, which is {} the same as the private one, will still be visible in configurations."#,
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

fn extract_credentials(config: Config) -> ZResult<Option<InfluxDbCredentials>> {
    match (
        get_private_conf(config, PROP_BACKEND_ORG_ID)?,
        get_private_conf(config, PROP_TOKEN)?,
    ) {
        // (Some(org_id), Some(token)) => Ok(Some(InfluxDbCredentials::Creds(
        //     org_id.clone(),
        //     token.clone(),
        // ))),
        (Some(org_id), Some(token)) => Ok(Some(InfluxDbCredentials {
            org_id: org_id.clone(),
            token: token.clone(),
        })),
        _ => {
            tracing::error!("Couldn't get token and org");
            bail!(
                "Properties `{}` and `{}` must exist",
                PROP_BACKEND_ORG_ID,
                PROP_TOKEN
            );
        }
    }
}

pub struct InfluxDbBackend {}

#[cfg(feature = "dynamic_plugin")]
zenoh_plugin_trait::declare_plugin!(InfluxDbBackend);

impl Plugin for InfluxDbBackend {
    type StartArgs = VolumeConfig;
    type Instance = VolumeInstance;

    const DEFAULT_NAME: &'static str = "influxdb_backend2";
    const PLUGIN_VERSION: &'static str = plugin_version!();
    const PLUGIN_LONG_VERSION: &'static str = plugin_long_version!();

    fn start(_name: &str, config: &Self::StartArgs) -> ZResult<Self::Instance> {
        try_init_log_from_env();

        tracing::debug!("InfluxDBv2 backend {}", Self::PLUGIN_VERSION);

        let mut config = config.clone();
        config
            .rest
            .insert("version".into(), Self::PLUGIN_VERSION.into());

        let url = match config.rest.get(PROP_BACKEND_URL) {
            Some(serde_json::Value::String(url)) => url.clone(),
            _ => {
                bail!(
                    "Mandatory property `{}` for InfluxDbv2 Backend must be a string",
                    PROP_BACKEND_URL
                )
            }
        };

        // The InfluxDB "admin" client that will be used for creating and dropping buckets (create/drop databases)
        #[allow(unused_mut)]
        let mut admin_client: Client;

        match extract_credentials(&config.rest)? {
            Some(creds) => {
                admin_client = match std::panic::catch_unwind(|| {
                    Client::new(url.clone(), creds.org_id.clone(), creds.token.clone())
                }) {
                    Ok(client) => client,
                    Err(e) => bail!("Error in creating client for InfluxDBv2 volume: {:?}", e),
                };
                match async_std::task::block_on(async { admin_client.ready().await }) {
                    Ok(res) => {
                        if !res {
                            bail!("InfluxDBv2 server is not ready! ")
                        }
                    }
                    Err(e) => bail!("Failed to create InfluxDBv2 Volume : {:?}", e),
                }

                Ok(Box::new(InfluxDbVolume {
                    admin_status: config,
                    admin_client,
                    credentials: Some(creds),
                }))
            }
            None => {
                bail!("Admin creds not provided. Can't proceed without them.");
            }
        }
    }
}

// Struct for credentials
#[derive(Debug)]
struct InfluxDbCredentials {
    org_id: String,
    token: String,
}

pub struct InfluxDbVolume {
    admin_status: VolumeConfig,
    admin_client: Client,
    credentials: Option<InfluxDbCredentials>,
}

#[async_trait]
impl Volume for InfluxDbVolume {
    fn get_admin_status(&self) -> serde_json::Value {
        self.admin_status.to_json_value()
    }

    fn get_capability(&self) -> Capability {
        Capability {
            persistence: Persistence::Durable,
            history: History::All,
            read_cost: 1,
        }
    }

    async fn create_storage(&self, mut config: StorageConfig) -> ZResult<Box<dyn Storage>> {
        let volume_cfg = match config.volume_cfg.as_object() {
            Some(v) => v,
            None => bail!("InfluxDBv2 backed storages need some volume-specific configuration"),
        };

        let on_closure = match volume_cfg.get(PROP_STORAGE_ON_CLOSURE) {
            Some(serde_json::Value::String(x)) if x == "drop_series" => OnClosure::DropSeries,
            Some(serde_json::Value::String(x)) if x == "drop_db" => OnClosure::DropDb,
            Some(serde_json::Value::String(x)) if x == "do_nothing" => OnClosure::DoNothing,
            None => OnClosure::DoNothing,
            Some(_) => {
                bail!(
                    r#"`{}` property of InfluxDBv2 storage `{}` must be one of "do_nothing" (default), "drop_db" and "drop_series""#,
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
            Some(v) => bail!("Invalid value for ${PROP_STORAGE_DB} config property: ${v}"),
        };

        // The Influx client on database used to write/query on this storage
        let url = match &self.admin_status.rest.get(PROP_BACKEND_URL) {
            Some(serde_json::Value::String(url)) => url.clone(),
            _ => {
                bail!(
                    "Mandatory property `{}` for InfluxDBv2 Backend must be a string",
                    PROP_BACKEND_URL
                )
            }
        };

        let creds = match extract_credentials(volume_cfg)? {
            Some(creds) => creds,
            _ => bail!("No credentials specified to access database '{}'", db),
        };
        let client = match std::panic::catch_unwind(|| {
            Client::new(url.clone(), creds.org_id.clone(), creds.token.clone())
        }) {
            Ok(client) => client,
            Err(e) => bail!("Error in creating client for InfluxDBv2 storage: {:?}", e),
        };

        //check if db exists, if it doesn't create one if user has set createdb=true in config
        match async_std::task::block_on(async { is_db_existing(&client, &db).await }) {
            Ok(res) => {
                if !res && createdb {
                    // try to create db using user credentials
                    match async_std::task::block_on(async {
                        create_db(&self.admin_client, &creds.org_id, &db).await
                    }) {
                        Ok(res) => {
                            if !res {
                                bail!("Database '{}' wasnt't created in InfluxDBv2 storage", db)
                            }
                        }
                        Err(e) => bail!("Failed to create InfluxDBv2 Storage : {:?}", e),
                    }
                }
            }
            Err(e) => bail!("Failed to create InfluxDBv2 Storage : {:?}", e),
        }

        config
            .volume_cfg
            .as_object_mut()
            .unwrap()
            .entry(PROP_STORAGE_DB)
            .or_insert(db.clone().into());

        // The Influx client on database with backend's credentials (admin), to drop measurements and database
        let admin_creds = match &self.credentials {
            Some(creds) => creds,
            None => bail!("No credentials specified to access database '{}'", db),
        };
        let admin_client = match std::panic::catch_unwind(|| {
            Client::new(url.clone(), &admin_creds.org_id, &admin_creds.token)
        }) {
            Ok(client) => client,
            Err(e) => bail!("Error in creating client for InfluxDBv2 volume: {:?}", e),
        };

        Ok(Box::new(InfluxDbStorage {
            config,
            admin_client,
            client,
            on_closure,
            timer: Timer::default(),
        }))
    }
}

enum OnClosure {
    DropDb,
    DropSeries,
    DoNothing,
}

impl TryFrom<&Parameters<'_>> for OnClosure {
    type Error = Error;
    fn try_from(p: &Parameters) -> ZResult<OnClosure> {
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
    config: StorageConfig,
    admin_client: Client,
    client: Client,
    on_closure: OnClosure,
    timer: Timer,
}

impl InfluxDbStorage {
    async fn get_deletion_timestamp(&self, measurement: &str) -> ZResult<Option<Timestamp>> {
        let db = get_db_name(self.config.clone())?;
        let qs = format!(
            "from(bucket: \"{}\")
                                    |> range(start: {})
                                    |> filter(fn: (r) => r._measurement == \"{}\")
                                    |> filter(fn: (r) => r[\"kind\"] == \"DEL\")
                                    |> last()
                                ",
            db, 0, measurement
        );

        // get the value and if it exists then extract the timestamp from it

        #[derive(Debug, Default, FromDataPoint)]
        struct ZenohPoint {
            #[allow(dead_code)]
            // NOTE: "kind" is present within InfluxDB and used in query clauses, but not read in Rust...
            kind: String,
            timestamp: String,
            encoding_prefix: i64, //should be u8 but not supported in v2.x so using a workaround
            encoding_suffix: String,
            base64: bool,
            value: String,
        }

        let query = Query::new(qs);
        let mut query_result: Vec<ZenohPoint> = vec![];

        match async_std::task::block_on(async {
            self.client.query::<ZenohPoint>(Some(query)).await
        }) {
            Ok(result) => {
                query_result = result;
            }
            Err(e) => {
                tracing::error!(
                    "Couldn't get data from InfluxDBv2 database {} with error: {} ",
                    db,
                    e
                );
            }
        };

        if query_result.is_empty() {
            return Ok(None);
        }

        match Timestamp::from_str(&query_result[0].timestamp) {
            Ok(ts) => Ok(Some(ts)),
            Err(e) => {
                bail!("Couldn't parse the deletion timestamp {:?}", e)
            }
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

    // fn keyexpr_from_serie(&self, serie_name: &str) -> ZResult<Option<OwnedKeyExpr>> {
    //     if serie_name.eq(NONE_KEY) {
    //         Ok(None)
    //     } else {
    //         match OwnedKeyExpr::from_str(serie_name) {
    //             Ok(key) => Ok(Some(key)),
    //             Err(e) => Err(format!("{}", e).into()),
    //         }
    //     }
    // }
}

#[async_trait]
impl Storage for InfluxDbStorage {
    fn get_admin_status(&self) -> serde_json::Value {
        // TODO: possibly add more properties in returned Value for more information about this storage
        self.config.to_json_value()
    }

    async fn put(
        &mut self,
        key: Option<OwnedKeyExpr>,
        value: Value,
        timestamp: Timestamp,
    ) -> ZResult<StorageInsertionResult> {
        let measurement = key.unwrap_or_else(|| OwnedKeyExpr::from_str(NONE_KEY).unwrap());
        let influx_time = timestamp.get_time().to_duration().as_nanos() as i64;

        if let Some(del_time) = self.get_deletion_timestamp(measurement.as_str()).await? {
            // ignore sample if oldest than the deletion
            if timestamp < del_time {
                tracing::debug!(
                    "Received a value for {:?} with timestamp older than its deletion in InfluxDBv2 database; Ignore it",
                    measurement
                );
                return Ok(StorageInsertionResult::Outdated);
            }
        }

        // Note: assume that uhlc timestamp was generated by a clock using UNIX_EPOCH (that's the case by default)
        // encode the value as a string to be stored in InfluxDB, converting to base64 if the buffer is not a UTF-8 string
        let (base64, strvalue) =
            match String::from_utf8(ZBuf::from(value.payload()).contiguous().into_owned()) {
                Ok(s) => (false, s),
                Err(err) => (true, b64_std_engine.encode(err.into_bytes())),
            };

        let db = get_db_name(self.config.clone())?;

        // Note: tags are stored as strings in InfluxDB, while fileds are typed.
        // For simpler/faster deserialization, we store encoding, timestamp and base64 as fields.
        // while the kind is stored as a tag to be indexed by InfluxDB and have faster queries on it.
        let encoding_string_rep = value.encoding().clone().to_string(); // field only supports Strings and not Vec<u8>
        let encoding: Encoding = (value.encoding().clone()).into();

        let zenoh_point = vec![DataPoint::builder(measurement.clone())
            .tag("kind", "PUT")
            .field("timestamp", timestamp.to_string())
            .field("encoding_prefix", u16::from(encoding.id()) as i64) //should be u8 but not supported in v2.x so using a workaround
            .field("encoding_suffix", encoding_string_rep) // TODO: Left for compatibility, consider replacing wiht single "Encoding" field
            .field("base64", base64)
            .field("value", strvalue)
            .timestamp(influx_time) //converted timestamp to i64
            .build()?];

        match async_std::task::block_on(async {
            self.client.write(&db, stream::iter(zenoh_point)).await
        }) {
            Ok(_) => Ok(StorageInsertionResult::Inserted),
            Err(e) => bail!(
                "Failed to put Value for {:?} in InfluxDBv2 storage : {}",
                measurement,
                e
            ),
        }
    }

    async fn delete(
        &mut self,
        key: Option<OwnedKeyExpr>,
        timestamp: Timestamp,
    ) -> ZResult<StorageInsertionResult> {
        let measurement = key.unwrap_or_else(|| OwnedKeyExpr::from_str(NONE_KEY).unwrap());
        // Note: assume that uhlc timestamp was generated by a clock using UNIX_EPOCH (that's the case by default)
        // delete all points from the measurement that are older than this DELETE message
        // (in case more recent PUT have been recevived un-ordered)

        let db = get_db_name(self.config.clone())?;

        let start_timestamp = NaiveDateTime::UNIX_EPOCH;
        let stop_timestamp = chrono::DateTime::from_timestamp(
            timestamp.get_time().as_secs() as i64,
            timestamp.get_time().subsec_nanos(),
        )
        .ok_or_else(|| zerror!("delete: stop_timestamp out of range"))?
        .naive_utc();

        let predicate = None; //can be specified with tag or field values
        tracing::debug!(
            "Delete {:?} with Influx query in InfluxDBv2 storage",
            measurement
        );
        if let Err(e) = self
            .client
            .delete(&db, start_timestamp, stop_timestamp, predicate)
            .await
        {
            bail!(
                "Failed to delete points for measurement '{}' from InfluxDBv2 storage : {}",
                measurement,
                e
            )
        }
        // store a point (with timestamp) with "delete" tag, thus we don't re-introduce an older point later;
        // filling fields with dummy values
        let influx_time = timestamp.get_time().to_duration().as_nanos() as i64;

        let zenoh_point = vec![DataPoint::builder(measurement.clone())
            .tag("kind", "DEL")
            .field("timestamp", timestamp.to_string())
            .field("encoding_prefix", 0_i64) //should be u8 but not supported in v2.x so using a workaround
            .field("encoding_suffix", "")
            .field("base64", false)
            .field("value", "")
            .timestamp(influx_time) //converted timestamp to i64
            .build()?];

        tracing::debug!(
            "Mark measurement {} as deleted at time {} in InfluxDBv2 storage",
            measurement.clone(),
            stop_timestamp
        );

        if let Err(e) = self.client.write(&db, stream::iter(zenoh_point)).await {
            bail!(
                "Failed to mark measurement {:?} as deleted : {} in InfluxDBv2 storage",
                measurement,
                e
            )
        }
        // schedule_measurement_drop is used to schedule the drop of measurement later in the future, if it's empty
        // influx 2.x doesn't support dropping measurements from the API
        let _ = self.schedule_measurement_drop(measurement.as_str()).await;
        Ok(StorageInsertionResult::Deleted)
    }

    async fn get(
        &mut self,
        key: Option<OwnedKeyExpr>,
        parameters: &str,
    ) -> ZResult<Vec<StoredData>> {
        let measurement = match key.clone() {
            Some(k) => k,
            None => OwnedKeyExpr::from_str(NONE_KEY)?,
        };

        let db = get_db_name(self.config.clone())?;

        // the expected JSon type resulting from the query
        // #[derive(Deserialize, Debug)]
        #[derive(Debug, Default, FromDataPoint)]
        struct ZenohPoint {
            #[allow(dead_code)]
            // NOTE: "kind" is present within InfluxDB and used in query clauses, but not read in Rust...
            kind: String,
            timestamp: String,
            encoding_prefix: i64, //should be u8 but not supported in v2.x so using a workaround
            encoding_suffix: String,
            base64: bool,
            value: String,
        }

        #[allow(unused_assignments)]
        let mut qs: String = String::new();
        match timerange_from_parameters(parameters)? {
            Some(range) => {
                qs = format!(
                    "from(bucket: \"{}\")
                                            |> range({})
                                            |> filter(fn: (r) => r._measurement == \"{}\")
                                            |> filter(fn: (r) => r[\"kind\"] == \"PUT\")
                                        ",
                    db, range, measurement
                );
            }
            None => {
                qs = format!(
                    "from(bucket: \"{}\")
                                            |> range(start:0)
                                            |> filter(fn: (r) => r._measurement == \"{}\")
                                            |> filter(fn: (r) => r[\"kind\"] == \"PUT\")
                                            |> last()
                                        ",
                    db, measurement
                );
            }
        }

        tracing::debug!(
            "Get {:?} with Influx query:{} in InfluxDBv2 storage",
            key,
            qs
        );

        let query = Query::new(qs);
        let mut query_result: Vec<ZenohPoint> = vec![];

        match async_std::task::block_on(async {
            self.client.query::<ZenohPoint>(Some(query)).await
        }) {
            Ok(result) => {
                query_result = result;
            }
            Err(e) => {
                tracing::error!(
                    "Couldn't get data from database {} in InfluxDBv2 storage with error: {} ",
                    db,
                    e
                );
            }
        };
        let mut result: Vec<StoredData> = vec![];

        for zpoint in query_result {
            // get the encoding
            let encoding_prefix = (if zpoint.encoding_prefix >= 0 && zpoint.encoding_prefix <= 255 {
                Ok(zpoint.encoding_prefix as u16)
            } else {
                Err(zerror!(
                    "Encoding {} is outside possible range of values",
                    zpoint.encoding_prefix
                ))
            })?;

            let encoding = if zpoint.encoding_suffix.is_empty() {
                Encoding::new(encoding_prefix, None)
            } else {
                Encoding::from(zpoint.encoding_suffix)
            };
            // get the payload
            let payload = if zpoint.base64 {
                match b64_std_engine.decode(zpoint.value) {
                    Ok(v) => ZBuf::from(v),
                    Err(e) => {
                        tracing::warn!(
                            r#"Failed to decode zenoh base64 Value from Influx point with timestamp="{}": {}"#,
                            zpoint.timestamp,
                            e
                        );
                        continue;
                    }
                }
            } else {
                ZBuf::from(zpoint.value.into_bytes())
            };
            // get the timestamp
            let timestamp = match Timestamp::from_str(&zpoint.timestamp) {
                Ok(t) => t,
                Err(e) => {
                    tracing::warn!(
                        r#"Failed to decode zenoh Timestamp from Influx point with timestamp="{}": {:?}"#,
                        zpoint.timestamp,
                        e
                    );
                    continue;
                }
            };
            let value = Value::new(payload, encoding);
            result.push(StoredData { value, timestamp });
        }
        Ok(result)
    }

    //putting a stub here to be implemented later
    async fn get_all_entries(&self) -> ZResult<Vec<(Option<OwnedKeyExpr>, Timestamp)>> {
        tracing::warn!("!!! get_all_entries is NOT implemented yet !!!"); // See : https://github.com/ZettaScaleLabs/zenoh-backend-influxdb/pull/4
        tracing::warn!("called get_all_entries in InfluxDBv2 storage");
        let mut result: Vec<(Option<OwnedKeyExpr>, Timestamp)> = Vec::new();
        // TODO: Pass real zid from session instead
        let curr_time = match std::num::NonZeroU128::new(1u128).map(new_timestamp) {
            Some(x) => x,
            None => {
                bail!("Error Creating Timestamp ID for 0x01");
            }
        };

        result.push((None, curr_time));
        return Ok(result);
    }
}

fn get_db_name(config: StorageConfig) -> ZResult<String> {
    let db = match config.volume_cfg.get(PROP_STORAGE_DB) {
        Some(serde_json::Value::String(s)) => s.to_string(),
        _ => bail!("no db was found"),
    };
    Ok(db)
}

impl Drop for InfluxDbStorage {
    fn drop(&mut self) {
        tracing::debug!("Closing InfluxDBv2 storage");
        let db = match self.config.volume_cfg.get(PROP_STORAGE_DB) {
            Some(serde_json::Value::String(s)) => s.to_string(),
            _ => {
                tracing::error!("no db was found");
                return;
            }
        };

        match self.on_closure {
            OnClosure::DropDb => {
                task::block_on(async move {
                    tracing::debug!("Close InfluxDBv2 storage, dropping database {}", db);
                    if let Err(e) = self.admin_client.delete_bucket(&db).await {
                        tracing::error!("Failed to drop InfluxDbv2 database '{}' : {}", db, e)
                    }
                });
            }
            OnClosure::DropSeries => {
                task::block_on(async move {
                    tracing::debug!(
                        "Close InfluxDBv2 storage, dropping all series from database {}",
                        db
                    );
                    let start = NaiveDateTime::MIN;
                    let stop = NaiveDateTime::MAX;
                    if let Err(e) = self.client.delete(&db, start, stop, None).await {
                        tracing::error!(
                            "Failed to drop all series from InfluxDbv2 database '{}' : {}",
                            db,
                            e
                        )
                    }
                });
            }
            OnClosure::DoNothing => {
                tracing::debug!("Close InfluxDBv2 storage, keeping database {} as it is", db);
            }
        }
    }
}

// Scheduled dropping of a measurement after a timeout, if it's empty
#[allow(dead_code)]
struct TimedMeasurementDrop {
    client: Client,
    measurement: String,
}

#[async_trait]
impl Timed for TimedMeasurementDrop {
    async fn run(&mut self) {
        //keeping the stub here for implemntation in a future time
        //currently the influxDB library doesn't allow dropping a measurement
    }
}

fn generate_db_name() -> String {
    format!("zenoh_db_{}", Uuid::new_v4().simple())
}

async fn is_db_existing(client: &Client, db: &str) -> ZResult<bool> {
    let request = ListBucketsRequest {
        name: Some(db.to_owned()),
        ..ListBucketsRequest::default()
    };

    let dbs = client.list_buckets(Some(request)).await?.buckets;
    if !dbs.is_empty() {
        Ok(true)
    } else {
        Ok(false)
    }
}

async fn create_db(client: &Client, org_id: &str, db: &str) -> ZResult<bool> {
    let result = client
        .create_bucket(Some(PostBucketRequest::new(
            org_id.to_owned(),
            db.to_owned(),
        )))
        .await;
    match result {
        Ok(_) => Ok(true),
        Err(_) => Ok(false), //can post error here
    }
}
// Returns an InfluxDB regex (see https://docs.influxdata.com/influxdb/v1.8/query_language/explore-data/#regular-expressions)
// corresponding to the list of path expressions. I.e.:
// Replace "**" with ".*", "*" with "[^\/]*"  and "/" with "\/".
// Concat each with "|", and surround the result with '/^' and '$/'.
fn key_exprs_to_influx_regex(path_exprs: &[&keyexpr]) -> String {
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
                '/' => result.push_str(r"\/"),
                _ => result.push(c),
            }
        }
    }
    result.push_str("$/");
    result
}

fn timerange_from_parameters(p: &str) -> ZResult<Option<String>> {
    use zenoh::selector::{TimeBound, TimeRange};
    let time_range = TimeRange::from_str(p);
    let mut result = String::new();
    match time_range {
        Ok(TimeRange(start, stop)) => {
            match start {
                TimeBound::Inclusive(t) => {
                    result.push_str("start:");
                    write_timeexpr(&mut result, t, 0);
                }
                TimeBound::Exclusive(t) => {
                    result.push_str("start:");
                    write_timeexpr(&mut result, t, 1);
                }
                TimeBound::Unbounded => {}
            }
            match stop {
                TimeBound::Inclusive(t) => {
                    result.push_str(", stop:");
                    write_timeexpr(&mut result, t, 1);
                }
                TimeBound::Exclusive(t) => {
                    result.push_str(", stop:");
                    write_timeexpr(&mut result, t, 0);
                }
                TimeBound::Unbounded => {}
            }
        }
        Err(err) => Err(zerror!(
            "Could not Make TimeRange From string {}  Err :{}",
            p,
            err
        ))?,
    }

    Ok(Some(result))
}

fn write_timeexpr(s: &mut String, t: TimeExpr, i: u64) {
    use std::fmt::Write;
    match t {
        TimeExpr::Fixed(t) => {
            // let tm = t +
            let time_duration = t.duration_since(UNIX_EPOCH).expect("Time went backwards")
                + Duration::from_nanos(i); //adding 1ns for inclusive timebinding ;
            let datetime = chrono::DateTime::from_timestamp(
                time_duration
                    .as_secs()
                    .try_into()
                    .expect("Error in converting seconds from u64 to i64"),
                time_duration.subsec_nanos(),
            )
            .expect("Error in converting duration to datetime");
            write!(
                s,
                "{}",
                datetime.to_rfc3339_opts(SecondsFormat::Nanos, true)
            )
        }
        TimeExpr::Now { offset_secs } => {
            let os = offset_secs * 1e9 + i as f64; //adding 1ns for inclusive timebinding
            write!(s, "{}ns", os)
        }
    }
    .unwrap()
}

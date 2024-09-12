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

use std::{
    convert::{TryFrom, TryInto},
    str::FromStr,
    sync::Arc,
    time::{Duration, UNIX_EPOCH},
};

use async_trait::async_trait;
use base64::{engine::general_purpose::STANDARD as b64_std_engine, Engine};
use chrono::{NaiveDateTime, SecondsFormat};
use futures::prelude::*;
use influxdb2::{
    api::buckets::ListBucketsRequest,
    models::{DataPoint, PostBucketRequest, Query},
    Client,
};
use uuid::Uuid;
use zenoh::{
    bytes::Encoding,
    internal::{bail, buffers::ZBuf, zerror, Value},
    key_expr::{keyexpr, OwnedKeyExpr},
    query::{Parameters, TimeExpr, ZenohParameters},
    time::Timestamp,
    try_init_log_from_env, Error, Result as ZResult,
};
use zenoh_backend_traits::{
    config::{PrivacyGetResult, PrivacyTransparentGet, StorageConfig, VolumeConfig},
    StorageInsertionResult, *,
};
use zenoh_plugin_trait::{plugin_long_version, plugin_version, Plugin};

const WORKER_THREAD_NUM: usize = 2;
const MAX_BLOCK_THREAD_NUM: usize = 50;
lazy_static::lazy_static! {
    // The global runtime is used in the dynamic plugins, which we can't get the current runtime
    static ref TOKIO_RUNTIME: tokio::runtime::Runtime = tokio::runtime::Builder::new_multi_thread()
               .worker_threads(WORKER_THREAD_NUM)
               .max_blocking_threads(MAX_BLOCK_THREAD_NUM)
               .enable_all()
               .build()
               .expect("Unable to create runtime");
}

#[macro_export]
macro_rules! await_task {
    ($e: expr, $($x:ident),*) => {
        match tokio::runtime::Handle::try_current() {
            Ok(_) => {
                $e
            },
            Err(_) => {
                // We need to clone all the variables used by async func
                $(
                    let $x = $x.clone();
                )*
                TOKIO_RUNTIME
                    .spawn(
                        async move { $e },
                    )
                    .await
                    .map_err(|e| zerror!("Unable to spawn the task: {e}"))?
            },
        }
    };
}

#[inline(always)]
fn blockon_runtime<F: Future>(task: F) -> F::Output {
    // Check whether able to get the current runtime
    match tokio::runtime::Handle::try_current() {
        Ok(rt) => {
            // Able to get the current runtime (standalone binary), spawn on the current runtime
            tokio::task::block_in_place(|| rt.block_on(task))
        }
        Err(_) => {
            // Unable to get the current runtime (dynamic plugins), spawn on the global runtime
            tokio::task::block_in_place(|| TOKIO_RUNTIME.block_on(task))
        }
    }
}

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

lazy_static::lazy_static!(
    static ref INFLUX_REGEX_ALL: String = key_exprs_to_influx_regex(&["**".try_into().unwrap()]);
    static ref NONE_KEY_REF: OwnedKeyExpr = OwnedKeyExpr::from_str(NONE_KEY).unwrap();
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
        (Some(org_id), Some(token)) => Ok(Some(InfluxDbCredentials {
            org_id: org_id.clone(),
            token: token.clone(),
        })),
        (None, None) => Ok(None),
        _ => {
            tracing::error!(
                "Couldn't get {} and {} from config",
                PROP_BACKEND_ORG_ID,
                PROP_TOKEN
            );
            bail!(
                "Properties `{}` and `{}` must both exist",
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
                match blockon_runtime(admin_client.ready()) {
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

impl InfluxDbVolume {
    async fn create_db(&self, org_id: &str, db: &str) -> ZResult<()> {
        let post_bucket_options = PostBucketRequest::new(org_id.into(), db.into());
        let admin_client = self.admin_client.clone();
        match await_task!(admin_client.create_bucket(Some(post_bucket_options)).await,) {
            Ok(_) => tracing::info!("Created {db} Influx"),
            Err(e) => bail!("Failed to create InfluxDBv2 Storage : {:?}", e),
        }
        Ok(())
    }
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
        let volume_cfg = config.volume_cfg.as_object().ok_or_else(|| {
            zerror!("InfluxDBv2 backed storages need some volume-specific configuration")
        })?;

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

        let storage_creds = match extract_credentials(volume_cfg)? {
            Some(creds) => creds,
            None => {
                match &self.credentials {
                    Some(creds) => {
                        tracing::debug!("Credentials not provided for new storage '{}'. Using volume credentials.", db);
                        InfluxDbCredentials {
                            org_id: creds.org_id.clone(),
                            token: creds.token.clone(),
                        }
                    }
                    None => bail!("No credentials specified to access database '{}'.", db),
                }
            }
        };

        // Client::new can panic: TODO : Switch to libraries without Panics
        let client = match std::panic::catch_unwind(|| {
            Client::new(
                url.clone(),
                storage_creds.org_id.clone(),
                storage_creds.token.clone(),
            )
        }) {
            Ok(client) => client,
            Err(e) => bail!("Error in creating client for InfluxDBv2 storage: {:?}", e),
        };

        let client_cloned = client.clone();
        let db_cloned = db.clone();
        match await_task!(does_db_exist(&client_cloned, &db_cloned).await,) {
            Ok(db_exists) => {
                if !db_exists && createdb {
                    // Try to create db using user credentials
                    self.create_db(&storage_creds.org_id, &db).await?
                } else if db_exists && createdb {
                    tracing::warn!("Database '{db}' already exists exists in Influx and config 'create_db'='true'");
                }
            }
            Err(e) => bail!("Failed to get Buckets from InfluxDB : {:?}", e),
        };

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

        let db_name: String = match config.volume_cfg.get(PROP_STORAGE_DB) {
            Some(serde_json::Value::String(s)) => s.to_string(),
            _ => bail!("No `db` was found in Config"),
        };

        Ok(Box::new(InfluxDbStorage {
            db_name,
            config,
            admin_client,
            client: Arc::new(client),
            on_closure,
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

/// ZenohPoint is a representation of a Zenoh Sample Stored in the Database
/// `encoding_prefix`` and `encoding_suffix` are stored separately for backwards compatability
/// The payload of the Sample is stored as a String in influx, possibly Base64 encoded bytes if it cannot be deserialized as such

#[derive(Debug, Default)]
struct ZenohPoint {
    // NOTE: "kind" is present within InfluxDB and used in query clauses, but not read in Rust...
    kind: String,
    timestamp: String,
    encoding_prefix: i64, //should be u8 but not supported in v2.x so using a workaround
    encoding_suffix: String,
    base64: bool,
    value: String,
    key_expr: String,
}

// Safest Thing for now is to send back default values if keys dont exist in query
// But a proper solution would be real deserializing of the structure
// Either you get the full data into a Zenoh Point or the function fails to deserialize
// Or we make the fields of a Zenoh Point optional
// I do not like using a Default value and expecting the GenericMap to have the values
// The underlying library influxDB2 must change to support proper Deserialization
// as influxdb2::FromMap should be Failable
impl influxdb2::FromMap for ZenohPoint {
    fn from_genericmap(map: influxdb2_structmap::GenericMap) -> Self {
        use influxdb2_structmap::value::Value as V;

        let mut z_point = ZenohPoint::default();

        if let Some(V::String(kind)) = map.get("kind") {
            z_point.kind = kind.clone();
        };
        if let Some(V::String(timestamp)) = map.get("timestamp") {
            z_point.timestamp = timestamp.clone();
        };
        if let Some(V::Long(encoding_prefix)) = map.get("encoding_prefix") {
            z_point.encoding_prefix = *encoding_prefix;
        };
        if let Some(V::String(encoding_suffix)) = map.get("encoding_suffix") {
            z_point.encoding_suffix = encoding_suffix.clone();
        };
        if let Some(V::Bool(base64)) = map.get("base64") {
            z_point.base64 = *base64;
        };
        if let Some(V::String(value)) = map.get("value") {
            z_point.value = value.clone();
        };
        if let Some(V::String(_measurement)) = map.get("_measurement") {
            z_point.key_expr = _measurement.clone();
        };
        z_point
    }
}

struct InfluxDbStorage {
    db_name: String,
    config: StorageConfig,
    admin_client: Client,
    client: Arc<Client>,
    on_closure: OnClosure,
}

impl InfluxDbStorage {
    async fn get_deletion_timestamp(&self, measurement: &str) -> ZResult<Option<Timestamp>> {
        let qs = format!(
            "from(bucket: \"{}\")
                |> range(start: {})
                |> filter(fn: (r) => r._measurement == \"{}\")
                |> filter(fn: (r) => r[\"kind\"] == \"DEL\")
                |> last()",
            self.db_name, 0, measurement
        );

        // get the value and if it exists then extract the timestamp from it
        let query = Query::new(qs);

        let client = self.client.clone();
        let query_result: Vec<ZenohPoint> =
            match await_task!(client.query::<ZenohPoint>(Some(query)).await,) {
                Ok(result) => result,
                Err(e) => {
                    tracing::error!(
                        "Couldn't get data from InfluxDBv2 database {} with error: {} ",
                        self.db_name,
                        e
                    );
                    return Ok(None);
                }
            };

        match query_result.first() {
            Some(zp) => match Timestamp::from_str(&zp.timestamp) {
                Ok(ts) => Ok(Some(ts)),
                Err(e) => bail!("Couldn't parse the deletion timestamp {:?}", e),
            },
            None => Ok(None),
        }
    }
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
        let measurement = key.unwrap_or_else(|| NONE_KEY_REF.clone());

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

        let (base64, strvalue) = match value.payload().deserialize::<String>() {
            Ok(s) => (false, s),
            Err(err) => (true, b64_std_engine.encode(err.to_string())),
        };

        // Note: tags are stored as strings in InfluxDB, while fileds are typed.
        // For simpler/faster deserialization, we store encoding, timestamp and base64 as fields.
        // while the kind is stored as a tag to be indexed by InfluxDB and have faster queries on it.
        let encoding_string_rep = value.encoding().clone().to_string(); // field only supports Strings and not Vec<u8>
        let encoding = value.encoding();

        let zenoh_point = vec![DataPoint::builder(measurement.clone())
            .tag("kind", "PUT")
            .field("timestamp", timestamp.to_string())
            .field("encoding_prefix", encoding.id() as i64) //should be u8 but not supported in v2.x so using a workaround
            .field("encoding_suffix", encoding_string_rep) // TODO: Left for compatibility, consider replacing wiht single "Encoding" field
            .field("base64", base64)
            .field("value", strvalue)
            .timestamp(influx_time) //converted timestamp to i64
            .build()?];

        let client = self.client.clone();
        let db_name = self.db_name.clone();

        match await_task!(client.write(&db_name, stream::iter(zenoh_point)).await,) {
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
        let measurement = key.unwrap_or_else(|| NONE_KEY_REF.clone());

        // Note: assume that uhlc timestamp was generated by a clock using UNIX_EPOCH (that's the case by default)
        // delete all points from the measurement that are older than this DELETE message
        // (in case more recent PUT have been recevived un-ordered)

        let start_timestamp = NaiveDateTime::UNIX_EPOCH;
        let stop_timestamp = chrono::DateTime::from_timestamp(
            timestamp.get_time().as_secs() as i64,
            timestamp.get_time().subsec_nanos(),
        )
        .ok_or_else(|| zerror!("delete: stop_timestamp {timestamp} out of range"))?
        .naive_utc();

        let predicate = Some(format!("_measurement=\"{measurement}\"")); //can be specified with tag or field values
        tracing::debug!(
            "Delete {:?} with Influx query in InfluxDBv2 storage, Time Range: {:?} - {:?}, Predicate: {:?}",
            measurement, start_timestamp, stop_timestamp, predicate
        );

        let client = self.client.clone();
        let db_name = self.db_name.clone();
        if let Err(e) = await_task!(
            client
                .delete(&db_name, start_timestamp, stop_timestamp, predicate)
                .await,
        ) {
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

        let client = self.client.clone();
        let db_name = self.db_name.clone();
        if let Err(e) = await_task!(client.write(&db_name, stream::iter(zenoh_point)).await,) {
            bail!(
                "Failed to mark measurement {:?} as deleted : {} in InfluxDBv2 storage",
                measurement,
                e
            )
        }
        // schedule_measurement_drop is used to schedule the drop of measurement later in the future, if it's empty
        // influx 2.x doesn't support dropping measurements from the API
        Ok(StorageInsertionResult::Deleted)
    }

    async fn get(
        &mut self,
        key: Option<OwnedKeyExpr>,
        parameters: &str,
    ) -> ZResult<Vec<StoredData>> {
        let owned_key = key.unwrap_or_else(|| NONE_KEY_REF.clone());

        // Bucket Name
        let mut query_string = format!("from(bucket: \"{}\")\n", self.db_name);

        // Time Range
        match timerange_from_parameters(parameters)? {
            Some(range) => query_string.push_str(&format!("|> range({})\n", range)),
            None => query_string.push_str("|> range(start:0)\n"), // Could not Parse out valid Timerange
        };

        // Filter Measurement
        if owned_key.contains('*') {
            let regex = key_exprs_to_influx_regex(&[&owned_key]);
            query_string.push_str(&format!(
                "|> filter(fn: (r) => r._measurement =~ {})\n",
                regex
            ));
        } else {
            query_string.push_str(&format!(
                "|> filter(fn: (r) => r._measurement == \"{}\")\n",
                owned_key
            ));
        };

        // Filter Kind
        query_string.push_str("|> filter(fn: (r) => r[\"kind\"] == \"PUT\")\n");

        // Execute Query
        tracing::debug!(
            "Get {:?} with Influx query:{} in InfluxDBv2 storage",
            owned_key,
            query_string
        );

        let query = Query::new(query_string);
        let client = self.client.clone();

        let query_result: Vec<ZenohPoint> =
            match await_task!(client.query::<ZenohPoint>(Some(query)).await,) {
                Ok(result) => result,
                Err(e) => {
                    tracing::error!(
                        "Couldn't get data from database {} in InfluxDBv2 storage with error: {} ",
                        self.db_name,
                        e
                    );
                    vec![]
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

    /// Gets the Latest of Each Key Expression from the database
    /// Example:
    /// Database contains the following (KeyExpr,Timestamp) tuples
    /// [
    ///     ("demo/a",t1),
    ///     ("demo/a",t3),
    ///     ("demo/b",t2),
    ///     ("demo/b",t5),
    ///     ("demo/a",t2),
    /// ]
    /// get_all_entries returns
    /// [
    ///     ("demo/a",t3),
    ///     ("demo/b",t5)
    /// ]
    async fn get_all_entries(&self) -> ZResult<Vec<(Option<OwnedKeyExpr>, Timestamp)>> {
        let mut result: Vec<(Option<OwnedKeyExpr>, Timestamp)> = Vec::new();

        let qs: String = format!(
            "from(bucket: \"{}\")
                    |> range(start:0)
                    |> last()
            ",
            self.db_name
        );

        let query = Query::new(qs);

        let client = self.client.clone();
        let vec_zpoint: Vec<ZenohPoint> =
            match await_task!(client.query::<ZenohPoint>(Some(query)).await,) {
                Ok(result) => result,
                Err(e) => {
                    bail!(
                        "Couldn't get data from database {} in InfluxDBv2 storage with error: {} ",
                        self.db_name,
                        e
                    );
                }
            };

        for zp in vec_zpoint {
            match (
                OwnedKeyExpr::from_str(&zp.key_expr),
                Timestamp::from_str(&zp.timestamp),
            ) {
                (Ok(ke), Ok(ts)) => {
                    if ke.eq(&NONE_KEY_REF) {
                        result.push((None, ts))
                    } else {
                        result.push((Some(ke), ts))
                    }
                }
                (Err(err_ke), Err(err_ts)) => tracing::warn!(
                    "Failed to parse (OwnedKeyExpr,Timestamp):({:?},{:?})  Errors:({:?},{:?})",
                    zp.key_expr,
                    zp.timestamp,
                    err_ke,
                    err_ts
                ),
                (_, Err(err)) => tracing::warn!(
                    "Failed to parse Timestamp:{:?}  Error:{:?}",
                    zp.timestamp,
                    err
                ),
                (Err(err), _) => tracing::warn!(
                    "Failed to parse OwnedKeyExpr:{:?}  Error:{:?}",
                    zp.timestamp,
                    err
                ),
            };
        }

        return Ok(result);
    }
}

impl Drop for InfluxDbStorage {
    fn drop(&mut self) {
        tracing::debug!("Closing InfluxDBv2 storage");
        match self.on_closure {
            OnClosure::DropDb => {
                let db_name = self.db_name.clone();
                let org = self.client.org.clone();
                if let Err(e) = blockon_runtime(async {
                    tracing::debug!("Getting bucket ID for database {}", db_name);
                    let list_buckets_req = ListBucketsRequest {
                        after: None,
                        id: None,
                        limit: None,
                        name: Some(db_name.clone()),
                        offset: None,
                        org: None,
                        org_id: Some(org),
                    };
                    let response = self
                        .admin_client
                        .list_buckets(Some(list_buckets_req))
                        .await?;
                    if response.buckets.is_empty() {
                        bail!("Received empty bucket list from database");
                    }
                    if response.buckets.len() > 1 {
                        bail!("Influxdb2 bucket list contains more than one matching database");
                    }
                    let bucket_id = response.buckets[0]
                        .id
                        .clone()
                        .ok_or_else(|| zerror!("database bucket ID is None"))?;
                    tracing::debug!(
                        "Close InfluxDBv2 storage, dropping database {} with bucket ID {}",
                        db_name,
                        bucket_id
                    );
                    self.admin_client.delete_bucket(&bucket_id).await?;

                    Ok::<(), Error>(())
                }) {
                    tracing::error!(
                        "Failed to drop InfluxDbv2 database '{}': {}",
                        self.db_name,
                        e
                    );
                }
            }
            OnClosure::DropSeries => {
                blockon_runtime(async move {
                    tracing::debug!(
                        "Close InfluxDBv2 storage, dropping all series from database {}",
                        self.db_name
                    );
                    let start = NaiveDateTime::MIN;
                    let stop = NaiveDateTime::MAX;
                    if let Err(e) = self.client.delete(&self.db_name, start, stop, None).await {
                        tracing::error!(
                            "Failed to drop all series from InfluxDbv2 database '{}' : {}",
                            self.db_name,
                            e
                        )
                    }
                });
            }
            OnClosure::DoNothing => {
                tracing::debug!(
                    "Close InfluxDBv2 storage, keeping database {} as it is",
                    self.db_name
                );
            }
        }
    }
}

fn generate_db_name() -> String {
    format!("zenoh_db_{}", Uuid::new_v4().simple())
}

async fn does_db_exist(client: &Client, db: &str) -> ZResult<bool> {
    Ok(client
        .list_buckets(None)
        .await?
        .buckets
        .into_iter()
        .any(|bucket| bucket.name == db))
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
    use zenoh::query::{TimeBound, TimeRange};

    if p.is_empty() {
        return Ok(None);
    }

    let parameters = Parameters::from(p);
    let time_range = match parameters.time_range() {
        Some(time_range) => time_range,
        None => return Ok(None),
    };

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
                TimeBound::Unbounded => {
                    result.push_str("start:1970-01-01T00:00:00Z");
                }
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
            "Could not Make TimeRange From string '{}'  Err :{}",
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

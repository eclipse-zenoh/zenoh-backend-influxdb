<img src="http://zenoh.io/img/zenoh-dragon-small.png" width="150">

[![CI](https://github.com/eclipse-zenoh/zenoh-backend-influxdb/workflows/CI/badge.svg)](https://github.com/eclipse-zenoh/zenoh-backend-influxdb/actions?query=workflow%3A%22CI%22)
[![Gitter](https://badges.gitter.im/atolab/zenoh.svg)](https://gitter.im/atolab/zenoh?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge)
[![License](https://img.shields.io/badge/License-EPL%202.0-blue)](https://choosealicense.com/licenses/epl-2.0/)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)

# InfluxDB backend for Eclipse zenoh

In zenoh a backend is a storage technology (such as DBMS, time-series database, file system...) alowing to store the
keys/values publications made via zenoh and return them on queries.
See the [zenoh documentation](http://zenoh.io/docs/manual/backends/) for more details.

This backend relies on an [InfluxDB](https://www.influxdata.com/products/influxdb/) server
to implement the storages.
Its library name (without OS specific prefix and extension) that zenoh will rely on to find it and load it is **`zbackend_influxdb`**.

:point_right: **Download stable versions:** https://download.eclipse.org/zenoh/zenoh-backend-influxdb/

:point_right: **Build "master" branch:** see [below](#How-to-build-it)

-------------------------------
## :warning: Documentation for previous 0.5 versions:
The following documentation related to the version currently in development in "master" branch: 0.6.x.

For previous versions see the README and code of the corresponding tagged version:
 - [0.5.0-beta.9](https://github.com/eclipse-zenoh/zenoh-backend-influxdb/tree/0.5.0-beta.9#readme)
 - [0.5.0-beta.8](https://github.com/eclipse-zenoh/zenoh-backend-influxdb/tree/0.5.0-beta.8#readme)

-------------------------------
## **Examples of usage**

Prerequisites:
 - You have a zenoh router (`zenohd`) installed, and the `zbackend_influxdb` library file is available in `~/.zenoh/lib`.
 - You have an InfluxDB service running and listening on `http://localhost:8086`

You can setup storages either at zenoh router startup via a configuration file, either at runtime via the zenoh admin space, using for instance the REST API.

### **Setup via a JSON configuration file**

  - Create a `zenoh.json5` configuration file containing:
    ```json5
    {
      plugins: {
        // configuration of "storages" plugin:
        storages: {
          backends: {
            // configuration of a "influxdb" backend (the "zbackend_influxdb" library will be loaded at startup)
            influxdb: {
              // URL to the InfluxDB service
              url: "http://localhost:8086",
              storages: {
                // configuration of a "demo" storage using the "influxdb" backend
                demo: {
                  // the key expression this storage will subscribes to
                  key_expr: "/demo/example/**",
                  // the database name within InfluxDB
                  db: "zenoh_example",
                  // if the database doesn't exist, create it
                  create_db: true
    } } } } } } }
    ```
  - Run the zenoh router with:  
    `zenohd -c zenoh.json5`

### **Setup at runtime via `curl` commands on the admin space**

  - Run the zenoh router without any specific configuration, but loading the storages plugin:  
    `zenohd -P storages`
  - Add the "influxdbn" backend (the "zbackend_fs" library will be loaded), connected to InfluxDB service on http://localhost:8086:
    `curl -X PUT -H 'content-type:application/json' -d '{url:"http://localhost:8086"}' http://localhost:8000/@/router/local/config/plugins/storages/backends/influxdb`
 - Add the "demo" storage using the "influxdb" backend:
   `curl -X PUT -H 'content-type:application/json' -d '{key_expr:"/demo/example/**",db:"zenoh_example",create_db:true}' http://localhost:8000/@/router/local/config/plugins/storages/backends/influxdb/storages/demo`

### **Tests using the REST API**

Using `curl` to publish and query keys/values, you can:
```bash
# Put some values at different time intervals
curl -X PUT -d "TEST-1" http://localhost:8000/demo/example/test
curl -X PUT -d "TEST-2" http://localhost:8000/demo/example/test
curl -X PUT -d "TEST-3" http://localhost:8000/demo/example/test

# Retrive them as a time serie
curl http://localhost:8000/demo/example/test?(starttime=0)
```

<!-- TODO: after release of eclipse/zenoh:0.6.0 update wrt. conf file and uncomment this:

### **Usage with `eclipse/zenoh` Docker image**
Alternatively, you can test running both the zenoh router and the InfluxDB service in Docker containers:
 - Download the [docker-compose.yml](https://github.com/eclipse-zenoh/zenoh-backend-influxdb/blob/master/docker-compose.yml) file
 - In the same directory, create the `./zenoh_docker/lib` sub-directories and place the `libzbackend_influxdb.so` library
   for `x86_64-unknown-linux-musl` target within.
 - Start the containers running 
   ```bash
   docker-compose up -d
   ```
 - Run the `curl` commands above, replacing the URL to InfluxDB with `http://influxdb:8086` (instead of localhost)
-->Ò

-------------------------------
## **Properties for Backend creation**

- **`"lib"`** (optional) : the path to the backend library file. If not speficied, the Backend identifier in admin space must be `influxdb` (i.e. zenoh will automatically search for a library named `zbackend_influxdb`).

- **`"url"`** (**required**) : an URL to the InfluxDB service. Example: `http://localhost:8086`

- **`"username"`** (optional) : an [InfluxDB admin](https://docs.influxdata.com/influxdb/v1.8/administration/authentication_and_authorization/#admin-users) user name. It will be used for creation of databases, granting read/write privileges of databases mapped to storages and dropping of databases and measurements.

- **`"password"`** (optional) : the admin user's password.

-------------------------------
## **Properties for Storage creation**

- **`"key_expr"`** (**required**) : the Storage's [Key Expression](../abstractions#key-expression)

- **`"strip_prefix"`** (optional) : a prefix of the `"key_expr"` that will be stripped from each key to store.  
  _Example: with `"key_expr"="/demo/example/**"` and `"strip_prefix"="/demo/example/"` the key `"/demo/example/foo/bar"` will be stored as key: `"foo/bar"`. But replying to a get on `"/demo/**"`, the key `"foo/bar"` will be transformed back to the original key (`"/demo/example/foo/bar"`)._

- **`"db"`** (optional) : the InfluxDB database name the storage will map into. If not specified, a random name will be generated, and the corresponding database will be created (even if `"create_db"` is not set).

- **`"create_db"`** (optional) : create the InfluxDB database if not already existing.
  By default the database is not created, unless `"db"` property is not specified.
  *(the value doesn't matter, only the property existence is checked)*

- **`"on_closure"`** (optional) : the strategy to use when the Storage is removed. There are 3 options:
  - *unset*: the database remains untouched (this is the default behaviour)
  - `"drop_db"`: the database is dropped (i.e. removed)
  - `"drop_series"`: all the series (measurements) are dropped and the database remains empty.

- **`"username"`** (optional) : an InfluxDB user name (usually [non-admin](https://docs.influxdata.com/influxdb/v1.8/administration/authentication_and_authorization/#non-admin-users)). It will be used to read/write points in the database on GET/PUT/DELETE zenoh operations.

- **`"password"`** (optional) : the user's password.

-------------------------------
## **Behaviour of the backend**

### Mapping to InfluxDB concepts
Each **storage** will map to an InfluxDB **database**.  
Each **key** to store will map to a an InfluxDB
[**measurement**](https://docs.influxdata.com/influxdb/v1.8/concepts/key_concepts/#measurement)
named with the key stripped from the `"strip_prefix"` property (see below).  
Each **key/value** put into the storage will map to an InfluxDB
[**point**](https://docs.influxdata.com/influxdb/v1.8/concepts/key_concepts/#point) reusing the timestamp set by zenoh
(but with a precision of nanoseconds). The fileds and tags of the point is are the following:
 - `"kind"` tag: the zenoh change kind (`"PUT"` for a value that have been put, or `"DEL"` to mark the deletion of the key)
 - `"timestamp"` field: the original zenoh timestamp
 - `"encoding"` field: the value's encoding flag
 - `"base64"` field: a boolean indicating if the value is encoded in base64
 - `"value"`field: the value as a string, possibly encoded in base64 for binary values.

### Behaviour on deletion
On deletion of a key, all points with a timestamp before the deletion message are deleted.
A point with `"kind"="DEL`" is inserted (to avoid re-insertion of points with an older timestamp in case of un-ordered messages).
After a delay (5 seconds), the measurement corresponding to the deleted key is dropped if it still contains no points.

### Behaviour on GET
On GET operations, by default the storage returns only the latest point for each key/measurement.
This is to be coherent with other backends technologies that only store 1 value per-key.  
If you want to get time-series as a result of a GET operation, you need to specify the `"starttime"` and/or `"stoptime"`
properties in your [Selector](../abstractions#selector).

Examples of selectors:
```bash
  # get the complete time-series
  /demo/example/**?(starttime=0)

  # get points within a fixed date interval
  /demo/example/influxdb/**?(starttime=2020-01-01;starttime=2020-01-02T12:00:00.000000000Z)

  # get points within a relative date interval
  /demo/example/influxdb/**?(starttime=now()-2d;stoptime=now()-1d)
```

The `"starttime"` and `"stoptime"` properties support the InfluxDB **[time syntax](https://docs.influxdata.com/influxdb/v1.8/query_language/explore-data/#time-syntax)** (*<rfc3339_date_time_string>*, *<rfc3339_like_date_time_string>*, *<epoch_time>* and relative time using `now()`).


-------------------------------
## How to build it

At first, install [Cargo and Rust](https://doc.rust-lang.org/cargo/getting-started/installation.html). 

:warning: **WARNING** :warning: : As Rust doesn't have a stable ABI, the backend library should be
built with the exact same Rust version than `zenohd`. Otherwise, incompatibilities in memory mapping
of shared types between `zenohd` and the library can lead to a `"SIGSEV"` crash.

To know the Rust version you're `zenohd` has been built with, use the `--version` option.  
Example:
```bash
$ zenohd --version
The zenoh router v0.6.0-dev-24-g1f20c86 built with rustc 1.57.0 (f1edd0429 2021-11-29)
```
Here, `zenohd` has been built with the rustc version `1.57.0`.  
Install and use this toolchain with the following command:

```bash
$ rustup default 1.57.0
```

And then build the backend with:

```bash
$ cargo build --release --all-targets
```

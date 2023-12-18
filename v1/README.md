<img src="https://raw.githubusercontent.com/eclipse-zenoh/zenoh/master/zenoh-dragon.png" height="150">

[![CI](https://github.com/eclipse-zenoh/zenoh-backend-influxdb/workflows/CI/badge.svg)](https://github.com/eclipse-zenoh/zenoh-backend-influxdb/actions?query=workflow%3A%22CI%22)
[![Discussion](https://img.shields.io/badge/discussion-on%20github-blue)](https://github.com/eclipse-zenoh/roadmap/discussions)
[![Discord](https://img.shields.io/badge/chat-on%20discord-blue)](https://discord.gg/2GJ958VuHs)
[![License](https://img.shields.io/badge/License-EPL%202.0-blue)](https://choosealicense.com/licenses/epl-2.0/)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)

# Eclipse Zenoh

The Eclipse Zenoh: Zero Overhead Pub/sub, Store/Query and Compute.

Zenoh (pronounce _/zeno/_) unifies data in motion, data at rest and computations. It carefully blends traditional pub/sub with geo-distributed storages, queries and computations, while retaining a level of time and space efficiency that is well beyond any of the mainstream stacks.

Check the website [zenoh.io](http://zenoh.io) and the [roadmap](https://github.com/eclipse-zenoh/roadmap) for more detailed information.

-------------------------------
# InfluxDB backend

In Zenoh a backend is a storage technology (such as DBMS, time-series database, file system...) alowing to store the
keys/values publications made via zenoh and return them on queries.
See the [zenoh documentation](http://zenoh.io/docs/manual/backends/) for more details.

This backend relies on an [InfluxDB](https://www.influxdata.com/products/influxdb/) server
to implement the storages.
Its library name (without OS specific prefix and extension) that zenoh will rely on to find it and load it is **`zenoh_backend_influxdb`**.

:point_right: **Install latest release:** see [below](#How-to-install-it)

:point_right: **Build "master" branch:** see [below](#How-to-build-it)

:note: This has partial support for InfluxDB 2.x.

-------------------------------
## :warning: Documentation for previous 0.5 versions:
The following documentation related to the version currently in development in "master" branch: 0.6.x.

For previous versions see the README and code of the corresponding tagged version:
 - [0.5.0-beta.9](https://github.com/eclipse-zenoh/zenoh-backend-influxdb/tree/0.5.0-beta.9#readme)
 - [0.5.0-beta.8](https://github.com/eclipse-zenoh/zenoh-backend-influxdb/tree/0.5.0-beta.8#readme)

-------------------------------
## **Examples of usage**

Prerequisites:
 - You have a zenoh router (`zenohd`) installed, and the `zenoh_backend_influxdb` library file is available in `~/.zenoh/lib`.
 - You have an InfluxDB service running and listening on `http://localhost:8086`

You can setup storages either at zenoh router startup via a configuration file, either at runtime via the zenoh admin space, using for instance the REST API.

### **Setup via a JSON5 configuration file**

  - Create a `zenoh.json5` configuration file containing for example:
    ```json5
    {
      plugins: {
        // configuration of "storage_manager" plugin:
        storage_manager: {
          volumes: {
            // configuration of a "influxdb" volume (the "zenoh_backend_influxdb" backend library will be loaded at startup)
            //this should be named influxdb for v1
            influxdb: {
              // URL to the InfluxDB service
              url: "http://localhost:8086",
              private: {
                //For Influxdb v1.x:
                // If needed: InfluxDB credentials, preferably admin for databases creation and drop
                  //username: "admin",
                  //password: "password"
                
                //For Influxdb v2.x:
                // If needed: InfluxDB credentials, preferably ALL-ACCESS for databases creation and drop
                //if not ALL-ACCESS then atleast with authorization to create/delete buckets
                //Note: this should not be left empty for the plugin to work; if you have no admin creds, you can copy the user creds instead
                //  token: "access token"

              }
            }
          },
          storages: {
            // configuration of a "demo" storage using the "influxdb" volume
            demo: {
              // the key expression this storage will subscribes to
              key_expr: "demo/example/**",
              // this prefix will be stripped from the received key when converting to database key.
              // i.e.: "demo/example/a/b" will be stored as "a/b"
              // this option is optional
              strip_prefix: "demo/example",
              volume: {
                //this will be influxdb for v1 (exactly the same name as in volumes section)
                id: "influxdb",
                // the database/bucket name within InfluxDB
                db: "zenoh_example",
                // if the database doesn't exist, create it
                create_db: false,
                // strategy on storage closure
                on_closure: "do_nothing",
                private: {

                  //For Influxdb v1.x:
                  //Required: InfluxDB credentials to read-write on the bucket
                  //username: "admin",
                  //password: "password"
                
                //For Influxdb v2.x:
                  //Required 
                  //InfluxDB credentials, with read/write privileges for the database
                    //this is a token with either:
                    //a.) Read-Write access to the existing DB named above in the config (case where db/bucket already exists)
                    //b.) Read-write access to ALL buckets in the organization so it can access the new bucket created by zenoh;
                                //(case where a new db/bucket has to be created)
                   // token: "user access token" 
                }
              }
            }
          }
        },
        // Optionally, add the REST plugin
        rest: { http_port: 8000 }
      }
    }
    ```
  - Run the zenoh router with:  
    `zenohd -c zenoh.json5`

### **Setup at runtime via `curl` commands on the admin space**

  - Run the zenoh router, with write permissions to its admin space:  
    `zenohd --adminspace-permissions rw`
  - Add the "influxdb" volume (the "zenoh_backend_fs" library will be loaded), connected to InfluxDB service on http://localhost:8086:
    `curl -X PUT -H 'content-type:application/json' -d '{url:"http://localhost:8086"}' http://localhost:8000/@/router/local/config/plugins/storage_manager/volumes/influxdb`
  - Add the "demo" storage using the "influxdb" volume:
    `curl -X PUT -H 'content-type:application/json' -d '{key_expr:"demo/example/**",volume:{id:"influxdb",db:"zenoh_example",create_db:true}}' http://localhost:8000/@/router/local/config/plugins/storage_manager/storages/demo`

### **Tests using the REST API**

Using `curl` to publish and query keys/values, you can:
```bash
# Put some values at different time intervals
curl -X PUT -d "TEST-1" http://localhost:8000/demo/example/test
curl -X PUT -d "TEST-2" http://localhost:8000/demo/example/test
curl -X PUT -d "TEST-3" http://localhost:8000/demo/example/test

# Retrive them as a time serie where '_time=[..]' means "infinite time range"
curl -g 'http://localhost:8000/demo/example/test?_time=[..]'
```

<!-- TODO: after release of eclipse/zenoh:0.6.0 update wrt. conf file and uncomment this:

### **Usage with `eclipse/zenoh` Docker image**
Alternatively, you can test running both the zenoh router and the InfluxDB service in Docker containers:
 - Download the [docker-compose.yml](https://github.com/eclipse-zenoh/zenoh-backend-influxdb/blob/master/docker-compose.yml) file
 - In the same directory, create the `./zenoh_docker/lib` sub-directories and place the `libzenoh_backend_influxdb.so` library
   for `x86_64-unknown-linux-musl` target within.
 - Start the containers running 
   ```bash
   docker-compose up -d
   ```
 - Run the `curl` commands above, replacing the URL to InfluxDB with `http://influxdb:8086` (instead of localhost)
-->

-------------------------------
## Volume configuration
InfluxDB-backed volumes need some configuration to work:

- **`"url"`** (**required**) : a URL to the InfluxDB service. Example: `http://localhost:8086`

#### admin levelcredentials:


#### for v1.x
- **`"username"`** (optional) : an [InfluxDB admin](https://docs.influxdata.com/influxdb/v1.8/administration/authentication_and_authorization/#admin-users) user name. It will be used for creation of databases, granting read/write privileges of databases mapped to storages and dropping of databases and measurements.

- **`"password"`** (optional) : the admin user's password.

Both `username` and `password` should be hidden behind a `private` object, as shown in the example [above](#setup-via-a-json5-configuration-file). In general, if you wish for a part of the configuration to be hidden when configuration is queried, you should hide it behind a `private` object.


#### for v2.x

- **`"token"`** (optional) : the admin user's token. It will be used for creation and dropping of databases. In Influxdb2.x, you can use an ALL ACCESS token for this (https://docs.influxdata.com/influxdb/cloud/admin/tokens/#all-access-token)

The `token` should be hidden behind a `private` object, like the "username" and "password" shown in the example [above](#setup-via-a-json5-configuration-file). In general, if you wish for a part of the configuration to be hidden when configuration is queried, you should hide it behind a `private` object.

-------------------------------
## Volume-specific storage configuration
Storages relying on a `influxdb` backed volume may have additional configuration through the `volume` section:
- **`"db"`** (optional, string) : the InfluxDB database name the storage will map into. If not specified, a random name will be generated, and the corresponding database will be created (even if `"create_db"` is not set).

- **`"create_db"`** (optional, boolean) : create the InfluxDB database if not already existing.
  By default the database is not created, unless `"db"` property is not specified.
  *(the value doesn't matter, only the property existence is checked)*

- **`"on_closure"`** (optional, string) : the strategy to use when the Storage is removed. There are 3 options:
  - *unset* or `"do_nothing"`: the database remains untouched (this is the default behaviour)
  - `"drop_db"`: the database is dropped (i.e. removed)
  - `"drop_series"`: all the series (measurements) are dropped and the database remains empty.

- **`"token"`** (optional, string) :  an InfluxDB access token, usually [non-admin](https://docs.influxdata.com/influxdb/cloud/admin/tokens/#readwrite-token). It will be used to read/write points in the database on GET/PUT/DELETE zenoh operations.

-------------------------------
## **Behaviour of the backend**

### Mapping to InfluxDB concepts
Each **storage** will map to an InfluxDB **database**.  
Each **key** to store will map to an InfluxDB
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
In v1, zenoh used to drop the measurement corresponding to the deleted key is dropped if it still contains no points after 5secs. However, influxdb 2.x doesn't support this feature.

### Behaviour on GET
On GET operations, by default the storage returns only the latest point for each key/measurement.
This is to be coherent with other backends technologies that only store 1 value per-key.  
If you want to get time-series as a result of a GET operation, you need to specify a time range via
the `"_time"`argument in your [Selector](https://github.com/eclipse-zenoh/roadmap/tree/main/rfcs/ALL/Selectors).

:note: Right now, wild chunks like * and ** works only for Influxdb 1.x.
This is due to lack of support in Influxdb 2.x API for our approach.

Examples of selectors:
```bash
  # get the complete time-series
  /demo/example/**?_time=[..]

  # get points within a fixed date interval
  /demo/example/influxdb/**?_time=[2020-01-01T00:00:00Z..2020-01-02T12:00:00.000000000Z]

  # get points within a relative date interval
  /demo/example/influxdb/**?_time=[now(-2d)..now(-1d)]
```

See the [`"_time"` RFC](https://github.com/eclipse-zenoh/roadmap/blob/main/rfcs/ALL/Selectors/_time.md) for a complete description of the time range format


-------------------------------
## How to install it

To install the latest release of this backend library, you can do as follows:

### Manual installation (all platforms)

All release packages can be downloaded from:  
 - https://download.eclipse.org/zenoh/zenoh-backend-influxdb/latest/   

Each subdirectory has the name of the Rust target. See the platforms each target corresponds to on https://doc.rust-lang.org/stable/rustc/platform-support.html

Choose your platform and download the `.zip` file.  
Unzip it in the same directory than `zenohd` or to any directory where it can find the backend library (e.g. /usr/lib or ~/.zenoh/lib)

### Linux Debian

Add Eclipse Zenoh private repository to the sources list, and install the `zenoh-backend-influxdb-v1` package:

```bash
echo "deb [trusted=yes] https://download.eclipse.org/zenoh/debian-repo/ /" | sudo tee -a /etc/apt/sources.list > /dev/null
sudo apt update
sudo apt install zenoh-backend-influxdb-v1
```


-------------------------------
## How to build it

> :warning: **WARNING** :warning: : Zenoh and its ecosystem are under active development. When you build from git, make sure you also build from git any other Zenoh repository you plan to use (e.g. binding, plugin, backend, etc.). It may happen that some changes in git are not compatible with the most recent packaged Zenoh release (e.g. deb, docker, pip). We put particular effort in mantaining compatibility between the various git repositories in the Zenoh project.

At first, install [Cargo and Rust](https://doc.rust-lang.org/cargo/getting-started/installation.html). If you already have the Rust toolchain installed, make sure it is up-to-date with:

```bash
$ rustup update
```

> :warning: **WARNING** :warning: : As Rust doesn't have a stable ABI, the backend library should be
built with the exact same Rust version than `zenohd`, and using for `zenoh` dependency the same version (or commit number) than 'zenohd'.
Otherwise, incompatibilities in memory mapping of shared types between `zenohd` and the library can lead to a `"SIGSEV"` crash.

To know the Rust version you're `zenohd` has been built with, use the `--version` option.

### Example with a downloaded version:
```bash
$ zenohd --version
The zenoh router v0.10.0-rc built with rustc 1.72.0 (5680fa18f 2023-08-23)
```
Here, `zenohd` is version `0.10.0-rc` has been built with the rustc version `1.72.0`.  
Install and use this same toolchain with the following command:

```bash
$ rustup default 1.72.0
```

And edit the update `Cargo.toml` file to make all the `zenoh` dependencies to use the same version number:
```toml
zenoh = { version = "0.10.0-rc", features = [ "unstable" ] }
```

Then build library only for the v1 backend, you have to specify it in the build command:
```bash
$ cargo build --release --all-targets -p zenoh-backend-influxdb-v1
```
You can build both the versions as well:
```bash
$ cargo build --release --all-targets
```

=======
### Example with a version built from sources:
```bash
$ zenohd --version
The zenoh router v0.11.0-dev-37-g9f7a37ee built with rustc 1.72.0 (5680fa18f 2023-08-23)
```
Here, `zenohd` version is `v0.11.0-dev-37-g9f7a37ee` where:
- `v0.11.0-dev` means it's a development version for the future `v0.11.0` release
- `-37`means there have been 37 commits since the tag `0.11.0-dev` marking the begining of developments for this future version
- `-g9f7a37ee` indicates the commit number of sources: `9f7a37ee` (the `g` prefix just meaning "git" shall be ignored)  

And it has been built with the rustc version `1.72.0`.  
Install and use this same toolchain with the following command:

```bash
$ rustup default 1.72.0
```

And update all the `zenoh` dependencies in `Cargo.lock` to use the commit id:
```bash
$ cargo update -p zenoh --precise 9f7a37ee
```

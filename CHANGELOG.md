### Prostore 5.3.0, 2022-01-14

#### New functionality
* The `UPSERT SELECT` LL-W function from ADB or ADQM logical entities into ADQM logical entities
* Logical materialized views in ADQM with datasources in ADB
* Sharding keys of any data type in ADQM logical entities
* Resuming broken LL-W operations
* The datamart changelog -- a system view in the service DB that records all successful and executing DDL operations of a datamart
* An automatic selection of datasource for MPP-R queries
* New SQL+ queries:
    * `GET_CHANGES` returns the datamart changelog
    * `DENY_CHANGES` enables a restriction on DDL operations of a datamart 
    * `ALLOW_CHANGES` disables a restriction on DDL operations of a datamart
    * `CHECK_MATERIALIZED_VIEW` returns the information about materialized views of a datamart
    * `GET_ENTITY_DDL` returns an SQL query to create a logical entity at its current state
* Advanced Kafka-Greenplum writer (PXF connector)
    * Disabled by default. To enable the advanced writer set the new configuration parameter `adb:mppw:usePxfConnector: ${ADB_MPPW_USE_ADVANCED_CONNECTOR:true|false}` to `true`

#### Fixes
* Duplicated parameters in a `CONFIG_SHOW` query
* ADQM primary keys with the `DATE` type
* LL-R:
    * `LIMIT` in subqueries
    * Some cases with the `IN` keyword
    * Constant comparison in `WHERE`
    * A mismatch between quantities of query parameters and template parameters
    * Parsing of a duplicate entity alias in a subquery
* A prepared statement using both dynamic and static data types
* Handling of view creation with the `DATASOURCE_TYPE` keyword
* Handling of materialized view creation with a reused name
* An initialization error occurring if ADP is the only DBMS in the storage
* An error in an `UPSERT SELECT` query without a list of target table columns
* A lack of a parsing error for a `SELECT` query with an `ORDER BY` clause specified after `DATASOURCE_TYPE`
* An error of getting a plugin status at `RESUME_WRITE_OPERATIONS` in ADQM
* Incorrect values being returned via `GET_WRITE_OPERATIONS` for an `UPSERT VALUES` query
* A sequential drop of two different datamarts with logical tables that are queried in a logical view
* Unawareness of an ADQM cluster topology in the query `CHECK_TABLE`
* Records of uploading delta being displayed via `SELECT` from an ADQM logical material view during synchronization
* An error in attributing temporal position of ADQM historical records after an `UPSERT VALUES` query

#### Changes
* Refactored ADQM MPP-W code
* Disabled cashing of LL-R with `FOR SYSTEM_TIME AS OF LATEST_UNCOMMITTED_DELTA`
* Disabled the check of an LL-R category in the case of the only DBMS in the storage
* Disabled entities selection from `INFORMATION_SCHEMA` for a `CREATE VIEW` query

#### Backward compatibility
* The `adqm:ddl:shardingKeyExpr: ${ADQM_SHARDING_EXPR:cityHash64|intAdd}` configuration parameter set to `intAdd` enables the old ADQM sharding algorithm with integer-valued keys


### Prostore 5.2.2, 2021-12-10

#### Fixes
* Fixed Calcite's `JOIN` rel to the sql conversion algorithm. It was making wrong decisions about column list replacement into Star.
* Fixed a left-side constant in query condition templates


### Prostore 5.2.1, 2021-11-16

#### Fixes

* ADP only configuration is now valid


### Prostore 5.2.0, 2021-10-26

#### New functionality
* New LL-W functions (`UPSERT VALUES`, `UPSERT SELECT`, `DELETE`)
* The auto-selection of LL-R datasource using a shard category
* The `COLLATE` keyword for the `WHERE` clause in ADG
* MPP-R queries for Logical Materialized Views
* The `CONFIG_SHOW` function to query current configuration
* The `GET_WRITE_OPERATIONS` function to show delta_hot write operations
* The `RESUME_WRITE_OPERATIONS` function to restart delta_hot unfinished write operations
* A custom pool of connections for ADB with configurable forced reconnect settings

#### Fixes
* The default application config (with `application.yml` replacing `application-default.yml`)
* Timestamp microsecond values for the `WHERE` clause of LL-R queries
* MPP-R `RIGHT JOIN` with null column-values in records
* The comparison with a Cyrillic string constant for the `WHERE` clause in the `SELECT` subquery of `CREATE VIEW`/`ALTER VIEW`
* The compatibility check of string data types and their length matching are delegated to the DBMS side

#### Changes
* Improved the performance of query parsing
* Restricted the DDL operations on `INFORMATION_SCHEMA`
* Removed `CORE_TIME_ZONE` from the configuration as obsolete
* Implemented a configurable setting `autoRestoreState`
* Implemented a configurable spring management port
* Refactored JDBC for the date, time and timestamp types algorithms
* Extended the failure logs for MPP-W
* Optimized performance of ADQM MPP-W
* Refactored the data model interface to ADQM
* The minimum requirement to use ADQM is now `Kafka-Clickhouse connector <3.5.5>`


### Prostore 5.1.0, 2021-08-27

#### New functionality
* Estimation of an enriched LL-R query by using the `ESTIMATE_ONLY` hint
* An ability to modify a logical schema without affecting a physical schema by using the `LOGICAL_ONLY` hint in DDL commands

#### Fixes
* Corrected `CHAR` and `UUID` logical datatypes names and length in `INFORMATION_SCHEMA`
* Fixed MPP-R using `LIMIT`
* Fixed an MPP-R enriched query for ADQM
* Fixed ADQM LL-R `JOIN`
* Fixed `CONFIG_STORAGE_ADD`
* Patched JDBC `getDate`, `getTime`, `getTimestamp` to use a specified calendar
* Fixed `CONFIG_STORAGE_ADD`
* Fixed `INFORMATION_SCHEMA.TABLES` displaying a logical table datasource after dropping the logical table with the said datasource
* Fixed the recognition of the select category "Undefined type"

#### Changes
* `CHECK_SUM` and CHECK_DATA` can use a normalization parameter for extra-large delta uploads
* Changed the `CHECK_SUM` and `CHECK_DATA` summation algorithm
* Included a commons-lang library into JDBC
* Enabled a STACKTRACE logging for blocked threads
* `CREATE TABLE` in ADQM, ADG, ADP no longer bypasses a check for a sharding key being a subset of a PK
* Changed the `CHECK_DATA` and the `CHECK_SUM` parsing to return more specific error messages
* Updated some error messages to be more informational


### Prostore 5.0.0, 2021-08-12

#### New functionality
* New datasource type ADP (PostgreSQL datasource)
	* Prerequisites: https://github.com/arenadata/kafka-postgres-connector
* Enabled MPP-R source specification within a respective query
* Added a new column `TABLE_DATASOURCE_TYPE` to `INFORMATION_SCHEMA.TABLES`
* Implemented subqueries in a `SELECT` clause: `SELECT * FROM tbl1 WHERE tbl1.id IN (subquery)`
#### Fixes

* Enabled more than 20 parameters for the `IN` keyword

#### Changes
* Enriched ADG error message logs (timeouts etc.)
* Changed names of the `INFORMATION_SCHEMA` sharding keys (`TABLE_CONSTRAINTS`, `KEY_COLUMN_USAGE`) to include a datamart name
* Refactored query enrichment for all of the Plugins (ADB, ADG, ADQM) to support the subqueries (see above)
* Fully reworked ADQM enrichment implementation
* Made component names of `CHECK_VERSIONS` to be consistent with product names


### Prostore 4.1.0, 2021-07-26

#### Changes

* `ROLLBACK DELTA` stops running MPP-W operations  
* Refactored the stopping mechanizm of MPP-W operations
* SQL+ DML SELECT valid syntax extended for `GROUP BY`, `ORDER BY`, `LIMIT`, `OFFSET` keywords combination
* Added support for functions within `JOIN` condition

### Prostore 4.0.1, 2021-07-20

Fixed start for a configuration without ADG.

### Prostore 4.0.0, 2021-07-12

#### New functionality

* New logical entity type - automatically synchronized logical materialized view \(ADB -> ADG\)

    * Prerequisites: [tarantool-pxf-connector version 1.0](https://github.com/arenadata/tarantool-pxf-connector/releases/tag/v1.0) and [adg version 0.3.5](https://github.com/arenadata/kafka-tarantool-loader/releases/tag/0.3.5) or higher
    * See the documentation for full information
    

#### Fixes

* Found and eliminated the intermittent ADB MPP-W failure “FDW server already exists”
* JDBC resultset meta-data patched for the `LINK`, `UUID` logical types
* Patched LL-R query with `count` and `limit` or `fetch next N rows only`
* Patched ADQM MPP-W for the tables with PK contains `Int32` type 

#### Changes

* An upgraded Vert.X 4.1, SQL client included
* Enriched logs with the unique operation identifier. The logging template parameter is `%vcl{requestId:-no_id}`
* Changed ADB LL-R queries to use Statement instead of PreparedStatement when no parameters supplied
* Added the explicit restriction for duplicate column names for logical entities
* Added a check of a timestamp string format on parsing stage for views and materialized views
* MPP-W failures log info extended
* Updated some error messages to be more informational 


### Prostore 3.7.3, 2021-06-30
#### Performance optimization
* Optimized ADB sql client connection parameters to maximize requests throughput.
* JDBC logging is off by default.
* Query-execution-core new configuration parameters:
    * `executorsCount`: $\{ADB\_EXECUTORS\_COUNT:20\}
    * `poolSize`: $\{ADB\_MAX\_POOL\_SIZE:5\}
    * `worker-pool`: $\{DTM\_CORE\_WORKER\_POOL\_SIZE:20\}
    * `event-loop-pool`: $\{DTM\_CORE\_EVENT\_LOOP\_POOL\_SIZE:20\}
* Removed Query-execution-core configuration parameter:
    * `maxSize`: $\{ADB\_MAX\_POOL\_SIZE:5\}

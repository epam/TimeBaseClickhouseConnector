# Timebase ClickHouse Connector

## Overview

Use our [open source ClickHouse connector](https://github.com/epam/TimeBaseClickhouseConnector) to replicate [TimeBase](https://kb.timebase.info/) streams to [ClickHouse](https://clickhouse.tech/) database.

[TimeBase Community Edition Repository](https://github.com/finos/TimeBase-CE)

TimeBase stores time series events as [Messages](https://kb.timebase.info/messages.html) and each type of event has a personal message class assigned to it in TimeBase. Message class has a set of fields (attributes) that characterize, describe, identify each specific type of event. In object-oriented programing languages messages can be seen as classes, each with a specific set of fields. Messages are stored in [Streams](https://kb.timebase.info/streams.html) chronologically by their timestamps for each symbol. Refer to a [Basic Concepts](https://kb.timebase.info/basic_concepts.html) page to learn more about TimeBase main principles and data structure.

To replicate TimeBase stream data to ClickHouse, we take objects and classes from a particular TimeBase stream and *unfold* them so each field corresponds to a particular ClickHouse table column. `Timestamp`, `Partition` and `Instrument` are auto generated and common for all ClickHouse tables where `Instrument` + `Timestamp` = `PrimaryKey`. ClickHouse tables are named after TimeBase stream names. Tables rows are created for each TimeBase message in a chronological order.

**The column naming convention:**

* column names for **fixed-type** objects' fields are named after the particular fields as is: for example `Symbol`.
* column names for **polymorphic** objects' fields follow this pattern: `object-name.field-name_field-data-type`.

## Data Type Mappings

* Refer to [ClickHouse Data Types](https://clickhouse.tech/docs/en/sql-reference/data-types/) to learn more about data types supported by ClickHouse DBMS.
* Refer to [Data Types](https://kb.timebase.info/data_types.html) to learn more about data types supported by TimeBase.
* Refer to [SchemaProcessor](https://github.com/epam/TimeBaseClickhouseConnector/blob/main/clickhouse-connector/src/main/java/deltix/timebase/connector/clickhouse/algos/SchemaProcessor.java) to view data mappings for the ClickHouse replicator. 


|TimeBase Type|ClickHouse Type|
|-------------|---------------|
|BOOLEAN|UInt8|
|CHAR/VARCHAR|String|
|TIMESTAMP|DateTime64|
|ENUM|Enum16|
|FLOAT/BINARY (32)|Float32|
|FLOAT/BINARY (64)|Float64|
|FLOAT/DECIMAL64|Decimal128|
|INTEGER/SIGNED (8)|Int8|
|INTEGER/SIGNED (16)|Int16|
|INTEGER/SIGNED (32)|Int32|
|INTEGER/SIGNED (64)|Int 64|
|TIMEOFDAY|Int32|

## Examples

Let's take a look at a **simplified** example. In this example we will show how a message with a polymorphic array is transformed into a ClickHouse table. 

Refer to [Example](https://github.com/epam/TimeBaseClickhouseConnector/tree/main/example) to view a step-by-step instruction on how to run this demo example and try the replication in action. 

```sql
/*TimeBase stream schema*/

DURABLE STREAM "clickhouse_stream" (
    CLASS "trade" (
        "price" '' FLOAT DECIMAL64,
        "size" '' FLOAT DECIMAL64
    );
    CLASS "bbo" (
        "askPrice" '' FLOAT DECIMAL64,
        "askSize" '' FLOAT DECIMAL64,
        "bidPrice" '' FLOAT DECIMAL64,
        "bidSize" '' FLOAT DECIMAL64
    );
    CLASS "Message" (
        "entries" ARRAY(OBJECT("trade", "bbo") NOT NULL)
    );
)
OPTIONS (POLYMORPHIC; PERIODICITY = 'IRREGULAR'; HIGHAVAILABILITY = FALSE)
```

The ClickHouse table will have the following structure. Here we see, that message fixed-type fields are named as `currencyCode_N_Int16 Nullable(Int16)`, whereas fields of a polymorphic object have `Array()` added `array.exchange_N_String Array(Nullable(String))`.

```sql
CREATE TABLE clickhouse.clickhouse_stream (
  `partition` Date,
  `timestamp` DateTime64(9),
  `instrument` String,
  `type` String,
  `currencyCode_N_Int16` Nullable(Int16),
  `originalTimestamp_N_DateTime64_3` Nullable(DateTime64(3)),
  `sequenceNumber_N_Int64` Nullable(Int64),
  `sourceId_N_String` Nullable(String),
  `packageType_PackageType` Enum16(
    'VENDOR_SNAPSHOT' = 0,
    'PERIODICAL_SNAPSHOT' = 1,
    'INCREMENTAL_UPDATE' = 2
  ),
  `array.type` Array(Nullable(String)),
  `array.price_N_Float64` Array(Nullable(Float64)),
  `array.size_N_Float64` Array(Nullable(Float64)),
  `array.askPrice_N_Float64` Array(Nullable(Float64)),
  `array.askSize_N_Float64` Array(Nullable(Float64)),
  `array.bidPrice_N_Float64` Array(Nullable(Float64)),
  `array.bidSize_N_Float64` Array(Nullable(Float64))
) ENGINE = MergeTree() PARTITION BY partition
ORDER BY
  (timestamp, instrument) SETTINGS index_granularity = 8192
```

## Failover Support 

In case `pollingIntervalMs` is configured, the replicator will periodically check the list of stream to be replicated and start replication in case is has been stopped for any reason. If this parameter is not configured, the replication process can be restarted manually. In both cases the next flow applies: 

1. Locate the `MAX` timestamp - a suspected crush/break point.<br>
```sql
SELECT MAX(EventTime) AS EventTime FROM table_name
```
2. Delete data with this timestamp.<br>
```sql
DELETE FROM table_name WHERE EventTime = max_time
```
3. Continue replication from the `MAX` timestamp.

## Deployment 

Refer to [Example](https://github.com/epam/TimeBaseClickhouseConnector/tree/main/example) to learn more about starting the replication.

## Configuration 

Supply and configure this [configuration file](https://github.com/epam/TimeBaseClickhouseConnector/blob/docs/clickhouse-connector/src/main/resources/application.yaml) with your replicator. 

```yaml
# configuration example

# ClickHouse parameters
clickhouse:
  url: jdbc:clickhouse://tbc-clickhouse:8123/default
  username: read
  password:
  database: tbc

# TimeBase parameters
timebase:
  url: dxtick://tbc-timebase:8011
  username:
  password:

# Replicator parameters
replication:
  pollingIntervalMs: 60_000
  flushMessageCount: 10_000
  flushTimeoutMs: 60_000
  streams: []
```

**ClickHouse parameters:**

* `url` - ClickHouse host name.
* `database` - ClickHouse database.
* `username` - login username.
* `password` - login password.

**TimeBase parameters:**

* `url` - TimeBase host.
* `username` - login username.
* `password` - login password.

**Replicator parameters:**

* `streams` - an array of TimeBase streams that will be replicated to ClickHouse. May include wildcards. 
* `pollingIntervalMs` - time interval in milliseconds to check for new streams to be replicated.
* `flushMessageCount` - data will be replicated in batch upon the achievement of the defined message count.
* `flushTimeoutMs` - in case `flushMessageCount` has not been reached data is replicated anyways after `flushTimeoutMs` time runs out.

## Known Limitations

The replicator does not currently support the replication of nested objects other than arrays. 


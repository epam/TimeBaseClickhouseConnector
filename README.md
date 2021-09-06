# TimebaseConnectors

## Overview

Use our [proprietary open source connector](https://github.com/epam/TimeBaseClickhouseConnector) to replicate TimeBase stream data to/from [ClickHouse](https://clickhouse.tech/) database.  

[ClickHouse](https://clickhouse.tech/docs/en/) is a [column-oriented database](https://en.wikipedia.org/wiki/Column-oriented_DBMS) management system (DBMS) for online analytical processing of queries (OLAP) that allows to generate analytical reports using SQL queries in real-time.

TimeBase stores time series events as [Messages](messages.html) and each type of event has a personal message class assigned to it in TimeBase. Message class has a set of fields (attributes) that characterize, describe, identify each specific type of event. In object-oriented programing languages messages can be seen as classes, each with a specific set of fields. Messages are stored in [Streams](streams.html) chronologically by their timestamps for each symbol. Refer to a [Basic Concepts](basic_concepts.html) page to learn more about TimeBase main principles and data structure.

To replicate TimeBase stream data to ClickHouse, we take objects and classes from a particular TimeBase stream and *unfold* them so each field corresponds to a particular ClickHouse table column. `Timestamp`, `Partition` and `Instrument` are auto generated and common for all ClickHouse tables where `Instrument` + `Timestamp` = `PrimaryKey`. ClickHouse tables are named after TimeBase stream names. Tables rows are created for each TimeBase message in a chronological order.

Let's take a look at a **simplified** example. In this example we will show how a message with fixed-type and polymorphic objects is transformed into a ClickHouse table. 

![](/clickhouse-connector/src/img/message_example.png)

For the example, we take a message with two fixed-type fields `Symbol` and `Timestamp`, and a polymorphic array `entries` with two types of entries (classes) `Trade` and `BBO`, each having a specific set of fields - shown on the illustration above.

Such data structure can be transformed to a ClickHouse table the following way: 

![](/clickhouse-connector/src/img/table1.png)

On the above table we see, that first data partition includes two entries of different types and the second just one. This way the output table has columns for each field from our source message, including objects' fields in the polymorphic array `entries`.

The column naming convention: 

* column names for **fixed-type** objects' fields are named after the particular fields as is: for example `Symbol`.
* column names for **polymorphic** objects' fields follow this pattern: `object-name.field-name_field-data-type`.

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

1. Run TimeBase<br>
```bash
# start TimeBase Community Edition
docker run --rm -d \
  -p 8011:8011 \
  --name=timebase-server \
  --ulimit nofile=65536:65536 \
  finos/timebase-ce-server:latest
```
2. Run replicator in [Docker](https://github.com/epam/TimeBaseClickhouseConnector/blob/main/clickhouse-connector/Dockerfile) or directly via `java -jar`


* Refer to [TimeBase Quick Start](quick-start.html) to learn more about starting TimeBase.
* Refer to [Replicator GitHub Repository](https://github.com/epam/TimeBaseClickhouseConnector/blob/main/clickhouse-connector/Dockerfile) to learn more about it's deployment. 


## Configuration 

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

## Data Type Mappings

* Refer to [ClickHouse Data Types](https://clickhouse.tech/docs/en/sql-reference/data-types/) to learn more about data types supported by ClickHouse DBMS.
* Refer to [Data Types](data_types.html) to learn more about data types supported by TimeBase.
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




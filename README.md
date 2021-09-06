# Timebase ClickHouse Connector

## Overview

Use our [proprietary open source connector](https://github.com/epam/TimeBaseClickhouseConnector) to replicate TimeBase stream data to/from [ClickHouse](https://clickhouse.tech/) database.  

[ClickHouse](https://clickhouse.tech/docs/en/) is a [column-oriented database](https://en.wikipedia.org/wiki/Column-oriented_DBMS) management system (DBMS) for online analytical processing of queries (OLAP) that allows to generate analytical reports using SQL queries in real-time.

TimeBase stores time series events as [Messages](https://kb.timebase.info/messages.html) and each type of event has a personal message class assigned to it in TimeBase. Message class has a set of fields (attributes) that characterize, describe, identify each specific type of event. In object-oriented programing languages messages can be seen as classes, each with a specific set of fields. Messages are stored in [Streams](https://kb.timebase.info/streams.html) chronologically by their timestamps for each symbol. Refer to a [Basic Concepts](https://kb.timebase.info/basic_concepts.html) page to learn more about TimeBase main principles and data structure.

To replicate TimeBase stream data to ClickHouse, we take objects and classes from a particular TimeBase stream and *unfold* them so each field corresponds to a particular ClickHouse table column. `Timestamp`, `Partition` and `Instrument` are auto generated and common for all ClickHouse tables where `Instrument` + `Timestamp` = `PrimaryKey`. ClickHouse tables are named after TimeBase stream names. Tables rows are created for each TimeBase message in a chronological order.

Let's take a look at a **simplified** example. In this example we will show how a message with fixed-type and polymorphic objects is transformed into a ClickHouse table. 

![](/clickhouse-connector/src/img/message_example.png)

For the example, we take a message with two fixed-type fields `Symbol` and `Timestamp`, and a polymorphic array `entries` with two types of entries (classes) `Trade` and `BBO`, each having a specific set of fields - shown on the illustration above.

Such data structure can be transformed to a ClickHouse table the following way: 
<!--
![](/clickhouse-connector/src/img/table1.png)
-->

|Timestamp|Symbol|entries.type|entries.price_float64|entries.size_float64|entries.AskPrice_float64|entries.AskSize_float64|entries.BidPrice_float64|entries.BidSize_float64|entries.exchange_string|
|---------|------|------------|---------------------|--------------------|------------------------|-----------------------|------------------------|-----------------------|-----------------------|
|2021-08-25T07:00:00.025Z|btcusd|entries.trade, entries.bbo|1|2|7|8|9|10|kraken,kraken|
|2021-08-25T07:00:00.026Z|btcusd|entries.bbo|||3|4|5|6|kraken|

On the above table we see, that first data partition includes two entries of different types and the second just one. This way the output table has columns for each field from our source message, including objects' fields in the polymorphic array `entries`.

The column naming convention: 

* column names for **fixed-type** objects' fields are named after the particular fields as is: for example `Symbol`.
* column names for **polymorphic** objects' fields follow this pattern: `object-name.field-name_field-data-type`.

We can describe a **more realistic** example now. Let's take a Kraken stream. 

```java
// Kraken stream schema. Here you can find all the included classes, enums and objects, their fields and data types.

DURABLE STREAM "kraken" (
    CLASS "deltix.timebase.api.messages.MarketMessage" 'Market Message' (
        "currencyCode" 'Currency Code' INTEGER SIGNED (16),
        "originalTimestamp" 'Original Timestamp' TIMESTAMP,
        "sequenceNumber" 'Sequence Number' INTEGER,
        "sourceId" 'Source Id' VARCHAR ALPHANUMERIC (10)
    );
    ENUM "deltix.timebase.api.messages.universal.PackageType" 'Package Type' (
        "VENDOR_SNAPSHOT" = 0,
        "PERIODICAL_SNAPSHOT" = 1,
        "INCREMENTAL_UPDATE" = 2
    );
    CLASS "deltix.timebase.api.messages.universal.PackageHeader" 'Package Header' 
    UNDER "deltix.timebase.api.messages.MarketMessage" (
        "packageType" 'Package Type' "deltix.timebase.api.messages.universal.PackageType" NOT NULL
    );
    CLASS "deltix.timebase.api.messages.universal.BaseEntry" 'Base Entry' (
        "contractId" 'Contract ID' VARCHAR ALPHANUMERIC (10),
        "exchangeId" 'Exchange Code' VARCHAR ALPHANUMERIC (10),
        "isImplied" 'Is Implied' BOOLEAN
    );
    CLASS "deltix.timebase.api.messages.universal.BasePriceEntry" 'Base Price Entry' 
    UNDER "deltix.timebase.api.messages.universal.BaseEntry" (
        "numberOfOrders" 'Number Of Orders' INTEGER,
        "participantId" 'Participant' VARCHAR,
        "price" 'Price' FLOAT DECIMAL64,
        "quoteId" 'Quote ID' VARCHAR,
        "size" 'Size' FLOAT DECIMAL64
    );
    ENUM "deltix.timebase.api.messages.QuoteSide" (
        "BID" = 0,
        "ASK" = 1
    );
    CLASS "deltix.timebase.api.messages.universal.L1Entry" 'L1Entry' 
    UNDER "deltix.timebase.api.messages.universal.BasePriceEntry" (
        "isNational" 'Is National' BOOLEAN,
        "side" 'Side' "deltix.timebase.api.messages.QuoteSide" NOT NULL
    );
    CLASS "deltix.timebase.api.messages.universal.L2EntryNew" 'L2EntryNew' 
    UNDER "deltix.timebase.api.messages.universal.BasePriceEntry" (
        "level" 'Level Index' INTEGER NOT NULL SIGNED (16),
        "side" 'Side' "deltix.timebase.api.messages.QuoteSide" NOT NULL
    );
    ENUM "deltix.timebase.api.messages.BookUpdateAction" 'Book Update Action' (
        "INSERT" = 0,
        "UPDATE" = 1,
        "DELETE" = 2
    );
    CLASS "deltix.timebase.api.messages.universal.L2EntryUpdate" 'L2EntryUpdate' 
    UNDER "deltix.timebase.api.messages.universal.BasePriceEntry" (
        "action" 'Action' "deltix.timebase.api.messages.BookUpdateAction" NOT NULL,
        "level" 'Level Index' INTEGER NOT NULL SIGNED (16),
        "side" 'Side' "deltix.timebase.api.messages.QuoteSide"
    );
    ENUM "deltix.timebase.api.messages.AggressorSide" 'Aggressor Side' (
        "BUY" = 0,
        "SELL" = 1
    );
    ENUM "deltix.timebase.api.messages.TradeType" (
        "REGULAR_TRADE" = 0,
        "AUCTION_CLEARING_PRICE" = 1,
        "CORRECTION" = 2,
        "CANCELLATION" = 3,
        "UNKNOWN" = 4
    );
    CLASS "deltix.timebase.api.messages.universal.TradeEntry" 'Trade Entry' 
    UNDER "deltix.timebase.api.messages.universal.BaseEntry" (
        "buyerNumberOfOrders" 'Buyer Number Of Orders' INTEGER,
        "buyerOrderId" 'Buyer Order ID' VARCHAR,
        "buyerParticipantId" 'Buyer Participant ID' VARCHAR,
        "condition" 'Condition' VARCHAR,
        "matchId" 'Match ID' VARCHAR,
        "price" 'Price' FLOAT DECIMAL64,
        "sellerNumberOfOrders" 'Seller Number Of Orders' INTEGER,
        "sellerOrderId" 'Seller Order ID' VARCHAR,
        "sellerParticipantId" 'Seller Participant ID' VARCHAR,
        "side" 'Side' "deltix.timebase.api.messages.AggressorSide",
        "size" 'Size' FLOAT DECIMAL64,
        "tradeType" 'Trade Type' "deltix.timebase.api.messages.TradeType"
    );
    CLASS "deltix.qsrv.hf.plugins.data.kraken.types.KrakenTradeEntry" 'Kraken Trade Entry' 
    UNDER "deltix.timebase.api.messages.universal.TradeEntry" (
        "orderType" 'Order Type' CHAR
    );
    ENUM "deltix.timebase.api.messages.DataModelType" (
        "LEVEL_ONE" = 0,
        "LEVEL_TWO" = 1,
        "LEVEL_THREE" = 2,
        "MAX" = 3
    );
    CLASS "deltix.timebase.api.messages.universal.BookResetEntry" 'Book Reset Entry' 
    UNDER "deltix.timebase.api.messages.universal.BaseEntry" (
        "modelType" 'Model Type' "deltix.timebase.api.messages.DataModelType" NOT NULL,
        "side" 'Side' "deltix.timebase.api.messages.QuoteSide"
    );
    CLASS "deltix.qsrv.hf.plugins.data.kraken.types.KrakenPackageHeader" 'Kraken Package Header' 
    UNDER "deltix.timebase.api.messages.universal.PackageHeader" (
        "entries" 'Entries' 
        ARRAY(OBJECT("deltix.timebase.api.messages.universal.L1Entry", "deltix.timebase.api.messages.universal.L2EntryNew", 
        "deltix.timebase.api.messages.universal.L2EntryUpdate", "deltix.qsrv.hf.plugins.data.kraken.types.KrakenTradeEntry", 
        "deltix.timebase.api.messages.universal.BookResetEntry") NOT NULL) NOT NULL
    );
    ENUM "deltix.timebase.api.messages.service.DataConnectorStatus" 'Data Connector Status' (
        "INITIAL" = 0,
        "CONNECTED_BY_USER" = 1,
        "AUTOMATICALLY_RESTORED" = 2,
        "DISCONNECTED_BY_USER" = 3,
        "DISCONNECTED_BY_COMPLETED_BATCH" = 4,
        "DISCONNECTED_BY_VENDOR_AND_RECONNECTING" = 5,
        "DISCONNECTED_BY_VENDOR_AND_HALTED" = 6,
        "DISCONNECTED_BY_ERROR_AND_RECONNECTING" = 7,
        "DISCONNECTED_BY_ERROR_AND_HALTED" = 8,
        "RECOVERING_BEGIN" = 9,
        "LIVE_BEGIN" = 10
    );
    CLASS "deltix.timebase.api.messages.service.ConnectionStatusChangeMessage" 'Connection Status Change Message' (
        "cause" 'Cause' VARCHAR,
        "status" 'Status' "deltix.timebase.api.messages.service.DataConnectorStatus"
    );
    ENUM "deltix.timebase.api.messages.status.SecurityStatus" (
        "FEED_CONNECTED" = 0,
        "FEED_DISCONNECTED" = 1,
        "TRADING_STARTED" = 2,
        "TRADING_STOPPED" = 3
    );
    CLASS "deltix.timebase.api.messages.status.SecurityStatusMessage" 'Security Status Change Message' 
    UNDER "deltix.timebase.api.messages.MarketMessage" (
        "cause" 'Cause' VARCHAR,
        "exchangeId" 'Exchange Code' VARCHAR ALPHANUMERIC (10)
        "originalStatus" 'Original Status' VARCHAR,
        "status" 'Status' "deltix.timebase.api.messages.status.SecurityStatus"
    );
)
OPTIONS (POLYMORPHIC; PERIODICITY = 'IRREGULAR'; HIGHAVAILABILITY = FALSE)
```

The ClickHouse table will have the following structure. Here we see, that message fixed-type fields are named as `currencyCode_N_Int16 Nullable(Int16)`, whereas fields of a polymorphic object have `array` added `entries.exchangeId_N_String Array(Nullable(String))`.

```sql
CREATE TABLE clickhouse.kraken (
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
  `entries.type` Array(Nullable(String)),
  `entries.contractId_N_String` Array(Nullable(String)),
  `entries.exchangeId_N_String` Array(Nullable(String)),
  `entries.isImplied_N_UInt8` Array(Nullable(UInt8)),
  `entries.numberOfOrders_N_Int64` Array(Nullable(Int64)),
  `entries.participantId_N_String` Array(Nullable(String)),
  `entries.price_N_Decimal128_12` Array(Nullable(Decimal(38, 12))),
  `entries.quoteId_N_String` Array(Nullable(String)),
  `entries.size_N_Decimal128_12` Array(Nullable(Decimal(38, 12))),
  `entries.isNational_N_UInt8` Array(Nullable(UInt8)),
  `entries.side_QuoteSide` Array(Enum16('BID' = 0, 'ASK' = 1)),
  `entries.level_Int16` Array(Int16),
  `entries.action_BookUpdateAction` Array(Enum16('INSERT' = 0, 'UPDATE' = 1, 'DELETE' = 2)),
  `entries.side_N_QuoteSide` Array(Nullable(Enum16('BID' = 0, 'ASK' = 1))),
  `entries.buyerNumberOfOrders_N_Int64` Array(Nullable(Int64)),
  `entries.buyerOrderId_N_String` Array(Nullable(String)),
  `entries.buyerParticipantId_N_String` Array(Nullable(String)),
  `entries.condition_N_String` Array(Nullable(String)),
  `entries.matchId_N_String` Array(Nullable(String)),
  `entries.sellerNumberOfOrders_N_Int64` Array(Nullable(Int64)),
  `entries.sellerOrderId_N_String` Array(Nullable(String)),
  `entries.sellerParticipantId_N_String` Array(Nullable(String)),
  `entries.side_N_AggressorSide` Array(Nullable(Enum16('BUY' = 0, 'SELL' = 1))),
  `entries.tradeType_N_TradeType` Array(
    Nullable(
      Enum16(
        'REGULAR_TRADE' = 0,
        'AUCTION_CLEARING_PRICE' = 1,
        'CORRECTION' = 2,
        'CANCELLATION' = 3,
        'UNKNOWN' = 4
      )
    )
  ),
  `entries.orderType_N_String` Array(Nullable(String)),
  `entries.modelType_DataModelType` Array(
    Enum16(
      'LEVEL_ONE' = 0,
      'LEVEL_TWO' = 1,
      'LEVEL_THREE' = 2,
      'MAX' = 3
    )
  ),
  `cause_N_String` Nullable(String),
  `status_N_DataConnectorStatus` Nullable(
    Enum16(
      'INITIAL' = 0,
      'CONNECTED_BY_USER' = 1,
      'AUTOMATICALLY_RESTORED' = 2,
      'DISCONNECTED_BY_USER' = 3,
      'DISCONNECTED_BY_COMPLETED_BATCH' = 4,
      'DISCONNECTED_BY_VENDOR_AND_RECONNECTING' = 5,
      'DISCONNECTED_BY_VENDOR_AND_HALTED' = 6,
      'DISCONNECTED_BY_ERROR_AND_RECONNECTING' = 7,
      'DISCONNECTED_BY_ERROR_AND_HALTED' = 8,
      'RECOVERING_BEGIN' = 9,
      'LIVE_BEGIN' = 10
    )
  ),
  `exchangeId_N_String` Nullable(String),
  `originalStatus_N_String` Nullable(String),
  `status_N_SecurityStatus` Nullable(
    Enum16(
      'FEED_CONNECTED' = 0,
      'FEED_DISCONNECTED' = 1,
      'TRADING_STARTED' = 2,
      'TRADING_STOPPED' = 3
    )
  )
) ENGINE = MergeTree() PARTITION BY partition
ORDER BY
  (timestamp, instrument) SETTINGS index_granularity = 8192
```

This table can be displayed in the TimeBase integration with [Tabix](https://tabix.io/) (business intelligence application and SQL editor for ClickHouse) as follows: 

![](/clickhouse-connector/src/img/tabix.png)

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


* Refer to [TimeBase Quick Start](https://kb.timebase.info/quick-start.html) to learn more about starting TimeBase.
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




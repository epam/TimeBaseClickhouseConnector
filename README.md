# TimebaseConnectors

## Overview

Use our [proprietary open source connector](https://github.com/epam/TimeBaseClickhouseConnector) to replicate TimeBase stream data to/from [ClickHouse](https://clickhouse.tech/) database.  

[ClickHouse](https://clickhouse.tech/docs/en/) is a [column-oriented database](https://en.wikipedia.org/wiki/Column-oriented_DBMS) management system (DBMS) for online analytical processing of queries (OLAP) that allows to generate analytical reports using SQL queries in real-time.

TimeBase stores time series events as [Messages](messages.html) and each type of event has a personal message class assigned to it in TimeBase. Message class has a set of fields (attributes) that characterize, describe, identify each specific type of event. In object-oriented programing languages messages can be seen as classes, each with a specific set of fields. Messages are stored in [Streams](streams.html) chronologically by their timestamps for each symbol. Refer to a [Basic Concepts](basic_concepts.html) page to learn more about TimeBase main principles and data structure.

To replicate TimeBase stream data to ClickHouse, we take objects and classes from a particular TimeBase stream and *unfold* them so each field corresponds to a particular ClickHouse table column. `Timestamp`, `Partition` and `Instrument` are auto generated and common for all ClickHouse tables where `Instrument` + `Timestamp` = `PrimaryKey`. ClickHouse tables are named after TimeBase stream names. Tables rows are created for each TimeBase message in a chronological order.

Let's take a look at a **simplified** example. In this example we will show how a message with fixed-type and polymorphic objects is transformed into a ClickHouse table. 

<a href="/images/message_example.png" data-lightbox="image-1" data-title="Message Example" ><img src="/images/message_example.png" class="zoom" style="max-width: 60% !important;"/></a>

For the example, we take a message with two fixed-type fields `Symbol` and `Timestamp`, and a polymorphic array `entries` with two types of entries (classes) `Trade` and `BBO`, each having a specific set of fields - shown on the illustration above. 


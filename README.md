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

<div class="language-java highlighter-rouge"><div class="highlight"><pre class="highlight" style="max-height: 600px; overflow: auto; margin-bottom: 20px;"><code>
<span class="c1">// Kraken stream schema. Here you can find all the included classes, enums and objects, their fields and data types.</span>

<span class="no">DURABLE</span> <span class="no">STREAM</span> <span class="s">"kraken"</span> <span class="o">(</span>
    <span class="no">CLASS</span> <span class="s">"deltix.timebase.api.messages.MarketMessage"</span> <span class="err">'</span><span class="nc">Market</span> <span class="nc">Message</span><span class="err">'</span> <span class="o">(</span>
        <span class="s">"currencyCode"</span> <span class="err">'</span><span class="nc">Currency</span> <span class="nc">Code</span><span class="err">'</span> <span class="no">INTEGER</span> <span class="nf">SIGNED</span> <span class="o">(</span><span class="mi">16</span><span class="o">),</span>
        <span class="s">"originalTimestamp"</span> <span class="err">'</span><span class="nc">Original</span> <span class="nc">Timestamp</span><span class="err">'</span> <span class="no">TIMESTAMP</span><span class="o">,</span>
        <span class="s">"sequenceNumber"</span> <span class="err">'</span><span class="nc">Sequence</span> <span class="nc">Number</span><span class="err">'</span> <span class="no">INTEGER</span><span class="o">,</span>
        <span class="s">"sourceId"</span> <span class="err">'</span><span class="nc">Source</span> <span class="nc">Id</span><span class="err">'</span> <span class="no">VARCHAR</span> <span class="nf">ALPHANUMERIC</span> <span class="o">(</span><span class="mi">10</span><span class="o">)</span>
    <span class="o">);</span>
    <span class="no">ENUM</span> <span class="s">"deltix.timebase.api.messages.universal.PackageType"</span> <span class="err">'</span><span class="nc">Package</span> <span class="nc">Type</span><span class="err">'</span> <span class="o">(</span>
        <span class="s">"VENDOR_SNAPSHOT"</span> <span class="o">=</span> <span class="mi">0</span><span class="o">,</span>
        <span class="s">"PERIODICAL_SNAPSHOT"</span> <span class="o">=</span> <span class="mi">1</span><span class="o">,</span>
        <span class="s">"INCREMENTAL_UPDATE"</span> <span class="o">=</span> <span class="mi">2</span>
    <span class="o">);</span>
    <span class="no">CLASS</span> <span class="s">"deltix.timebase.api.messages.universal.PackageHeader"</span> <span class="err">'</span><span class="nc">Package</span> <span class="nc">Header</span><span class="err">'</span> 
    <span class="no">UNDER</span> <span class="s">"deltix.timebase.api.messages.MarketMessage"</span> <span class="o">(</span>
        <span class="s">"packageType"</span> <span class="err">'</span><span class="nc">Package</span> <span class="nc">Type</span><span class="err">'</span> <span class="s">"deltix.timebase.api.messages.universal.PackageType"</span> <span class="no">NOT</span> <span class="no">NULL</span>
    <span class="o">);</span>
    <span class="no">CLASS</span> <span class="s">"deltix.timebase.api.messages.universal.BaseEntry"</span> <span class="err">'</span><span class="nc">Base</span> <span class="nc">Entry</span><span class="err">'</span> <span class="o">(</span>
        <span class="s">"contractId"</span> <span class="err">'</span><span class="nc">Contract</span> <span class="no">ID</span><span class="err">'</span> <span class="no">VARCHAR</span> <span class="nf">ALPHANUMERIC</span> <span class="o">(</span><span class="mi">10</span><span class="o">),</span>
        <span class="s">"exchangeId"</span> <span class="err">'</span><span class="nc">Exchange</span> <span class="nc">Code</span><span class="err">'</span> <span class="no">VARCHAR</span> <span class="nf">ALPHANUMERIC</span> <span class="o">(</span><span class="mi">10</span><span class="o">),</span>
        <span class="s">"isImplied"</span> <span class="err">'</span><span class="nc">Is</span> <span class="nc">Implied</span><span class="err">'</span> <span class="no">BOOLEAN</span>
    <span class="o">);</span>
    <span class="no">CLASS</span> <span class="s">"deltix.timebase.api.messages.universal.BasePriceEntry"</span> <span class="err">'</span><span class="nc">Base</span> <span class="nc">Price</span> <span class="nc">Entry</span><span class="err">'</span> 
    <span class="no">UNDER</span> <span class="s">"deltix.timebase.api.messages.universal.BaseEntry"</span> <span class="o">(</span>
        <span class="s">"numberOfOrders"</span> <span class="err">'</span><span class="nc">Number</span> <span class="nc">Of</span> <span class="nc">Orders</span><span class="err">'</span> <span class="no">INTEGER</span><span class="o">,</span>
        <span class="s">"participantId"</span> <span class="err">'</span><span class="nc">Participant</span><span class="err">'</span> <span class="no">VARCHAR</span><span class="o">,</span>
        <span class="s">"price"</span> <span class="err">'</span><span class="nc">Price</span><span class="err">'</span> <span class="no">FLOAT</span> <span class="no">DECIMAL64</span><span class="o">,</span>
        <span class="s">"quoteId"</span> <span class="err">'</span><span class="nc">Quote</span> <span class="no">ID</span><span class="err">'</span> <span class="no">VARCHAR</span><span class="o">,</span>
        <span class="s">"size"</span> <span class="err">'</span><span class="nc">Size</span><span class="err">'</span> <span class="no">FLOAT</span> <span class="no">DECIMAL64</span>
    <span class="o">);</span>
    <span class="no">ENUM</span> <span class="s">"deltix.timebase.api.messages.QuoteSide"</span> <span class="o">(</span>
        <span class="s">"BID"</span> <span class="o">=</span> <span class="mi">0</span><span class="o">,</span>
        <span class="s">"ASK"</span> <span class="o">=</span> <span class="mi">1</span>
    <span class="o">);</span>
    <span class="no">CLASS</span> <span class="s">"deltix.timebase.api.messages.universal.L1Entry"</span> <span class="err">'</span><span class="nc">L1Entry</span><span class="err">'</span> 
    <span class="no">UNDER</span> <span class="s">"deltix.timebase.api.messages.universal.BasePriceEntry"</span> <span class="o">(</span>
        <span class="s">"isNational"</span> <span class="err">'</span><span class="nc">Is</span> <span class="nc">National</span><span class="err">'</span> <span class="no">BOOLEAN</span><span class="o">,</span>
        <span class="s">"side"</span> <span class="err">'</span><span class="nc">Side</span><span class="err">'</span> <span class="s">"deltix.timebase.api.messages.QuoteSide"</span> <span class="no">NOT</span> <span class="no">NULL</span>
    <span class="o">);</span>
    <span class="no">CLASS</span> <span class="s">"deltix.timebase.api.messages.universal.L2EntryNew"</span> <span class="err">'</span><span class="nc">L2EntryNew</span><span class="err">'</span> 
    <span class="no">UNDER</span> <span class="s">"deltix.timebase.api.messages.universal.BasePriceEntry"</span> <span class="o">(</span>
        <span class="s">"level"</span> <span class="err">'</span><span class="nc">Level</span> <span class="nc">Index</span><span class="err">'</span> <span class="no">INTEGER</span> <span class="no">NOT</span> <span class="no">NULL</span> <span class="nf">SIGNED</span> <span class="o">(</span><span class="mi">16</span><span class="o">),</span>
        <span class="s">"side"</span> <span class="err">'</span><span class="nc">Side</span><span class="err">'</span> <span class="s">"deltix.timebase.api.messages.QuoteSide"</span> <span class="no">NOT</span> <span class="no">NULL</span>
    <span class="o">);</span>
    <span class="no">ENUM</span> <span class="s">"deltix.timebase.api.messages.BookUpdateAction"</span> <span class="err">'</span><span class="nc">Book</span> <span class="nc">Update</span> <span class="nc">Action</span><span class="err">'</span> <span class="o">(</span>
        <span class="s">"INSERT"</span> <span class="o">=</span> <span class="mi">0</span><span class="o">,</span>
        <span class="s">"UPDATE"</span> <span class="o">=</span> <span class="mi">1</span><span class="o">,</span>
        <span class="s">"DELETE"</span> <span class="o">=</span> <span class="mi">2</span>
    <span class="o">);</span>
    <span class="no">CLASS</span> <span class="s">"deltix.timebase.api.messages.universal.L2EntryUpdate"</span> <span class="err">'</span><span class="nc">L2EntryUpdate</span><span class="err">'</span> 
    <span class="no">UNDER</span> <span class="s">"deltix.timebase.api.messages.universal.BasePriceEntry"</span> <span class="o">(</span>
        <span class="s">"action"</span> <span class="err">'</span><span class="nc">Action</span><span class="err">'</span> <span class="s">"deltix.timebase.api.messages.BookUpdateAction"</span> <span class="no">NOT</span> <span class="no">NULL</span><span class="o">,</span>
        <span class="s">"level"</span> <span class="err">'</span><span class="nc">Level</span> <span class="nc">Index</span><span class="err">'</span> <span class="no">INTEGER</span> <span class="no">NOT</span> <span class="no">NULL</span> <span class="nf">SIGNED</span> <span class="o">(</span><span class="mi">16</span><span class="o">),</span>
        <span class="s">"side"</span> <span class="err">'</span><span class="nc">Side</span><span class="err">'</span> <span class="s">"deltix.timebase.api.messages.QuoteSide"</span>
    <span class="o">);</span>
    <span class="no">ENUM</span> <span class="s">"deltix.timebase.api.messages.AggressorSide"</span> <span class="err">'</span><span class="nc">Aggressor</span> <span class="nc">Side</span><span class="err">'</span> <span class="o">(</span>
        <span class="s">"BUY"</span> <span class="o">=</span> <span class="mi">0</span><span class="o">,</span>
        <span class="s">"SELL"</span> <span class="o">=</span> <span class="mi">1</span>
    <span class="o">);</span>
    <span class="no">ENUM</span> <span class="s">"deltix.timebase.api.messages.TradeType"</span> <span class="o">(</span>
        <span class="s">"REGULAR_TRADE"</span> <span class="o">=</span> <span class="mi">0</span><span class="o">,</span>
        <span class="s">"AUCTION_CLEARING_PRICE"</span> <span class="o">=</span> <span class="mi">1</span><span class="o">,</span>
        <span class="s">"CORRECTION"</span> <span class="o">=</span> <span class="mi">2</span><span class="o">,</span>
        <span class="s">"CANCELLATION"</span> <span class="o">=</span> <span class="mi">3</span><span class="o">,</span>
        <span class="s">"UNKNOWN"</span> <span class="o">=</span> <span class="mi">4</span>
    <span class="o">);</span>
    <span class="no">CLASS</span> <span class="s">"deltix.timebase.api.messages.universal.TradeEntry"</span> <span class="err">'</span><span class="nc">Trade</span> <span class="nc">Entry</span><span class="err">'</span> 
    <span class="no">UNDER</span> <span class="s">"deltix.timebase.api.messages.universal.BaseEntry"</span> <span class="o">(</span>
        <span class="s">"buyerNumberOfOrders"</span> <span class="err">'</span><span class="nc">Buyer</span> <span class="nc">Number</span> <span class="nc">Of</span> <span class="nc">Orders</span><span class="err">'</span> <span class="no">INTEGER</span><span class="o">,</span>
        <span class="s">"buyerOrderId"</span> <span class="err">'</span><span class="nc">Buyer</span> <span class="nc">Order</span> <span class="no">ID</span><span class="err">'</span> <span class="no">VARCHAR</span><span class="o">,</span>
        <span class="s">"buyerParticipantId"</span> <span class="err">'</span><span class="nc">Buyer</span> <span class="nc">Participant</span> <span class="no">ID</span><span class="err">'</span> <span class="no">VARCHAR</span><span class="o">,</span>
        <span class="s">"condition"</span> <span class="err">'</span><span class="nc">Condition</span><span class="err">'</span> <span class="no">VARCHAR</span><span class="o">,</span>
        <span class="s">"matchId"</span> <span class="err">'</span><span class="nc">Match</span> <span class="no">ID</span><span class="err">'</span> <span class="no">VARCHAR</span><span class="o">,</span>
        <span class="s">"price"</span> <span class="err">'</span><span class="nc">Price</span><span class="err">'</span> <span class="no">FLOAT</span> <span class="no">DECIMAL64</span><span class="o">,</span>
        <span class="s">"sellerNumberOfOrders"</span> <span class="err">'</span><span class="nc">Seller</span> <span class="nc">Number</span> <span class="nc">Of</span> <span class="nc">Orders</span><span class="err">'</span> <span class="no">INTEGER</span><span class="o">,</span>
        <span class="s">"sellerOrderId"</span> <span class="err">'</span><span class="nc">Seller</span> <span class="nc">Order</span> <span class="no">ID</span><span class="err">'</span> <span class="no">VARCHAR</span><span class="o">,</span>
        <span class="s">"sellerParticipantId"</span> <span class="err">'</span><span class="nc">Seller</span> <span class="nc">Participant</span> <span class="no">ID</span><span class="err">'</span> <span class="no">VARCHAR</span><span class="o">,</span>
        <span class="s">"side"</span> <span class="err">'</span><span class="nc">Side</span><span class="err">'</span> <span class="s">"deltix.timebase.api.messages.AggressorSide"</span><span class="o">,</span>
        <span class="s">"size"</span> <span class="err">'</span><span class="nc">Size</span><span class="err">'</span> <span class="no">FLOAT</span> <span class="no">DECIMAL64</span><span class="o">,</span>
        <span class="s">"tradeType"</span> <span class="err">'</span><span class="nc">Trade</span> <span class="nc">Type</span><span class="err">'</span> <span class="s">"deltix.timebase.api.messages.TradeType"</span>
    <span class="o">);</span>
    <span class="no">CLASS</span> <span class="s">"deltix.qsrv.hf.plugins.data.kraken.types.KrakenTradeEntry"</span> <span class="err">'</span><span class="nc">Kraken</span> <span class="nc">Trade</span> <span class="nc">Entry</span><span class="err">'</span> 
    <span class="no">UNDER</span> <span class="s">"deltix.timebase.api.messages.universal.TradeEntry"</span> <span class="o">(</span>
        <span class="s">"orderType"</span> <span class="err">'</span><span class="nc">Order</span> <span class="nc">Type</span><span class="err">'</span> <span class="no">CHAR</span>
    <span class="o">);</span>
    <span class="no">ENUM</span> <span class="s">"deltix.timebase.api.messages.DataModelType"</span> <span class="o">(</span>
        <span class="s">"LEVEL_ONE"</span> <span class="o">=</span> <span class="mi">0</span><span class="o">,</span>
        <span class="s">"LEVEL_TWO"</span> <span class="o">=</span> <span class="mi">1</span><span class="o">,</span>
        <span class="s">"LEVEL_THREE"</span> <span class="o">=</span> <span class="mi">2</span><span class="o">,</span>
        <span class="s">"MAX"</span> <span class="o">=</span> <span class="mi">3</span>
    <span class="o">);</span>
    <span class="no">CLASS</span> <span class="s">"deltix.timebase.api.messages.universal.BookResetEntry"</span> <span class="err">'</span><span class="nc">Book</span> <span class="nc">Reset</span> <span class="nc">Entry</span><span class="err">'</span> 
    <span class="no">UNDER</span> <span class="s">"deltix.timebase.api.messages.universal.BaseEntry"</span> <span class="o">(</span>
        <span class="s">"modelType"</span> <span class="err">'</span><span class="nc">Model</span> <span class="nc">Type</span><span class="err">'</span> <span class="s">"deltix.timebase.api.messages.DataModelType"</span> <span class="no">NOT</span> <span class="no">NULL</span><span class="o">,</span>
        <span class="s">"side"</span> <span class="err">'</span><span class="nc">Side</span><span class="err">'</span> <span class="s">"deltix.timebase.api.messages.QuoteSide"</span>
    <span class="o">);</span>
    <span class="no">CLASS</span> <span class="s">"deltix.qsrv.hf.plugins.data.kraken.types.KrakenPackageHeader"</span> <span class="err">'</span><span class="nc">Kraken</span> <span class="nc">Package</span> <span class="nc">Header</span><span class="err">'</span> 
    <span class="no">UNDER</span> <span class="s">"deltix.timebase.api.messages.universal.PackageHeader"</span> <span class="o">(</span>
        <span class="s">"entries"</span> <span class="err">'</span><span class="nc">Entries</span><span class="err">'</span> 
        <span class="no">ARRAY</span><span class="o">(</span><span class="no">OBJECT</span><span class="o">(</span><span class="s">"deltix.timebase.api.messages.universal.L1Entry"</span><span class="o">,</span> <span class="s">"deltix.timebase.api.messages.universal.L2EntryNew"</span><span class="o">,</span> 
        <span class="s">"deltix.timebase.api.messages.universal.L2EntryUpdate"</span><span class="o">,</span> <span class="s">"deltix.qsrv.hf.plugins.data.kraken.types.KrakenTradeEntry"</span><span class="o">,</span> 
        <span class="s">"deltix.timebase.api.messages.universal.BookResetEntry"</span><span class="o">)</span> <span class="no">NOT</span> <span class="no">NULL</span><span class="o">)</span> <span class="no">NOT</span> <span class="no">NULL</span>
    <span class="o">);</span>
    <span class="no">ENUM</span> <span class="s">"deltix.timebase.api.messages.service.DataConnectorStatus"</span> <span class="err">'</span><span class="nc">Data</span> <span class="nc">Connector</span> <span class="nc">Status</span><span class="err">'</span> <span class="o">(</span>
        <span class="s">"INITIAL"</span> <span class="o">=</span> <span class="mi">0</span><span class="o">,</span>
        <span class="s">"CONNECTED_BY_USER"</span> <span class="o">=</span> <span class="mi">1</span><span class="o">,</span>
        <span class="s">"AUTOMATICALLY_RESTORED"</span> <span class="o">=</span> <span class="mi">2</span><span class="o">,</span>
        <span class="s">"DISCONNECTED_BY_USER"</span> <span class="o">=</span> <span class="mi">3</span><span class="o">,</span>
        <span class="s">"DISCONNECTED_BY_COMPLETED_BATCH"</span> <span class="o">=</span> <span class="mi">4</span><span class="o">,</span>
        <span class="s">"DISCONNECTED_BY_VENDOR_AND_RECONNECTING"</span> <span class="o">=</span> <span class="mi">5</span><span class="o">,</span>
        <span class="s">"DISCONNECTED_BY_VENDOR_AND_HALTED"</span> <span class="o">=</span> <span class="mi">6</span><span class="o">,</span>
        <span class="s">"DISCONNECTED_BY_ERROR_AND_RECONNECTING"</span> <span class="o">=</span> <span class="mi">7</span><span class="o">,</span>
        <span class="s">"DISCONNECTED_BY_ERROR_AND_HALTED"</span> <span class="o">=</span> <span class="mi">8</span><span class="o">,</span>
        <span class="s">"RECOVERING_BEGIN"</span> <span class="o">=</span> <span class="mi">9</span><span class="o">,</span>
        <span class="s">"LIVE_BEGIN"</span> <span class="o">=</span> <span class="mi">10</span>
    <span class="o">);</span>
    <span class="no">CLASS</span> <span class="s">"deltix.timebase.api.messages.service.ConnectionStatusChangeMessage"</span> <span class="err">'</span><span class="nc">Connection</span> <span class="nc">Status</span> <span class="nc">Change</span> <span class="nc">Message</span><span class="err">'</span> <span class="o">(</span>
        <span class="s">"cause"</span> <span class="err">'</span><span class="nc">Cause</span><span class="err">'</span> <span class="no">VARCHAR</span><span class="o">,</span>
        <span class="s">"status"</span> <span class="err">'</span><span class="nc">Status</span><span class="err">'</span> <span class="s">"deltix.timebase.api.messages.service.DataConnectorStatus"</span>
    <span class="o">);</span>
    <span class="no">ENUM</span> <span class="s">"deltix.timebase.api.messages.status.SecurityStatus"</span> <span class="o">(</span>
        <span class="s">"FEED_CONNECTED"</span> <span class="o">=</span> <span class="mi">0</span><span class="o">,</span>
        <span class="s">"FEED_DISCONNECTED"</span> <span class="o">=</span> <span class="mi">1</span><span class="o">,</span>
        <span class="s">"TRADING_STARTED"</span> <span class="o">=</span> <span class="mi">2</span><span class="o">,</span>
        <span class="s">"TRADING_STOPPED"</span> <span class="o">=</span> <span class="mi">3</span>
    <span class="o">);</span>
    <span class="no">CLASS</span> <span class="s">"deltix.timebase.api.messages.status.SecurityStatusMessage"</span> <span class="err">'</span><span class="nc">Security</span> <span class="nc">Status</span> <span class="nc">Change</span> <span class="nc">Message</span><span class="err">'</span> 
    <span class="no">UNDER</span> <span class="s">"deltix.timebase.api.messages.MarketMessage"</span> <span class="o">(</span>
        <span class="s">"cause"</span> <span class="err">'</span><span class="nc">Cause</span><span class="err">'</span> <span class="no">VARCHAR</span><span class="o">,</span>
        <span class="s">"exchangeId"</span> <span class="err">'</span><span class="nc">Exchange</span> <span class="nc">Code</span><span class="err">'</span> <span class="no">VARCHAR</span> <span class="nf">ALPHANUMERIC</span> <span class="o">(</span><span class="mi">10</span><span class="o">)</span>
        <span class="s">"originalStatus"</span> <span class="err">'</span><span class="nc">Original</span> <span class="nc">Status</span><span class="err">'</span> <span class="no">VARCHAR</span><span class="o">,</span>
        <span class="s">"status"</span> <span class="err">'</span><span class="nc">Status</span><span class="err">'</span> <span class="s">"deltix.timebase.api.messages.status.SecurityStatus"</span>
    <span class="o">);</span>
<span class="o">)</span>
<span class="no">OPTIONS</span> <span class="o">(</span><span class="no">POLYMORPHIC</span><span class="o">;</span> <span class="no">PERIODICITY</span> <span class="o">=</span> <span class="err">'</span><span class="no">IRREGULAR</span><span class="err">'</span><span class="o">;</span> <span class="no">HIGHAVAILABILITY</span> <span class="o">=</span> <span class="no">FALSE</span><span class="o">)</span>
</code></pre></div></div>

The ClickHouse table will have the following structure. Here we see, that message fixed-type fields are named as `currencyCode_N_Int16 Nullable(Int16)`, whereas fields of a polymorphic object have `array` added `entries.exchangeId_N_String Array(Nullable(String))`.

<div class="language-sql highlighter-rouge"><div class="highlight"><pre class="highlight" style="max-height: 600px; overflow: auto; margin-bottom: 20px;"><code><span class="k">CREATE</span> <span class="k">TABLE</span> <span class="n">clickhouse</span><span class="p">.</span><span class="n">kraken</span> <span class="p">(</span>
  <span class="nv">`partition`</span> <span class="nb">Date</span><span class="p">,</span>
  <span class="nv">`timestamp`</span> <span class="n">DateTime64</span><span class="p">(</span><span class="mi">9</span><span class="p">),</span>
  <span class="nv">`instrument`</span> <span class="n">String</span><span class="p">,</span>
  <span class="nv">`type`</span> <span class="n">String</span><span class="p">,</span>
  <span class="nv">`currencyCode_N_Int16`</span> <span class="k">Nullable</span><span class="p">(</span><span class="n">Int16</span><span class="p">),</span>
  <span class="nv">`originalTimestamp_N_DateTime64_3`</span> <span class="k">Nullable</span><span class="p">(</span><span class="n">DateTime64</span><span class="p">(</span><span class="mi">3</span><span class="p">)),</span>
  <span class="nv">`sequenceNumber_N_Int64`</span> <span class="k">Nullable</span><span class="p">(</span><span class="n">Int64</span><span class="p">),</span>
  <span class="nv">`sourceId_N_String`</span> <span class="k">Nullable</span><span class="p">(</span><span class="n">String</span><span class="p">),</span>
  <span class="nv">`packageType_PackageType`</span> <span class="n">Enum16</span><span class="p">(</span>
    <span class="s1">'VENDOR_SNAPSHOT'</span> <span class="o">=</span> <span class="mi">0</span><span class="p">,</span>
    <span class="s1">'PERIODICAL_SNAPSHOT'</span> <span class="o">=</span> <span class="mi">1</span><span class="p">,</span>
    <span class="s1">'INCREMENTAL_UPDATE'</span> <span class="o">=</span> <span class="mi">2</span>
  <span class="p">),</span>
  <span class="nv">`entries.type`</span> <span class="n">Array</span><span class="p">(</span><span class="k">Nullable</span><span class="p">(</span><span class="n">String</span><span class="p">)),</span>
  <span class="nv">`entries.contractId_N_String`</span> <span class="n">Array</span><span class="p">(</span><span class="k">Nullable</span><span class="p">(</span><span class="n">String</span><span class="p">)),</span>
  <span class="nv">`entries.exchangeId_N_String`</span> <span class="n">Array</span><span class="p">(</span><span class="k">Nullable</span><span class="p">(</span><span class="n">String</span><span class="p">)),</span>
  <span class="nv">`entries.isImplied_N_UInt8`</span> <span class="n">Array</span><span class="p">(</span><span class="k">Nullable</span><span class="p">(</span><span class="n">UInt8</span><span class="p">)),</span>
  <span class="nv">`entries.numberOfOrders_N_Int64`</span> <span class="n">Array</span><span class="p">(</span><span class="k">Nullable</span><span class="p">(</span><span class="n">Int64</span><span class="p">)),</span>
  <span class="nv">`entries.participantId_N_String`</span> <span class="n">Array</span><span class="p">(</span><span class="k">Nullable</span><span class="p">(</span><span class="n">String</span><span class="p">)),</span>
  <span class="nv">`entries.price_N_Decimal128_12`</span> <span class="n">Array</span><span class="p">(</span><span class="k">Nullable</span><span class="p">(</span><span class="nb">Decimal</span><span class="p">(</span><span class="mi">38</span><span class="p">,</span> <span class="mi">12</span><span class="p">))),</span>
  <span class="nv">`entries.quoteId_N_String`</span> <span class="n">Array</span><span class="p">(</span><span class="k">Nullable</span><span class="p">(</span><span class="n">String</span><span class="p">)),</span>
  <span class="nv">`entries.size_N_Decimal128_12`</span> <span class="n">Array</span><span class="p">(</span><span class="k">Nullable</span><span class="p">(</span><span class="nb">Decimal</span><span class="p">(</span><span class="mi">38</span><span class="p">,</span> <span class="mi">12</span><span class="p">))),</span>
  <span class="nv">`entries.isNational_N_UInt8`</span> <span class="n">Array</span><span class="p">(</span><span class="k">Nullable</span><span class="p">(</span><span class="n">UInt8</span><span class="p">)),</span>
  <span class="nv">`entries.side_QuoteSide`</span> <span class="n">Array</span><span class="p">(</span><span class="n">Enum16</span><span class="p">(</span><span class="s1">'BID'</span> <span class="o">=</span> <span class="mi">0</span><span class="p">,</span> <span class="s1">'ASK'</span> <span class="o">=</span> <span class="mi">1</span><span class="p">)),</span>
  <span class="nv">`entries.level_Int16`</span> <span class="n">Array</span><span class="p">(</span><span class="n">Int16</span><span class="p">),</span>
  <span class="nv">`entries.action_BookUpdateAction`</span> <span class="n">Array</span><span class="p">(</span><span class="n">Enum16</span><span class="p">(</span><span class="s1">'INSERT'</span> <span class="o">=</span> <span class="mi">0</span><span class="p">,</span> <span class="s1">'UPDATE'</span> <span class="o">=</span> <span class="mi">1</span><span class="p">,</span> <span class="s1">'DELETE'</span> <span class="o">=</span> <span class="mi">2</span><span class="p">)),</span>
  <span class="nv">`entries.side_N_QuoteSide`</span> <span class="n">Array</span><span class="p">(</span><span class="k">Nullable</span><span class="p">(</span><span class="n">Enum16</span><span class="p">(</span><span class="s1">'BID'</span> <span class="o">=</span> <span class="mi">0</span><span class="p">,</span> <span class="s1">'ASK'</span> <span class="o">=</span> <span class="mi">1</span><span class="p">))),</span>
  <span class="nv">`entries.buyerNumberOfOrders_N_Int64`</span> <span class="n">Array</span><span class="p">(</span><span class="k">Nullable</span><span class="p">(</span><span class="n">Int64</span><span class="p">)),</span>
  <span class="nv">`entries.buyerOrderId_N_String`</span> <span class="n">Array</span><span class="p">(</span><span class="k">Nullable</span><span class="p">(</span><span class="n">String</span><span class="p">)),</span>
  <span class="nv">`entries.buyerParticipantId_N_String`</span> <span class="n">Array</span><span class="p">(</span><span class="k">Nullable</span><span class="p">(</span><span class="n">String</span><span class="p">)),</span>
  <span class="nv">`entries.condition_N_String`</span> <span class="n">Array</span><span class="p">(</span><span class="k">Nullable</span><span class="p">(</span><span class="n">String</span><span class="p">)),</span>
  <span class="nv">`entries.matchId_N_String`</span> <span class="n">Array</span><span class="p">(</span><span class="k">Nullable</span><span class="p">(</span><span class="n">String</span><span class="p">)),</span>
  <span class="nv">`entries.sellerNumberOfOrders_N_Int64`</span> <span class="n">Array</span><span class="p">(</span><span class="k">Nullable</span><span class="p">(</span><span class="n">Int64</span><span class="p">)),</span>
  <span class="nv">`entries.sellerOrderId_N_String`</span> <span class="n">Array</span><span class="p">(</span><span class="k">Nullable</span><span class="p">(</span><span class="n">String</span><span class="p">)),</span>
  <span class="nv">`entries.sellerParticipantId_N_String`</span> <span class="n">Array</span><span class="p">(</span><span class="k">Nullable</span><span class="p">(</span><span class="n">String</span><span class="p">)),</span>
  <span class="nv">`entries.side_N_AggressorSide`</span> <span class="n">Array</span><span class="p">(</span><span class="k">Nullable</span><span class="p">(</span><span class="n">Enum16</span><span class="p">(</span><span class="s1">'BUY'</span> <span class="o">=</span> <span class="mi">0</span><span class="p">,</span> <span class="s1">'SELL'</span> <span class="o">=</span> <span class="mi">1</span><span class="p">))),</span>
  <span class="nv">`entries.tradeType_N_TradeType`</span> <span class="n">Array</span><span class="p">(</span>
    <span class="k">Nullable</span><span class="p">(</span>
      <span class="n">Enum16</span><span class="p">(</span>
        <span class="s1">'REGULAR_TRADE'</span> <span class="o">=</span> <span class="mi">0</span><span class="p">,</span>
        <span class="s1">'AUCTION_CLEARING_PRICE'</span> <span class="o">=</span> <span class="mi">1</span><span class="p">,</span>
        <span class="s1">'CORRECTION'</span> <span class="o">=</span> <span class="mi">2</span><span class="p">,</span>
        <span class="s1">'CANCELLATION'</span> <span class="o">=</span> <span class="mi">3</span><span class="p">,</span>
        <span class="s1">'UNKNOWN'</span> <span class="o">=</span> <span class="mi">4</span>
      <span class="p">)</span>
    <span class="p">)</span>
  <span class="p">),</span>
  <span class="nv">`entries.orderType_N_String`</span> <span class="n">Array</span><span class="p">(</span><span class="k">Nullable</span><span class="p">(</span><span class="n">String</span><span class="p">)),</span>
  <span class="nv">`entries.modelType_DataModelType`</span> <span class="n">Array</span><span class="p">(</span>
    <span class="n">Enum16</span><span class="p">(</span>
      <span class="s1">'LEVEL_ONE'</span> <span class="o">=</span> <span class="mi">0</span><span class="p">,</span>
      <span class="s1">'LEVEL_TWO'</span> <span class="o">=</span> <span class="mi">1</span><span class="p">,</span>
      <span class="s1">'LEVEL_THREE'</span> <span class="o">=</span> <span class="mi">2</span><span class="p">,</span>
      <span class="s1">'MAX'</span> <span class="o">=</span> <span class="mi">3</span>
    <span class="p">)</span>
  <span class="p">),</span>
  <span class="nv">`cause_N_String`</span> <span class="k">Nullable</span><span class="p">(</span><span class="n">String</span><span class="p">),</span>
  <span class="nv">`status_N_DataConnectorStatus`</span> <span class="k">Nullable</span><span class="p">(</span>
    <span class="n">Enum16</span><span class="p">(</span>
      <span class="s1">'INITIAL'</span> <span class="o">=</span> <span class="mi">0</span><span class="p">,</span>
      <span class="s1">'CONNECTED_BY_USER'</span> <span class="o">=</span> <span class="mi">1</span><span class="p">,</span>
      <span class="s1">'AUTOMATICALLY_RESTORED'</span> <span class="o">=</span> <span class="mi">2</span><span class="p">,</span>
      <span class="s1">'DISCONNECTED_BY_USER'</span> <span class="o">=</span> <span class="mi">3</span><span class="p">,</span>
      <span class="s1">'DISCONNECTED_BY_COMPLETED_BATCH'</span> <span class="o">=</span> <span class="mi">4</span><span class="p">,</span>
      <span class="s1">'DISCONNECTED_BY_VENDOR_AND_RECONNECTING'</span> <span class="o">=</span> <span class="mi">5</span><span class="p">,</span>
      <span class="s1">'DISCONNECTED_BY_VENDOR_AND_HALTED'</span> <span class="o">=</span> <span class="mi">6</span><span class="p">,</span>
      <span class="s1">'DISCONNECTED_BY_ERROR_AND_RECONNECTING'</span> <span class="o">=</span> <span class="mi">7</span><span class="p">,</span>
      <span class="s1">'DISCONNECTED_BY_ERROR_AND_HALTED'</span> <span class="o">=</span> <span class="mi">8</span><span class="p">,</span>
      <span class="s1">'RECOVERING_BEGIN'</span> <span class="o">=</span> <span class="mi">9</span><span class="p">,</span>
      <span class="s1">'LIVE_BEGIN'</span> <span class="o">=</span> <span class="mi">10</span>
    <span class="p">)</span>
  <span class="p">),</span>
  <span class="nv">`exchangeId_N_String`</span> <span class="k">Nullable</span><span class="p">(</span><span class="n">String</span><span class="p">),</span>
  <span class="nv">`originalStatus_N_String`</span> <span class="k">Nullable</span><span class="p">(</span><span class="n">String</span><span class="p">),</span>
  <span class="nv">`status_N_SecurityStatus`</span> <span class="k">Nullable</span><span class="p">(</span>
    <span class="n">Enum16</span><span class="p">(</span>
      <span class="s1">'FEED_CONNECTED'</span> <span class="o">=</span> <span class="mi">0</span><span class="p">,</span>
      <span class="s1">'FEED_DISCONNECTED'</span> <span class="o">=</span> <span class="mi">1</span><span class="p">,</span>
      <span class="s1">'TRADING_STARTED'</span> <span class="o">=</span> <span class="mi">2</span><span class="p">,</span>
      <span class="s1">'TRADING_STOPPED'</span> <span class="o">=</span> <span class="mi">3</span>
    <span class="p">)</span>
  <span class="p">)</span>
<span class="p">)</span> <span class="n">ENGINE</span> <span class="o">=</span> <span class="n">MergeTree</span><span class="p">()</span> <span class="n">PARTITION</span> <span class="k">BY</span> <span class="n">partition</span>
<span class="k">ORDER</span> <span class="k">BY</span>
  <span class="p">(</span><span class="nb">timestamp</span><span class="p">,</span> <span class="n">instrument</span><span class="p">)</span> <span class="n">SETTINGS</span> <span class="n">index_granularity</span> <span class="o">=</span> <span class="mi">8192</span>
</code></pre></div></div>

This table can be displayed in the TimeBase integration with [Tabix](https://tabix.io/) (business intelligence application and SQL editor for ClickHouse) as follows: 

<a href="/images/tabix.png" data-lightbox="image-2" data-title="ClickHouse Table" ><img src="/images/tabix.png" class="zoom" style="max-width: 60% !important;"/></a>

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




Example of configuration file: <br>
```yaml
clickhouse:
  url: jdbc:clickhouse://localhost:8123/default
  username: default
  password:
  database: tbc

timebase:
  url: dxtick://localhost:8011
  username:
  password:

replication:
  pollingIntervalMs: 60_000
  flushMessageCount: 10_000
  flushTimeoutMs: 60_000
  columnNamingScheme: NAME # determines how the column names will be generated for all streams and query.
#  NAME
#  NAME_AND_DATATYPE by default
#  TYPE_AND_NAME   
  includePartitionColumn: true # false - not to use partition column. Determinate for all replications. True by default
  queries: # list of query
    -
      query: | # query on qql, use pipe for multiline query
        select entry.price as 'price', entry.size as 'size' TYPE "deltix.timebase.api.messages.universal.L1Entry"
        from bitfinex
        array join entries[this is L1Entry] as entry

        union
        select entry.price as 'price', entry.size as 'size' TYPE "deltix.timebase.api.messages.universal.L2EntryNew"
        from bitfinex
        array join entries[this is L2EntryNew] as entry
      key: entries # replication unique identifier 
      splitByTypes: false    # false by default.
        # false loads all messages into one table
      # true split messages by table base on typeTableMapping or used default mapping
      typeTableMapping:     # map for mapping by default use simple class name
        # if present explicitly overrides the value of splitByTypes to 'true'
        deltix.timebase.api.messages.universal.L1Entry: el1
        deltix.timebase.api.messages.universal.L2EntryNew: el2
      #        by default use simple class name
      #        deltix.timebase.api.messages.universal.L1Entry: L1Entry
      #        deltix.timebase.api.messages.universal.L2EntryNew: L1Entry
      writeMode: APPEND   # APPEND by default
        # APPEND starts replication from the last found timestamp from target table(s)
      # REWRITE recreates all target tables
      columnNamingScheme: TYPE_AND_NAME # overwrites columnNamingScheme for a specific stream or query.
      includePartitionColumn: false # overwrites includePartitionColumn for a specific stream or query
      table: getFromMapping  # set target table if use single table mapping 
    -
      query: |
        select entry.price as 'price', entry.size as 'size' TYPE "deltix.timebase.api.messages.universal.L1Entry"
        from bitfinex
        array join entries[this is L1Entry] as entry

        union
        select entry.price as 'price', entry.size as 'size' TYPE "deltix.timebase.api.messages.universal.L2EntryNew"
        from bitfinex
        array join entries[this is L2EntryNew] as entry
      key: entriesSingleTable
    -
      query: |
        select entry.price as 'price', entry.size as 'size' TYPE "deltix.timebase.api.messages.universal.L2EntryNew"
        from bitfinex
        array join entries[this is L2EntryNew] as entry
      key: l2entry
    -
      query: select * from trade
      key: qTrade
    -
      query: select * from bar
      splitByTypes: true
      key: qBarSplit
  streams:
    -
      stream: trade #   timebase stream key use as default table name on single table mapping
      key: entrySingleTable # replication unique identifier
      # the rest of the settings are identical to the request
      splitByTypes: false
      typeTableMapping:
        deltix.timebase.api.messages.BestBidOfferMessage: BBO
        deltix.timebase.api.messages.TradeMessage: TM
      writeMode: APPEND
      # create two tables BBO and TM
    -
      stream: trade
      key: stradeOut
      table: tradeStreamTableName  # set target table if use single table mapping 
      # create table tradeStreamTableName
    -
      stream: trade
      key: stradeOut2
      splitByTypes: true
      # create two tables BestBidOfferMessage and TradeMessage
    -
      stream: bitfinex
      key: bitfinex-live
```
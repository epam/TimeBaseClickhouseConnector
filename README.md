## Timebase ClickHouse Connector     


### Overview

[ClickHouse](https://clickhouse.tech/docs/en/) is a [column-oriented database](https://en.wikipedia.org/wiki/Column-oriented_DBMS) management system (DBMS) for online analytical processing of queries (OLAP) that allows to generate analytical reports using SQL queries in real-time.

TimeBase stores time series events as [Messages](https://kb.timebase.info/community/overview/messages) and each type of event has a personal message class assigned to it in TimeBase. Message class has a set of fields (attributes) that characterize, describe, identify each specific type of event. In object-oriented programing languages messages can be seen as classes, each with a specific set of fields. Messages are stored in [Streams](https://kb.timebase.info/community/overview/streams) chronologically by their timestamps for each symbol. Refer to a [Basic Concepts](https://kb.timebase.info/community/overview/basic_concepts) page to learn more about TimeBase main principles and data structure.

To replicate TimeBase stream data to ClickHouse, we take objects and classes from a particular TimeBase stream and *unfold* them so each field corresponds to a particular ClickHouse table column. `Timestamp`, `Partition` and `Instrument` are auto generated and common for all ClickHouse tables where `Instrument` + `Timestamp` = `PrimaryKey`. ClickHouse tables are named after TimeBase stream names. Tables rows are created for each TimeBase message in a chronological order.

### Features

  - Supports replicating queries and streams.
  - Support custom tables with subset of columns
  - Only strick columns data types mapping  
  - Failover support, is case of disconnects or unforces errors. 
  
### How to Build

You will require the Java 11+ and git-lfs to build.

Build the project with [Gradle](http://gradle.org/) using this [build.gradle](/build.gradle) file.

Full clean and build of all modules

```shell
    $ ./gradlew clean build
```

To buld Docker image use
```shell
    $ ./gradlew :java:clickhouse-connector:dockerBuildImage
```

### How to Run

```shell
    $ ./gradlew :java:clickhouse-connector:bootRun -Dspring.config.additional-location=<path to configuration file>	
```

Configuration template: [config](/configTemplate.md)

Sample configuration for docker-compose: [Sample](./Samples/Docker/docker-compose)



## License

Copyright © 2023 EPAM Systems, Inc.

Distributed under the [Apache License, Version 2.0](http://www.apache.org/licenses/LICENSE-2.0).

SPDX-License-Identifier: [Apache-2.0](https://spdx.org/licenses/Apache-2.0)
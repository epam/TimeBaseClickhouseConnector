# Run This Example to Test Replicator

This example serves **only** for demonstration purposes to show the replication process in action.

**Prerequisites**

* Docker
* Java 11+

**1. Launch ClickHouse**
  * Get ClickHouse Docker image<br>
  ```bash
  docker pull yandex/clickhouse-server
  ```
  * Run ClickHouse<br>
  ```bash
  docker run -d --name clickhouse-server --ulimit nofile=262144:262144 yandex/clickhouse-server
  ```
**2. Launch TimeBase**
  * Get and run the TimeBase Community Edition<br>
  ```bash
  docker pull finos/timebase-ce-server
 
  docker run -d --name tbserver -p 8011:8011 finos/timebase-ce-server
  ```
**3. Start TimeBase Shell CLI**<br>
  ```bash
  docker exec -it tbserver /timebase-server/bin/tickdb.sh
  
  ==> set db dxtick://localhost:8011

  ==> open
  ```
**4. Create a clickhouse_stream Stream in TimeBase**<br>
```bash

==> ??
create DURABLE STREAM "clickhouse_stream" (
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
/
```
**5. Write Into a clickhouse_stream Stream**
```bash
==> set stream clickhouse_stream
==> send [{"type":"Message","symbol":"btcusd","timestamp":"2021-09-06T23:08:45.790Z","entries":[{"type":"trade","price":"333.1","size":"444.2"}]}]
```
**6. Run Replicator**
  * **In a new console window**, go to the ClickHouse replicator directory<br>
  ```bash
  cd TimeBaseClickHouseConnector
  ```
  * Build the ClickHouse replicator<br>
  ```bash
  gradlew clean build
  ```
  * Start the ClickHouse replicator. Refer to README to learn more about the available [configuration parameters](https://github.com/epam/TimeBaseClickhouseConnector).<br>
  ```bash
  TBD
  ```
**7. View Stream in ClickHouse**
  * TBD
  ```bash
  TBD
  ```

---------------------------------------------------

**To Run Replicator in Docker**

TBD


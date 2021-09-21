# Run This Example to Test Replicator

This example serves **only** for demonstration purposes to show the replication process in action.

**Prerequisites**

* Docker
* Java 11+

**1. Launch ClickHouse**
  * Get a ClickHouse Docker image<br>
  ```bash
  docker pull yandex/clickhouse-server
  ```
  * Run ClickHouse<br>
  ```bash
  docker run --name tbc-clickhouse --network host -e CLICKHOUSE_DB=tbc -e CLICKHOUSE_USER=read -e CLICKHOUSE_DEFAULT_ACCESS_MANAGEMENT=1 -e CLICKHOUSE_PASSWORD=password -p 9000:9000 -p 8123:8123 yandex/clickhouse-server
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
**5. Write Into a clickhouse_stream Stream**<br>
```bash
==> set stream clickhouse_stream
==> send [{"type":"Message","symbol":"btcusd","timestamp":"2021-09-06T23:08:45.790Z","entries":[{"type":"trade","price":"333.1","size":"444.2"}]}]
```
**6. Run Replicator**

* **In a new console window**, go to the ClickHouse replicator directory
```bash
cd TimeBaseClickHouseConnector
```
* Build the ClickHouse replicator.<br>
```bash
gradlew clean build
```
* Start the ClickHouse replicator.<br>
```bash
java -jar -Dclickhouse.password=password -Dreplication.streams=clickhouse_stream -Dclickhouse.url=jdbc:clickhouse://localhost:8123/default -Dtimebase.url=dxtick://localhost:8011 clickhouse-connector-1.0.20-SNAPSHOT.jar
```
**7. View Stream in ClickHouse**

* In any REST client run<br>
```bash
GET http://localhost:8123/?user=read&password=password&database=tbc&query=select * from clickhouse_stream
```
Refer to [ClickHouse](https://clickhouse.com/docs/ru/interfaces/http/) docs to learn more. 

---------------------------------------------------

**To Run Replicator in Docker**

1. Build project and create a Docker image.<br>
```bash
./gradlew clean build

./gradlew buildDockerImage
```
2. Create a replicator container.<br>
```bash
docker create --name tbc -e JAVA_OPTS="-Dclickhouse.password=password -Dreplication.streams=clickhouse_stream  -Dtimebase.url=dxtick://tbserver:8011" null/deltix.docker/timebaseconnectors/clickhouse-connector:1.0
```
3. Create a network between ClickHouse, TimeBase and Replicator.<br>
```bash
docker network create tbcnet
docker network connect tbcnet tbc-clickhouse
docker network connect tbcnet tbserver
docker network connect tbcnet tbc
```
4. Run the replicator.<br>
```bash
docker start tbc
```

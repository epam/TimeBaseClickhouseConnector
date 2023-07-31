/*
 * Copyright 2023 EPAM Systems, Inc
 *
 * See the NOTICE file distributed with this work for additional information
 * regarding copyright ownership. Licensed under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.epam.deltix.timebase.connector.clickhouse;

import com.clickhouse.client.config.ClickHouseDefaults;
import com.clickhouse.jdbc.ClickHouseDataSource;
import com.epam.deltix.timebase.connector.clickhouse.configuration.properties.ClickhouseProperties;
import com.epam.deltix.timebase.connector.clickhouse.configuration.properties.ReplicationProperties;
import com.epam.deltix.timebase.connector.clickhouse.containers.DockerClickHouseContainer;
import com.epam.deltix.timebase.connector.clickhouse.containers.DockerTimebaseContainer;
import com.epam.deltix.timebase.connector.clickhouse.timebase.BestBidOfferTestMessage;
import com.epam.deltix.timebase.connector.clickhouse.timebase.TradeTestMessage;
import com.epam.deltix.timebase.connector.clickhouse.util.Util;
import de.cronn.reflection.util.PropertyUtils;
import de.cronn.reflection.util.TypedPropertyGetter;
import com.epam.deltix.clickhouse.ClickhouseClient;
import com.epam.deltix.clickhouse.models.TableIdentity;
import com.epam.deltix.clickhouse.schema.ColumnDeclaration;
import com.epam.deltix.clickhouse.schema.TableDeclaration;
import com.epam.deltix.clickhouse.schema.types.DataTypes;
import com.epam.deltix.clickhouse.schema.types.NullableDataType;
import com.epam.deltix.clickhouse.schema.types.SqlDataType;
import com.epam.deltix.clickhouse.selector.SelectBuilder;
import com.epam.deltix.clickhouse.util.SelectQueryHelper;
import com.epam.deltix.gflog.api.Log;
import com.epam.deltix.gflog.api.LogFactory;
import com.epam.deltix.gflog.api.LogLevel;
import com.epam.deltix.timebase.messages.*;
import com.epam.deltix.qsrv.hf.pub.codec.FieldLayout;
import com.epam.deltix.qsrv.hf.pub.codec.RecordLayout;
import com.epam.deltix.qsrv.hf.pub.md.*;
import com.epam.deltix.qsrv.hf.tickdb.pub.*;
import com.epam.deltix.timebase.connector.clickhouse.algos.ColumnDeclarationEx;
import com.epam.deltix.timebase.connector.clickhouse.algos.SchemaProcessor;
import com.epam.deltix.timebase.connector.clickhouse.model.ColumnNamingScheme;
import com.epam.deltix.timebase.connector.clickhouse.model.StreamRequest;
import com.epam.deltix.timebase.connector.clickhouse.services.ReplicatorService;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Triple;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.springframework.jdbc.core.RowMapper;
import org.testcontainers.containers.ClickHouseContainer;
import org.testcontainers.containers.GenericContainer;

import javax.sql.DataSource;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class ConcurrentStreamReplicatorTests {
    protected static final Log LOG = LogFactory.getLog(ConcurrentStreamReplicatorTests.class);

    private final static String TB_SYMBOL_VALUE = "BTCUSD";
    private final static String CLICKHOUSE_DATABASE_NAME = "tbMessages";

    private final static String TIMEBASE_URL_SYSTEM_VAR_NAME = "TIMEBASE_URL";
    private final static String CLICKHOUSE_URL_SYSTEM_VAR_NAME = "CLICKHOUSE_URL";
    private final static String CLICKHOUSE_USER_SYSTEM_VAR_NAME = "CLICKHOUSE_USER";
    private final static long BASE_TIMESTAMP = Instant.now().toEpochMilli();

    private static final int MESSAGE_COUNT = 1_000_000;

    // required environment variables values
    // TIMEBASE_URL=dxtick://10.10.81.26:8011;
    // optional environment variables
    // CLICKHOUSE_URL=jdbc:clickhouse://10.10.81.55:8623/default;CLICKHOUSE_USER=read
    private ClickhouseClient clickhouseClient;
    private ClickhouseProperties clickhouseProperties;
    private DXTickDB tickDB;

    protected static RowMapper<Map<String, Object>> getRowMapper(final List<ColumnDeclaration> columns) {
        final Map<String, ColumnDeclaration> columnDeclarations =
                columns.stream().collect(Collectors.toMap(ColumnDeclaration::getDbColumnName, Function.identity()));

        return getRowMapper(columnDeclarations);
    }

    protected static RowMapper<Map<String, Object>> getRowMapper(final Map<String, ColumnDeclaration> columnDeclarations) {
        return (rs, rowNum) -> {
            Map<String, Object> result = new HashMap<>();

            for (Map.Entry<String, ColumnDeclaration> entry : columnDeclarations.entrySet()) {
                String key = entry.getKey();
                ColumnDeclaration value = entry.getValue();
                Object exprValue = getValue(value.getDbDataType(), rs, key);

                result.put(key, exprValue);
            }

            return result;
        };
    }

    protected static Object getValue(final SqlDataType type,
                                     final ResultSet rs, final String columnLabel) throws SQLException {
        if (type instanceof NullableDataType)
            return getValue(((NullableDataType) type).getNestedType(), rs, columnLabel);
        else
            return getValue(type.getType(), rs, columnLabel);
    }

    // helpers

    protected static Object getValue(final DataTypes type, final ResultSet rs, final String columnLabel) throws SQLException {
        switch (type) {
            case UINT8:
                return rs.getObject(columnLabel) != null ? rs.getBoolean(columnLabel) : null;
            case INT8:
                return rs.getObject(columnLabel) != null ? rs.getByte(columnLabel) : null;
            case INT16:
                return rs.getObject(columnLabel) != null ? rs.getShort(columnLabel) : null;
            case DATE_TIME64:
                return rs.getString(columnLabel);

            default:
                return rs.getObject(columnLabel);
        }
    }

    protected static DXTickStream createStream(final DXTickDB tickDb, final String streamKey, final Class<?> streamType) {
        Introspector emptyIntrospector = Introspector.createEmptyMessageIntrospector();

        StreamOptions streamOptions = new StreamOptions();
        streamOptions.distributionFactor = StreamOptions.MAX_DISTRIBUTION;
        streamOptions.version = "5.0";
        streamOptions.setPolymorphic(getRecordClassDescriptors(emptyIntrospector, streamType).toArray(new RecordClassDescriptor[0]));

        return tickDb.createStream(streamKey, streamOptions);
    }

    protected static ArrayList<RecordClassDescriptor> getRecordClassDescriptors(final Introspector emptyIntrospector, final Class<?>... streamTypes) {
        ArrayList<RecordClassDescriptor> descriptors = new ArrayList<>();
        for (Class<?> type : streamTypes) {
            if (InstrumentMessage.class.isAssignableFrom(type)) {
                RecordClassDescriptor classDescriptor;
                try {
                    classDescriptor = emptyIntrospector.introspectRecordClass(type);
                    descriptors.add(classDescriptor);
                } catch (Introspector.IntrospectionException e) {
                    LOG.log(LogLevel.ERROR).append(e.getMessage()).append(e).commit();
                }
            }
        }
        return descriptors;
    }

    protected static String getSelectAllQuery(final TableIdentity tableIdentity, String orderBy) {
        String tableIdentitySqlDef = getTableIdentitySqlDefinition(tableIdentity);
        return orderBy != null
                ? String.format("SELECT * FROM %s ORDER BY %s", tableIdentitySqlDef, orderBy)
                : String.format("SELECT * FROM %s ORDER BY seq", tableIdentitySqlDef);
    }

    protected static String getTableIdentitySqlDefinition(final TableIdentity tableIdentity) {
        return StringUtils.isNotBlank(tableIdentity.getDatabaseName()) ?
                String.format("%s.%s", tableIdentity.getDatabaseName(), tableIdentity.getTableName()) : tableIdentity.getTableName();
    }

    protected static void initSystemRequiredFields(final InstrumentMessage message) {
        message.setSymbol(TB_SYMBOL_VALUE);
    }

    protected static void systemRequiredFieldsCheck(final InstrumentMessage replicatedMessage, final Map<String, Object> actualValues) {
        String expectedMessageTypeValue = replicatedMessage.getClass().getName();

        String actualSymbolValue = actualValues.get(SchemaProcessor.INSTRUMENT_COLUMN_NAME).toString();
        String actualMessageTypeValue = actualValues.get(SchemaProcessor.TYPE_COLUMN_NAME).toString();

        assertAll(
                () -> assertEquals(TB_SYMBOL_VALUE, actualSymbolValue),
                () -> assertEquals(expectedMessageTypeValue, actualMessageTypeValue)
        );
    }

    protected static <T> ColumnDeclaration getClickhouseColumn(final DXTickStream stream, final Class<T> beanClass,
                                                               final TypedPropertyGetter<T, ?> propertyGetter) {
        String timebasePropertyName = PropertyUtils.getPropertyName(beanClass, propertyGetter);
        return getClickhouseColumn(timebasePropertyName, beanClass.getName(), stream);
    }

    protected static ColumnDeclaration getClickhouseColumn(final String timebasePropertyName, final String descriptorName, final DXTickStream stream) {
        RecordClassDescriptor[] descriptors;
        if (stream.isFixedType())
            descriptors = new RecordClassDescriptor[]{stream.getFixedType()};
        else
            descriptors = stream.getPolymorphicDescriptors();

        ColumnDeclarationEx columnDeclarationStream = null;
        for (ClassDescriptor d : descriptors) {
            RecordClassDescriptor descriptor = (RecordClassDescriptor) d;
            if (descriptor.isAbstract())
                continue;

            if (!(descriptor.getName().equals(descriptorName)))
                continue;

            RecordLayout recordLayout = new RecordLayout(descriptor);
            FieldLayout dataField = recordLayout.getField(timebasePropertyName);
            columnDeclarationStream = Util.getColumnDeclaration(descriptor, dataField, stream, ColumnNamingScheme.NAME_AND_DATATYPE);
            break;
        }

        if (columnDeclarationStream != null)
            return columnDeclarationStream;
        else
            throw new IllegalStateException();
    }

    private static InstrumentMessage generateBBO(int value) {
        final BestBidOfferTestMessage message = new BestBidOfferTestMessage();
        initSystemRequiredFields(message);
        message.setSequenceNumber(value);
        message.setBidNumOfOrders(value);
        message.setBidQuoteId(String.valueOf(value));
        message.setOfferPrice(MESSAGE_COUNT - value);
        message.setOfferSize(MESSAGE_COUNT - value);
        message.setOfferNumOfOrders(MESSAGE_COUNT - value);
        message.setOfferQuoteId(String.valueOf(MESSAGE_COUNT - value));
        return message;
    }

    private static InstrumentMessage generateTrades(int value) {
        final TradeTestMessage message = new TradeTestMessage();
        initSystemRequiredFields(message);
        message.setSequenceNumber(value);
        message.setPrice(value);
        message.setSize(value);
        message.setCondition(String.valueOf(value));
        message.setTimestampField(BASE_TIMESTAMP + value);
        return message;
    }

    @BeforeAll
    void init() {
        initClickhouse();
        initTimebase();
    }

    private void initTimebase() {
        String url = System.getenv(TIMEBASE_URL_SYSTEM_VAR_NAME);
        if (StringUtils.isEmpty(url)) {
            GenericContainer<?> tb = DockerTimebaseContainer.getInstance().getContainer();
            String address = tb.getHost();
            Integer port = tb.getFirstMappedPort();
            url = String.format("dxtick://%s:%d", address, port);
        }

        tickDB = TickDBFactory.createFromUrl(url);
        tickDB.open(false);
    }

    private void initClickhouse() {
        String url = System.getenv(CLICKHOUSE_URL_SYSTEM_VAR_NAME);
        String user = System.getenv(CLICKHOUSE_USER_SYSTEM_VAR_NAME);
        String version = System.getenv(BaseStreamReplicatorTests.CLICKHOUSE_SERVER_VERSION);
        if (StringUtils.isEmpty(version)){
            version = BaseStreamReplicatorTests.DEFAULT_CLICKHOUSE_SERVER_VERSION;
        }
        if (StringUtils.isEmpty(url) || StringUtils.isEmpty(user)) {
            ClickHouseContainer ch = DockerClickHouseContainer.getInstance(version).getContainer();
            url = ch.getJdbcUrl();
            user = ch.getUsername();
        }

        Properties clickHouseProperties = new Properties();
        clickHouseProperties.put(ClickHouseDefaults.USER.getKey(), user);
        DataSource dataSource;
        try {
            dataSource = new ClickHouseDataSource(url, clickHouseProperties);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

        clickhouseClient = new ClickhouseClient(dataSource);

        ClickhouseProperties internalProperties = new ClickhouseProperties();
        internalProperties.setUrl(url);
        internalProperties.setUsername(user);
        internalProperties.setPassword("");
        internalProperties.setDatabase(CLICKHOUSE_DATABASE_NAME);

        this.clickhouseProperties = internalProperties;
    }

    protected DXTickStream loadData(final Collection<InstrumentMessage> messages) {
        if (messages.size() == 0)
            throw new IllegalArgumentException("messages");

        final Class<? extends InstrumentMessage> aClass = messages.stream().findFirst().get().getClass();
        String streamKey = String.format("%s_%s", aClass.getName(), Instant.now());

        DXTickStream stream;
        TickLoader loader = null;
        try {
            Class<?> streamType = aClass;

            stream = createStream(tickDB, streamKey, streamType);
            loader = BaseStreamReplicatorTests.createLoader(stream, streamType);

            for (InstrumentMessage message : messages) {
                loader.send(message);
            }
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        } finally {
            if (loader != null)
                loader.close();
        }

        return stream;
    }

    protected DXTickStream loadData(Class<? extends InstrumentMessage> aClass, int number, Function<Integer, InstrumentMessage> generator) {

        String streamKey = String.format("%s_%s", aClass.getName(), Instant.now());

        DXTickStream stream;
        TickLoader loader = null;
        try {
            Class<?> streamType = aClass;

            stream = createStream(tickDB, streamKey, streamType);
            loader = BaseStreamReplicatorTests.createLoader(stream, streamType);

            for (int i = 0; i < number; i++) {
                loader.send(generator.apply(i));
            }
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        } finally {
            if (loader != null)
                loader.close();
        }

        return stream;
    }

    protected List<Map<String, Object>> selectAllValues(final TableDeclaration tableDeclaration, String orderBy) {
        RowMapper<Map<String, Object>> rowMapper = getRowMapper(tableDeclaration.getColumns());

        String sql = getSelectAllQuery(tableDeclaration.getTableIdentity(), orderBy);
        List<Map<String, Object>> values = SelectQueryHelper.executeQuery(sql, clickhouseClient, rowMapper);

        return values;
    }


    protected void checkAllValues(final TableDeclaration tableDeclaration, String orderBy, DXTickStream tickStream,
                                  Consumer<Triple<DXTickStream, List<Map<String, Object>>, Integer>> verifier) {
        try {
            RowMapper<Map<String, Object>> rowMapper = getRowMapper(tableDeclaration.getColumns());

            final TableIdentity tableIdentity = tableDeclaration.getTableIdentity();
//        String sql = getSelectAllQuery(tableIdentity, orderBy);
//        List<Map<String, Object>> values = SelectQueryHelper.executeQuery(sql, clickhouseClient, rowMapper);

            int take = 10000;
            for (int i = 0; true; ++i) {
                SelectBuilder builder = new SelectBuilder();
                builder
                        .expressions("*")
                        .from(tableIdentity).done()
                        .orderBy(orderBy).limit(i * take, take);
                final List<Map<String, Object>> rawData = SelectQueryHelper.executeQuery(builder, clickhouseClient, rowMapper);

                verifier.accept(Triple.of(tickStream, rawData, i * take));
                if (rawData.size() < take)
                    break;
            }
        } catch (Throwable ex) {
            ex.printStackTrace(System.out);
        }
    }

    protected int getCount(final TableIdentity tableIdentity) {
        String tableIdentitySqlDef = getTableIdentitySqlDefinition(tableIdentity);
        RowMapper<Integer> countMapper = (rs, rowNum) -> rs.getInt(1);

        String sql = String.format("SELECT count() from %s", tableIdentitySqlDef);
        int count = SelectQueryHelper.executeQuery(sql, clickhouseClient, countMapper).get(0);

        return count;
    }

//    @Test
//    public void massConcurrentTest() throws InterruptedException {
//        for (int i = 0; i < 100; i++) {
//            concurrentTest();
//        }
//    }

    @Test
    public void concurrentTest() throws InterruptedException {
        try {
            int expectedMessageCount = MESSAGE_COUNT;

            List<DXTickStream> tickStreams = new ArrayList<DXTickStream>();

            tickStreams.add(loadData(BestBidOfferTestMessage.class, MESSAGE_COUNT, value -> generateBBO(value)));
            tickStreams.add(loadData(TradeTestMessage.class, MESSAGE_COUNT, value -> generateTrades(value)));
            tickStreams.add(loadData(BestBidOfferTestMessage.class, MESSAGE_COUNT, value -> generateBBO(value)));
            tickStreams.add(loadData(TradeTestMessage.class, MESSAGE_COUNT, value -> generateTrades(value)));

            LOG.info().append("Data streams created.").commit();
            final ReplicationProperties replicationProperties = new ReplicationProperties();
            final List<StreamRequest> streams = tickStreams.stream().map(t -> {
                StreamRequest streamRequest = new StreamRequest();
                streamRequest.setStream(t.getKey());
                streamRequest.setKey(t.getKey());
                return streamRequest;
            }).collect(Collectors.toList());

            replicationProperties.setStreams(streams);
            replicationProperties.setFlushMessageCount(10_000);
            replicationProperties.setFlushTimeoutMs(20_000);

            final ReplicatorService service = new ReplicatorService(clickhouseClient, tickDB, clickhouseProperties, replicationProperties);
            service.startReplicators();

            LOG.info().append("Starting replication.").commit();

            List<TableDeclaration> tableDeclarations = tickStreams.stream()
                    .map(stream -> Util.getTableDeclaration(stream, ColumnNamingScheme.NAME_AND_DATATYPE))
                    .collect(Collectors.toList());

            final List<TableDeclaration> tableDeclarations2 = new ArrayList<>(tableDeclarations);
            Map<String, Boolean> existsTables = new HashMap<>(tableDeclarations.size());
            while (tableDeclarations.size() > 0) {
                for (int i = tableDeclarations.size(); i-- > 0; ) {
                    final TableDeclaration tableDeclaration = tableDeclarations.get(i);
                    final TableIdentity tableIdentity = tableDeclaration.getTableIdentity();
                    Boolean isTableExists = existsTables.get(tableIdentity.getTableName());
                    if (isTableExists == null || !isTableExists) {
                        existsTables.put(tableIdentity.getTableName(), clickhouseClient.existsTable(tableIdentity));
                    } else if (expectedMessageCount == getCount(tableIdentity)){
                        tableDeclarations.remove(i);
                    }
                }
                Thread.sleep(5000);
            }
            service.destroy();
            LOG.info().append("Replication finished.").commit();

            int executed = 0;
            final int size = tableDeclarations2.size();
            final int poolSize = 4;
            ThreadPoolExecutor executor = new ThreadPoolExecutor(poolSize, poolSize, 0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue());
            for (int i = 0; i < size; i++) {
                final TableDeclaration tableDeclaration = tableDeclarations2.get(i);
                final DXTickStream tickStream = tickStreams.get(i);
                if (tableDeclaration.getTableIdentity().getTableName().contains("BestBidOfferTestMessage")) {
                    final String sequenceNumber = getClickhouseColumn(tickStream, BestBidOfferTestMessage.class, BestBidOfferTestMessage::getSequenceNumber).getDbColumnName();
                    executor.execute(() -> checkAllValues(tableDeclaration, sequenceNumber, tickStream,
                            pair -> verifyBboValues(pair.getLeft(), pair.getMiddle(), pair.getRight())));
                    executed++;
                } else if (tableDeclaration.getTableIdentity().getTableName().contains("TradeTestMessage")) {
                    final String sequenceNumber = getClickhouseColumn(tickStream, TradeTestMessage.class, TradeTestMessage::getSequenceNumber).getDbColumnName();
                    executor.execute(() -> checkAllValues(tableDeclaration, sequenceNumber, tickStream,
                            pair -> verifyTradeValues(pair.getLeft(), pair.getMiddle(), pair.getRight())));
                    executed++;
                }
            }
            while( executor.getCompletedTaskCount() != executed) {
                Thread.sleep(5000);
            }
            LOG.info().append("Concurrent test finished.").commit();
        } catch (InterruptedException e) {
            e.printStackTrace();
            LOG.error().append("Concurrent test failed.").append(e).commit();
        }
    }

    private void verifyBboValues(DXTickStream tickStream, List<Map<String, Object>> allValues, Integer offset) {
        final String bidNumOfOrdersColumnName = getClickhouseColumn(tickStream, BestBidOfferTestMessage.class, BestBidOfferTestMessage::getBidNumOfOrders).getDbColumnName();
        final String bidQuoteIdColumnName = getClickhouseColumn(tickStream, BestBidOfferTestMessage.class, BestBidOfferTestMessage::getBidQuoteId).getDbColumnName();

        final String offerPriceColumnName = getClickhouseColumn(tickStream, BestBidOfferTestMessage.class, BestBidOfferTestMessage::getOfferPrice).getDbColumnName();
        final String offerSizeColumnName = getClickhouseColumn(tickStream, BestBidOfferTestMessage.class, BestBidOfferTestMessage::getOfferSize).getDbColumnName();
        final String offerNumOfOrdersColumnName = getClickhouseColumn(tickStream, BestBidOfferTestMessage.class, BestBidOfferTestMessage::getOfferNumOfOrders).getDbColumnName();
        final String offerQuoteIdColumnName = getClickhouseColumn(tickStream, BestBidOfferTestMessage.class, BestBidOfferTestMessage::getOfferQuoteId).getDbColumnName();

        for (int i = 0; i < allValues.size(); ++i) {
            final Map<String, Object> map = allValues.get(i);

            final int value = i + offset;

            assertEquals(Integer.valueOf(String.valueOf(map.get(bidNumOfOrdersColumnName))), value);
            assertEquals(Integer.valueOf(String.valueOf(map.get(bidQuoteIdColumnName))), value);

            assertEquals(Double.valueOf(String.valueOf(map.get(offerPriceColumnName))), MESSAGE_COUNT - value);
            assertEquals(Double.valueOf(String.valueOf(map.get(offerSizeColumnName))), MESSAGE_COUNT - value);
            assertEquals(Integer.valueOf(String.valueOf(map.get(offerNumOfOrdersColumnName))), MESSAGE_COUNT - value);
            assertEquals(Integer.valueOf(String.valueOf(map.get(offerQuoteIdColumnName))), MESSAGE_COUNT - value);
        }
    }

    private void verifyTradeValues(DXTickStream tickStream, List<Map<String, Object>> allValues, Integer offset) {
        final String priceColumnName = getClickhouseColumn(tickStream, TradeTestMessage.class, TradeTestMessage::getPrice).getDbColumnName();
        final String sizeColumnName = getClickhouseColumn(tickStream, TradeTestMessage.class, TradeTestMessage::getSize).getDbColumnName();
        final String conditionColumnName = getClickhouseColumn(tickStream, TradeTestMessage.class, TradeTestMessage::getCondition).getDbColumnName();
        final String timestampField = getClickhouseColumn(tickStream, TradeTestMessage.class, TradeTestMessage::getTimestampField).getDbColumnName();

        for (int i = 0; i < allValues.size(); ++i) {
            final Map<String, Object> map = allValues.get(i);

            final int value = i + offset;

            assertEquals(Double.valueOf(String.valueOf(map.get(priceColumnName))), value);
            assertEquals(Double.valueOf(String.valueOf(map.get(sizeColumnName))), value);
            assertEquals(Integer.valueOf(String.valueOf(map.get(conditionColumnName))), value);
            assertEquals(getTimestampMs(map.get(timestampField)), BASE_TIMESTAMP + value);
        }
    }

    private long getTimestampMs(Object timestamp) {
        try {
            SimpleDateFormat timestampFormatMs = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
            timestampFormatMs.setTimeZone(TimeZone.getTimeZone("UTC"));
            return timestampFormatMs.parse(convertTimestamp(timestamp)).getTime();
        } catch (ParseException e) {
            throw new RuntimeException(e);
        }
    }
    private String convertTimestamp(Object timestamp) throws ParseException {
        String str = (String) timestamp;
        if (str.length() == 22) {
            str = str + "0";
        } else if (str.length() == 21) {
            str = str + "00";
        } else if (str.length() == 20) {
            str = str + "000";
        } else if (str.length() == 19) {
            str = str + ".000";
        }
        return str;
    }
}
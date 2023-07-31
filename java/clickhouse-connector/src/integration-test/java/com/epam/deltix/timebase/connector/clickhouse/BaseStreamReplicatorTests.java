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
import com.clickhouse.data.value.UnsignedByte;
import com.clickhouse.jdbc.ClickHouseDataSource;
import com.epam.deltix.clickhouse.schema.types.*;
import com.epam.deltix.timebase.connector.clickhouse.configuration.properties.ClickhouseProperties;
import com.epam.deltix.timebase.connector.clickhouse.containers.DockerClickHouseContainer;
import com.epam.deltix.timebase.connector.clickhouse.containers.DockerTimebaseContainer;
import com.epam.deltix.timebase.connector.clickhouse.timebase.CustomTypeLoader;
import com.epam.deltix.timebase.connector.clickhouse.util.ClickhouseUtil;
import com.epam.deltix.timebase.connector.clickhouse.util.Util;
import com.epam.deltix.timebase.messages.InstrumentMessage;
import com.epam.deltix.util.time.TimeKeeper;
import de.cronn.reflection.util.PropertyUtils;
import de.cronn.reflection.util.TypedPropertyGetter;
import com.epam.deltix.clickhouse.ClickhouseClient;
import com.epam.deltix.clickhouse.models.TableIdentity;
import com.epam.deltix.clickhouse.schema.ColumnDeclaration;
import com.epam.deltix.clickhouse.schema.TableDeclaration;
import com.epam.deltix.clickhouse.util.SelectQueryHelper;
import com.epam.deltix.dfp.Decimal64Utils;
import com.epam.deltix.gflog.api.Log;
import com.epam.deltix.gflog.api.LogFactory;
import com.epam.deltix.gflog.api.LogLevel;
import com.epam.deltix.qsrv.hf.pub.codec.FieldLayout;
import com.epam.deltix.qsrv.hf.pub.codec.RecordLayout;
import com.epam.deltix.qsrv.hf.pub.md.ClassDescriptor;
import com.epam.deltix.qsrv.hf.pub.md.Introspector;
import com.epam.deltix.qsrv.hf.pub.md.RecordClassDescriptor;
import com.epam.deltix.qsrv.hf.tickdb.pub.*;
import com.epam.deltix.timebase.connector.clickhouse.algos.ColumnDeclarationEx;
import com.epam.deltix.timebase.connector.clickhouse.algos.QueryReplicator;
import com.epam.deltix.timebase.connector.clickhouse.algos.SchemaProcessor;
import com.epam.deltix.timebase.connector.clickhouse.algos.StreamReplicator;
import com.epam.deltix.timebase.connector.clickhouse.model.ColumnNamingScheme;
import com.epam.deltix.timebase.connector.clickhouse.model.QueryRequest;
import com.epam.deltix.timebase.connector.clickhouse.model.StreamRequest;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestInstance;
import org.springframework.jdbc.core.RowMapper;
import org.testcontainers.containers.ClickHouseContainer;
import org.testcontainers.containers.GenericContainer;

import javax.sql.DataSource;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.math.BigDecimal;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.*;

import static com.clickhouse.client.config.ClickHouseClientOption.USE_OBJECTS_IN_ARRAYS;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public abstract class BaseStreamReplicatorTests {
    protected static final Log LOG = LogFactory.getLog(BaseStreamReplicatorTests.class);

    private final static String TB_SYMBOL_VALUE = "BTCUSD";
    protected final static String CLICKHOUSE_DATABASE_NAME = "tbMessages";

    private final static String TIMEBASE_URL_SYSTEM_VAR_NAME = "TIMEBASE_URL";
    private final static String CLICKHOUSE_URL_SYSTEM_VAR_NAME = "CLICKHOUSE_URL";
    private final static String CLICKHOUSE_USER_SYSTEM_VAR_NAME = "CLICKHOUSE_USER";
    public static final String CLICKHOUSE_SERVER_VERSION = "CLICKHOUSE_VERSION";
    public static final String DEFAULT_CLICKHOUSE_SERVER_VERSION = "23.4.3.48";
    private final static DateTimeFormatter FORMAT = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS");
    public static final SimpleDateFormat TIMESTAMP_FORMAT_MS = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");

    private static final int DEFAULT_EXPECTED_MESSAGE_COUNT = 1;
    private static final int DEFAULT_FLUSH_MESSAGE_COUNT = 1;
    private static final long DEFAULT_FLUSH_TIMEOUT_MS = 1;


    // required environment variables values
    // TIMEBASE_URL=dxtick://10.10.81.26:8011;
    // optional environment variables
    // CLICKHOUSE_URL=jdbc:clickhouse://10.10.81.55:8623/default;CLICKHOUSE_USER=read
    protected ClickhouseClient clickhouseClient;
    protected ClickhouseProperties clickhouseProperties;
    protected DXTickDB tickDB;


    @BeforeAll
    void init() {
        initClickhouse();
        initTimebase();
        TIMESTAMP_FORMAT_MS.setTimeZone(TimeZone.getTimeZone("UTC"));
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
        String version = System.getenv(CLICKHOUSE_SERVER_VERSION);
        if (StringUtils.isEmpty(version)){
            version = DEFAULT_CLICKHOUSE_SERVER_VERSION;
        }
        if (StringUtils.isEmpty(url) || StringUtils.isEmpty(user)) {
            ClickHouseContainer ch = DockerClickHouseContainer.getInstance(version).getContainer();
            url = ch.getJdbcUrl();
            user = ch.getUsername();
        }

        final Properties clickHouseProperties = new Properties();
        clickHouseProperties.put(USE_OBJECTS_IN_ARRAYS.getKey(), true);
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

        try {
            clickhouseClient.createDatabase(clickhouseProperties.getDatabase(), true);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    // helpers

    protected static RowMapper<Map<String, Object>> getRowMapper(final List<ColumnDeclaration> columns) {
        Map<String, ColumnDeclaration> columnDeclarations = new HashMap<>();
        extractColumnDeclarationMap(columnDeclarations, columns, "");

        return getRowMapper(columnDeclarations);
    }

    private static void extractColumnDeclarationMap(Map<String, ColumnDeclaration> columnDeclarations,
                                                    List<ColumnDeclaration> columns, String parent) {
        for (ColumnDeclaration column : columns) {
            SqlDataType dbDataType = column.getDbDataType();
            if (dbDataType instanceof ObjectDataType) {
                extractColumnDeclarationMap(columnDeclarations, ((ObjectDataType) dbDataType).getColumns(), parent + column.getDbColumnName() + "_");
            } else {
                columnDeclarations.put(parent + column.getDbColumnName(), column);
            }
        }
    }

    protected static RowMapper<Map<String, Object>> getRowMapper(final Map<String, ColumnDeclaration> columnDeclarations) {
        return (rs, rowNum) -> {
            Map<String, Object> result = new HashMap<>();

            for (Map.Entry<String, ColumnDeclaration> entry : columnDeclarations.entrySet()) {
                String key = entry.getKey();
                ColumnDeclarationEx value = (ColumnDeclarationEx) entry.getValue();
                extractColumnValue(rs, result, key, value, false);

            }
            return result;
        };
    }

    private static void extractColumnValue(ResultSet rs, Map<String, Object> result, String key, ColumnDeclarationEx value, boolean isNested) throws SQLException {
        SqlDataType innerDataType = value.getDbDataType();
        if (innerDataType instanceof NestedDataType) {
            List<ColumnDeclaration> columns = ((NestedDataType) innerDataType).getColumns();
            for (ColumnDeclaration column : columns) {
                extractColumnValue(rs, result, key + "." + column.getDbColumnName(), (ColumnDeclarationEx) column, true);
            }
        } else if (innerDataType instanceof ObjectDataType) {
            List<ColumnDeclaration> columns = ((ObjectDataType) innerDataType).getColumns();
            for (ColumnDeclaration column : columns) {
                extractColumnValue(rs, result, key + "_" + column.getDbColumnName(), (ColumnDeclarationEx) column, isNested);
            }
        } else {
            Object exprValue = getValue(value.getDbDataType(), rs, key, isNested);
            result.put(key, exprValue);
        }
    }

    protected static Object getValue(final SqlDataType type,
                                     final ResultSet rs, final String columnLabel, boolean isNested) throws SQLException {
        if (type instanceof NullableDataType)
            return getValue(((NullableDataType) type).getNestedType(), rs, columnLabel, isNested);
        else{
            if (type.getType() == DataTypes.ARRAY){
                return getArrayValue((ArraySqlType)type, rs, columnLabel, isNested);
            }
            return getValue(type.getType(), rs, columnLabel, isNested);
        }

    }

    private static Object getArrayValue(ArraySqlType type, ResultSet rs, String columnLabel, boolean isNested) throws SQLException {
        if (isNested) return rs.getObject(columnLabel);
        SqlDataType elementType = type.getElementType();
        elementType = elementType instanceof NullableDataType ? ((NullableDataType) elementType).getNestedType() : elementType;
        Object object = rs.getObject(columnLabel);
        switch (elementType.getType()) {
            case DATE_TIME64:
                return convertDataTime((LocalDateTime[]) object);
            case UINT8:
                return convertBytes((UnsignedByte[]) object);

        }
        return object;
    }

    protected static Object convertBytes(UnsignedByte[] object) {
        Byte[] bytes = new Byte[object.length];
        for (int i = 0; i < object.length; i++) {
            if (object[i] == null)
                bytes[i] = null;
            else
                bytes[i] = object[i].byteValue();
        }
        return bytes;
    }

    private static Object convertDataTime(LocalDateTime[] object) {
        String[] strings = new String[object.length];
        for (int i = 0; i < object.length; i++) {
            if (object[i] == null)
                strings[i] = null;
            else
                strings[i] = object[i].format(FORMAT);
        }
        return strings;
    }

    protected static Object getValue(final DataTypes type, final ResultSet rs, final String columnLabel, boolean isNested) throws SQLException {
        if (isNested) return rs.getObject(columnLabel);
        switch (type) {
            case UINT8:
                return isNotNullOrArray(rs, columnLabel) ? rs.getBoolean(columnLabel) : rs.getObject(columnLabel);
            case INT8:
                return isNotNullOrArray(rs, columnLabel) ? rs.getByte(columnLabel) : rs.getObject(columnLabel);
            case INT16:
                return isNotNullOrArray(rs, columnLabel) ? rs.getShort(columnLabel) : rs.getObject(columnLabel);
            case DATE_TIME64:
                return rs.getString(columnLabel);

            default:
                return rs.getObject(columnLabel);
        }
    }

    private static boolean isNotNullOrArray(ResultSet rs, String columnLabel) throws SQLException {
        return rs.getObject(columnLabel) != null && !rs.getObject(columnLabel).getClass().isArray();
    }

    protected static DXTickStream createStream(final DXTickDB tickDb, final String streamKey, final Class<?>... streamType) {
        Introspector emptyIntrospector = Introspector.createEmptyMessageIntrospector();

        StreamOptions streamOptions = new StreamOptions();
        streamOptions.distributionFactor = StreamOptions.MAX_DISTRIBUTION;
        streamOptions.version = "5.0";
        streamOptions.setPolymorphic(getRecordClassDescriptors(emptyIntrospector, streamType).toArray(new RecordClassDescriptor[0]));
//        streamOptions.setFixedType(getRecordClassDescriptors(emptyIntrospector, streamType).toArray(new RecordClassDescriptor[0])[0]);

        return tickDb.createStream(streamKey, streamOptions);
    }

    protected static TickLoader createLoader(final DXTickStream stream, java.lang.Class... types) {
        LoadingOptions loadingOptions = new LoadingOptions(LoadingOptions.WriteMode.APPEND);
        loadingOptions.typeLoader = new CustomTypeLoader(types);

        return stream.createLoader(loadingOptions);
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

    protected static String getSelectAllQuery(final TableIdentity tableIdentity) {
        String tableIdentitySqlDef = getTableIdentitySqlDefinition(tableIdentity);

        return String.format("SELECT * FROM %s order by timestamp, instrument", tableIdentitySqlDef);
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

    protected static int toMillisOfDay(final LocalTime time) {
        return (int) (time.toNanoOfDay() / 1_000_000);
    }

    protected static long getTimestamp(final String timestamp) throws ParseException {
        return TIMESTAMP_FORMAT_MS.parse(timestamp).getTime();
    }

    protected Pair<DXTickStream, TableDeclaration> loadAndReplicateData(final InstrumentMessage message, java.lang.Class... types) {
        DXTickStream stream = loadData(message, types);
        TableDeclaration tableDeclaration = getTableDeclaration(stream, ColumnNamingScheme.TYPE_AND_NAME);
        startStreamReplication(stream, tableDeclaration.getTableIdentity());

        return Pair.of(stream, tableDeclaration);
    }

    protected DXTickStream loadData(final InstrumentMessage message, java.lang.Class... types) {
        String streamKey = String.format("%s_%s", message.getClass().getName(), TimeKeeper.currentTime);

        DXTickStream stream;
        TickLoader loader = null;
        try {
            Class<?> streamType = message.getClass();

            stream = createStream(tickDB, streamKey, streamType);
            loader = createLoader(stream, types);

            loader.send(message);
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        } finally {
            if (loader != null)
                loader.close();
        }

        return stream;
    }

    public static void assertColumnsCount(TableDeclaration tableDeclaration, int expectedCount) {
        List<ColumnDeclaration> columns = tableDeclaration.getColumns();
        List<ColumnDeclarationEx> columnsDeep = ColumnDeclarationEx.getColumnsDeep(ClickhouseUtil.toExColumnsUnchecked(columns));
        assertEquals(expectedCount, columnsDeep.size());
    }

    protected Pair<DXTickStream, TableDeclaration> loadAndReplicateData(final InstrumentMessage message) {
        DXTickStream stream = loadData(message);
        TableDeclaration tableDeclaration = getTableDeclaration(stream, ColumnNamingScheme.TYPE_AND_NAME);
        startStreamReplication(stream, tableDeclaration.getTableIdentity());

        return Pair.of(stream, tableDeclaration);
    }

    protected TableDeclaration getTableDeclaration(DXTickStream stream, ColumnNamingScheme pattern) {
        return Util.getTableDeclaration(stream, pattern);
    }

    protected DXTickStream loadData(final InstrumentMessage message) {
        String streamKey = String.format("%s_%s", message.getClass().getName(), Instant.now());

        DXTickStream stream;
        TickLoader loader = null;
        try {
            Class<?> streamType = message.getClass();

            stream = createStream(tickDB, streamKey, streamType);
            loader = createLoader(stream, streamType);

            loader.send(message);
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        } finally {
            if (loader != null)
                loader.close();
        }

        return stream;
    }

    protected void startStreamReplication(final TickStream stream, final TableIdentity tableIdentity) {
        startStreamReplication(stream, tableIdentity, DEFAULT_EXPECTED_MESSAGE_COUNT, DEFAULT_FLUSH_MESSAGE_COUNT, DEFAULT_FLUSH_TIMEOUT_MS, null);
    }
    protected void startStreamReplication(final TickStream stream, final TableIdentity tableIdentity, StreamRequest streamRequest, int expectedMessageCount) {
        startStreamReplication(stream, tableIdentity, expectedMessageCount, DEFAULT_FLUSH_MESSAGE_COUNT, DEFAULT_FLUSH_TIMEOUT_MS, streamRequest);
    }

    protected void startStreamReplication(final TickStream stream, final TableIdentity tableIdentity, final int expectedMessageCount,
                                          final int flushMessageCount, final long flushTimeoutMs, StreamRequest streamRequest) {
        if (streamRequest == null) {
            streamRequest = new StreamRequest();
            streamRequest.setKey(stream.getKey());
            streamRequest.setStream(stream.getKey());
            streamRequest.setColumnNamingScheme(ColumnNamingScheme.TYPE_AND_NAME);
            streamRequest.setIncludePartitionColumn(true);
        }
        StreamReplicator replicator = new StreamReplicator(streamRequest, tickDB, clickhouseClient, clickhouseProperties, flushMessageCount, flushTimeoutMs, streamReplicator -> {});
        Thread replicatorThread = new Thread(replicator, String.format("Stream replicator '%s'", stream.getKey()));

        replicatorThread.start();

        // poll from CH until not replicated
        boolean isTableExists = clickhouseClient.existsTable(tableIdentity);
        int messageCount = 0;
        do {
            if (Thread.currentThread().isInterrupted())
                break;

            if (replicatorThread.getState() == Thread.State.TERMINATED){
                throw new RuntimeException("Replication failed");
            }

            if (isTableExists)
                messageCount = getCount(tableIdentity);
            else
                isTableExists = clickhouseClient.existsTable(tableIdentity);
        } while (expectedMessageCount != messageCount);

        replicator.stop();
        try {
            LOG.info()
                    .append("Joining thread '")
                    .append(replicatorThread.getName())
                    .append("'")
                    .commit();

            replicatorThread.join();
        } catch (InterruptedException e) {
            // log and continue
            LOG.info()
                    .append("Thread '")
                    .append(replicatorThread.getName())
                    .append("' interrupted.")
                    .commit();
        }
    }

    protected void startQueryReplication(final TableIdentity tableIdentity, final int expectedMessageCount,
                                          final int flushMessageCount, final long flushTimeoutMs, QueryRequest queryRequest) {
        QueryReplicator replicator = new QueryReplicator(queryRequest, tickDB, clickhouseClient, clickhouseProperties, flushMessageCount, flushTimeoutMs, streamReplicator -> {});
        Thread replicatorThread = new Thread(replicator, String.format("Stream replicator '%s'", queryRequest.getKey()));

        replicatorThread.start();

        // poll from CH until not replicated
        boolean isTableExists = clickhouseClient.existsTable(tableIdentity);
        int messageCount = 0;
        do {
            if (Thread.currentThread().isInterrupted())
                break;

            if (replicatorThread.getState() == Thread.State.TERMINATED){
                throw new RuntimeException("Replication failed");
            }

            if (isTableExists)
                messageCount = getCount(tableIdentity);
            else
                isTableExists = clickhouseClient.existsTable(tableIdentity);
        } while (expectedMessageCount != messageCount);

        replicator.stop();
        try {
            LOG.info()
                    .append("Joining thread '")
                    .append(replicatorThread.getName())
                    .append("'")
                    .commit();

            replicatorThread.join();
        } catch (InterruptedException e) {
            // log and continue
            LOG.info()
                    .append("Thread '")
                    .append(replicatorThread.getName())
                    .append("' interrupted.")
                    .commit();
        }
    }

    protected static <T> String getDbColumnName(final DXTickStream stream, final Class<T> beanClass,
                                                               final TypedPropertyGetter<T, ?> propertyGetter) {
        return getClickhouseColumn(stream, beanClass, propertyGetter).getDbColumnName();
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
            columnDeclarationStream = Util.getColumnDeclaration(descriptor, dataField, stream, ColumnNamingScheme.TYPE_AND_NAME);
            break;
        }

        if (columnDeclarationStream != null)
            return columnDeclarationStream;
        else
            throw new IllegalStateException();
    }

    protected static <T> String getFieldNotNullableMessage(final Class<T> beanClass, final TypedPropertyGetter<T, ?> propertyGetter) {
        String timebasePropertyName = PropertyUtils.getPropertyName(beanClass, propertyGetter);
        return String.format("'%s' field is not nullable", timebasePropertyName);
    }

    protected static long getDecimal64Value(final BigDecimal value) {
        return value != null ? Decimal64Utils.parse(value.toString()) : Decimal64Utils.NULL;
    }

    protected List<Map<String, Object>> selectAllValues(final TableDeclaration tableDeclaration) {
        RowMapper<Map<String, Object>> rowMapper = getRowMapper(tableDeclaration.getColumns());

        String sql = getSelectAllQuery(tableDeclaration.getTableIdentity());
        List<Map<String, Object>> values = SelectQueryHelper.executeQuery(sql, clickhouseClient, rowMapper);

        return values;
    }

    protected int getCount(final TableIdentity tableIdentity) {
        String tableIdentitySqlDef = getTableIdentitySqlDefinition(tableIdentity);
        RowMapper<Integer> countMapper = (rs, rowNum) -> rs.getInt(1);

        String sql = String.format("SELECT count() from %s", tableIdentitySqlDef);
        int count = SelectQueryHelper.executeQuery(sql, clickhouseClient, countMapper).get(0);

        return count;
    }

    protected <T> T getArrayValue(Map<String, Object> values, String dbColumnName, Class<T> clazz) {
        Object result = values.get(dbColumnName);
        return clazz.cast(result);
    }

    public static void assertEqualsObjectFields(Map<String, Object> values, Map<String, InstrumentMessage> objects) throws IllegalAccessException, InvocationTargetException {
        for (Map.Entry<String, Object> entry : values.entrySet()) {
            String columnName = entry.getKey();
            for (Map.Entry<String, InstrumentMessage> object_entry : objects.entrySet()) {
                InstrumentMessage object = object_entry.getValue();
                if (columnName.equals(SchemaProcessor.TYPE_COLUMN_NAME) || columnName.equals(SchemaProcessor.INSTRUMENT_COLUMN_NAME) ||
                        columnName.equals(SchemaProcessor.TIMESTAMP_COLUMN_NAME) || columnName.equals(SchemaProcessor.PARTITION_COLUMN_NAME))
                    continue;
                if (columnName.startsWith(object_entry.getKey())) {
                    if (columnName.contains(object_entry.getKey() + "_type")) {
                        assertEquals(entry.getValue(), object.getClass().getName());
                    } else {
                        Method m = Arrays.stream(object.getClass().getMethods())
                                .filter(method -> method.getName().startsWith("get") || method.getName().startsWith("is"))
                                .filter(method -> columnName.contains(method.getName().substring(4)))
                                .findFirst()
                                .orElseThrow(() -> new AssertionError("Not found value for field from " + columnName));
                        Object invoke = m.invoke(object);
                        assertEquals(entry.getValue(), invoke);
                    }
                }
            }
        }
    }
}
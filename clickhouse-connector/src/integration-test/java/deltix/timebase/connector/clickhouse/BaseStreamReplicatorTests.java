package deltix.timebase.connector.clickhouse;

import de.cronn.reflection.util.PropertyUtils;
import de.cronn.reflection.util.TypedPropertyGetter;
import deltix.clickhouse.ClickhouseClient;
import deltix.clickhouse.models.TableIdentity;
import deltix.clickhouse.schema.ColumnDeclaration;
import deltix.clickhouse.schema.TableDeclaration;
import deltix.clickhouse.schema.types.DataTypes;
import deltix.clickhouse.schema.types.NullableDataType;
import deltix.clickhouse.util.SelectQueryHelper;
import deltix.dfp.Decimal64Utils;
import deltix.gflog.Log;
import deltix.gflog.LogFactory;
import deltix.gflog.LogLevel;
import deltix.qsrv.hf.pub.codec.FieldLayout;
import deltix.qsrv.hf.pub.codec.RecordLayout;
import deltix.qsrv.hf.pub.md.ClassDescriptor;
import deltix.qsrv.hf.pub.md.Introspector;
import deltix.qsrv.hf.pub.md.RecordClassDescriptor;
import deltix.qsrv.hf.tickdb.pub.*;
import deltix.timebase.connector.clickhouse.algos.ColumnDeclarationEx;
import deltix.timebase.connector.clickhouse.algos.SchemaProcessor;
import deltix.timebase.connector.clickhouse.algos.StreamReplicator;
import deltix.timebase.connector.clickhouse.configuration.properties.ClickhouseProperties;
import deltix.timebase.connector.clickhouse.containers.DockerClickHouseContainer;
import deltix.timebase.connector.clickhouse.containers.DockerTimebaseContainer;
import deltix.timebase.connector.clickhouse.timebase.CustomTypeLoader;
import deltix.timebase.messages.InstrumentMessage;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestInstance;
import org.springframework.jdbc.core.RowMapper;
import org.testcontainers.containers.ClickHouseContainer;
import org.testcontainers.containers.GenericContainer;
import ru.yandex.clickhouse.ClickHouseDataSource;
import ru.yandex.clickhouse.settings.ClickHouseProperties;

import javax.sql.DataSource;
import java.math.BigDecimal;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.ParseException;
import java.time.Instant;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import static deltix.clickhouse.writer.codec.CodecUtil.TIMESTAMP_FORMAT_MS;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public abstract class BaseStreamReplicatorTests {
    protected static final Log LOG = LogFactory.getLog(BaseStreamReplicatorTests.class);

    private final static String TB_SYMBOL_VALUE = "BTCUSD";
    private final static String CLICKHOUSE_DATABASE_NAME = "tbMessages";

    private final static String TIMEBASE_URL_SYSTEM_VAR_NAME = "TIMEBASE_URL";
    private final static String CLICKHOUSE_URL_SYSTEM_VAR_NAME = "CLICKHOUSE_URL";
    private final static String CLICKHOUSE_USER_SYSTEM_VAR_NAME = "CLICKHOUSE_USER";

    private static final int DEFAULT_EXPECTED_MESSAGE_COUNT = 1;
    private static final int DEFAULT_FLUSH_MESSAGE_COUNT = 1;
    private static final long DEFAULT_FLUSH_TIMEOUT_MS = 1;


    // required environment variables values
    // TIMEBASE_URL=dxtick://10.10.81.26:8011;
    // optional environment variables
    // CLICKHOUSE_URL=jdbc:clickhouse://10.10.81.55:8623/default;CLICKHOUSE_USER=read
    private ClickhouseClient clickhouseClient;
    private ClickhouseProperties clickhouseProperties;
    private DXTickDB tickDB;


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
        if (StringUtils.isEmpty(url) || StringUtils.isEmpty(user)) {
            ClickHouseContainer ch = DockerClickHouseContainer.getInstance().getContainer();
            url = ch.getJdbcUrl();
            user = ch.getUsername();
        }

        ClickHouseProperties clickHouseProperties = new ClickHouseProperties();
        clickHouseProperties.setUser(user);
        DataSource dataSource = new ClickHouseDataSource(url, clickHouseProperties);

        clickhouseClient = new ClickhouseClient(dataSource);

        ClickhouseProperties internalProperties = new ClickhouseProperties();
        internalProperties.setUrl(url);
        internalProperties.setUsername(user);
        internalProperties.setPassword("");
        internalProperties.setDatabase(CLICKHOUSE_DATABASE_NAME);

        this.clickhouseProperties = internalProperties;
    }

    // helpers

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

    protected static Object getValue(final deltix.clickhouse.schema.types.DataType type,
                                     final ResultSet rs, final String columnLabel) throws SQLException {
        if (type instanceof NullableDataType)
            return getValue(((NullableDataType) type).getNestedType(), rs, columnLabel);
        else
            return getValue(type.getType(), rs, columnLabel);
    }

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

        return String.format("SELECT * FROM %s", tableIdentitySqlDef);
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
        TableDeclaration tableDeclaration = SchemaProcessor.timebaseStreamToClickhouseTable(stream, new HashMap<>(), new ArrayList<>(), CLICKHOUSE_DATABASE_NAME);
        startStreamReplication(stream, tableDeclaration.getTableIdentity());

        return Pair.of(stream, tableDeclaration);
    }

    protected DXTickStream loadData(final InstrumentMessage message, java.lang.Class... types) {
        String streamKey = String.format("%s_%s", message.getClass().getName(), Instant.now());

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


    protected Pair<DXTickStream, TableDeclaration> loadAndReplicateData(final InstrumentMessage message) {
        DXTickStream stream = loadData(message);
        TableDeclaration tableDeclaration = SchemaProcessor.timebaseStreamToClickhouseTable(stream, new HashMap<>(), new ArrayList<>(), CLICKHOUSE_DATABASE_NAME);
        startStreamReplication(stream, tableDeclaration.getTableIdentity());

        return Pair.of(stream, tableDeclaration);
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
        startStreamReplication(stream, tableIdentity, DEFAULT_EXPECTED_MESSAGE_COUNT, DEFAULT_FLUSH_MESSAGE_COUNT, DEFAULT_FLUSH_TIMEOUT_MS);
    }

    protected void startStreamReplication(final TickStream stream, final TableIdentity tableIdentity, final int expectedMessageCount,
                                          final int flushMessageCount, final long flushTimeoutMs) {
        StreamReplicator replicator = new StreamReplicator(stream, clickhouseClient, clickhouseProperties, flushMessageCount, flushTimeoutMs, streamReplicator -> {});
        Thread replicatorThread = new Thread(replicator, String.format("Stream replicator '%s'", stream.getKey()));

        replicatorThread.start();

        // poll from CH until not replicated
        boolean isTableExists = clickhouseClient.existsTable(tableIdentity);
        int messageCount = 0;
        do {
            if (Thread.currentThread().isInterrupted())
                break;

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

        ClassDescriptor[] allDescriptors = stream.getAllDescriptors();

        ColumnDeclarationEx columnDeclarationStream = null;
        for (ClassDescriptor d : descriptors) {
            RecordClassDescriptor descriptor = (RecordClassDescriptor) d;
            if (descriptor.isAbstract())
                continue;

            if (!(descriptor.getName().equals(descriptorName)))
                continue;

            RecordLayout recordLayout = new RecordLayout(descriptor);
            FieldLayout dataField = recordLayout.getField(timebasePropertyName);
            columnDeclarationStream = SchemaProcessor.timebaseDataFieldToClickhouseColumnDeclaration(descriptor, dataField, allDescriptors);
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
}

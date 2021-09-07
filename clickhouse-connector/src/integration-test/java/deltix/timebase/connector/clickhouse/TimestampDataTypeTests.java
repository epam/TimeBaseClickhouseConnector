package deltix.timebase.connector.clickhouse;

import deltix.clickhouse.schema.ColumnDeclaration;
import deltix.clickhouse.schema.TableDeclaration;
import com.epam.deltix.qsrv.hf.pub.md.DateTimeDataType;
import com.epam.deltix.qsrv.hf.tickdb.pub.DXTickStream;
import deltix.timebase.connector.clickhouse.timebase.NullableTimestampMessage;
import deltix.timebase.connector.clickhouse.timebase.TimestampMessage;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.text.ParseException;
import java.time.Instant;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.sameInstance;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class TimestampDataTypeTests extends BaseStreamReplicatorTests {

    // Tests for nullable Timestamp data type

    @Test
    @Timeout(10)
    void readNullableTimestampFromCH_expectedNotNullValue() throws ParseException {
        final long expectedValue = Instant.parse("2020-06-10T20:40:38.155Z").toEpochMilli();

        NullableTimestampMessage message = new NullableTimestampMessage();
        message.setTimestampField(expectedValue);
        initSystemRequiredFields(message);

        Pair<DXTickStream, TableDeclaration> chSchemaByStream = loadAndReplicateData(message);
        ColumnDeclaration clickhouseColumn = getClickhouseColumn(chSchemaByStream.getLeft(), NullableTimestampMessage.class, NullableTimestampMessage::getTimestampField);
        Map<String, Object> values = selectAllValues(chSchemaByStream.getRight()).get(0);
        long actualValue = getTimestamp((String) values.get(clickhouseColumn.getDbColumnName()));

        systemRequiredFieldsCheck(message, values);
        assertEquals(expectedValue, actualValue);
    }

    @Test
    @Timeout(10)
    void readNullableTimestampFromCH_expectedNullValue() {
        final long timestampNull = DateTimeDataType.NULL;
        final Object expectedValue = null;

        NullableTimestampMessage message = new NullableTimestampMessage();
        message.setTimestampField(timestampNull);
        initSystemRequiredFields(message);

        Pair<DXTickStream, TableDeclaration> chSchemaByStream = loadAndReplicateData(message);
        ColumnDeclaration clickhouseColumn = getClickhouseColumn(chSchemaByStream.getLeft(), NullableTimestampMessage.class, NullableTimestampMessage::getTimestampField);
        Map<String, Object> values = selectAllValues(chSchemaByStream.getRight()).get(0);
        Object actualValue = values.get(clickhouseColumn.getDbColumnName());

        systemRequiredFieldsCheck(message, values);
        assertEquals(expectedValue, actualValue);
    }

    // Tests for non nullable Timestamp data type

    @Test
    @Timeout(10)
    void readNonNullableTimestampFromCH_expectedNotNullValue() throws ParseException {
        final long expectedValue = Instant.parse("2020-06-10T20:40:38.155Z").toEpochMilli();

        TimestampMessage message = new TimestampMessage();
        message.setTimestampField(expectedValue);
        initSystemRequiredFields(message);

        Pair<DXTickStream, TableDeclaration> chSchemaByStream = loadAndReplicateData(message);
        ColumnDeclaration clickhouseColumn = getClickhouseColumn(chSchemaByStream.getLeft(), TimestampMessage.class, TimestampMessage::getTimestampField);
        Map<String, Object> values = selectAllValues(chSchemaByStream.getRight()).get(0);
        long actualValue = getTimestamp((String) values.get(clickhouseColumn.getDbColumnName()));

        systemRequiredFieldsCheck(message, values);
        assertEquals(expectedValue, actualValue);
    }

    @Test
    @Timeout(10)
    void writeNullToNonNullableTimestampInTB_expectedRuntimeException() {
        final long illegalValue = DateTimeDataType.NULL;
        final String expectedErrorMessage = getFieldNotNullableMessage(TimestampMessage.class, TimestampMessage::getTimestampField);

        TimestampMessage message = new TimestampMessage();
        message.setTimestampField(illegalValue);
        initSystemRequiredFields(message);

        Exception exception = assertThrows(RuntimeException.class, () -> loadAndReplicateData(message));
        assertThat(exception.getCause().getClass(), sameInstance(IllegalArgumentException.class));
        assertEquals(expectedErrorMessage, exception.getCause().getMessage());
    }
}

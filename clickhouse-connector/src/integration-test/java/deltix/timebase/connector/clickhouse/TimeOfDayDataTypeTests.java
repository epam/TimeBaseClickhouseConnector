package deltix.timebase.connector.clickhouse;

import deltix.clickhouse.schema.ColumnDeclaration;
import deltix.clickhouse.schema.TableDeclaration;
import deltix.qsrv.hf.pub.md.TimeOfDayDataType;
import deltix.qsrv.hf.tickdb.pub.DXTickStream;
import deltix.timebase.connector.clickhouse.timebase.NullableTimeOfDayMessage;
import deltix.timebase.connector.clickhouse.timebase.TimeOfDayMessage;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.time.LocalTime;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class TimeOfDayDataTypeTests extends BaseStreamReplicatorTests {

    // Tests for nullable TimeOfDay data type

    @Timeout(10)
    @ParameterizedTest
    @ValueSource(
            strings = {"12:16:48.111", "12:16:48", "23:59:59.999", "00:00:00.000"}
    )
    void readNullableTimeOfDayFromCH_expectedNotNullValue(String argument) {
        final int expectedValue = toMillisOfDay(LocalTime.parse(argument));

        NullableTimeOfDayMessage message = new NullableTimeOfDayMessage();
        message.setTimeOfDayField(expectedValue);
        initSystemRequiredFields(message);

        Pair<DXTickStream, TableDeclaration> chSchemaByStream = loadAndReplicateData(message);
        ColumnDeclaration clickhouseColumn = getClickhouseColumn(chSchemaByStream.getLeft(), NullableTimeOfDayMessage.class, NullableTimeOfDayMessage::getTimeOfDayField);
        Map<String, Object> values = selectAllValues(chSchemaByStream.getRight()).get(0);
        int actualValue = (int) values.get(clickhouseColumn.getDbColumnName());

        systemRequiredFieldsCheck(message, values);
        assertEquals(expectedValue, actualValue);
    }

    @Test
    @Timeout(10)
    void readNullableTimeOfDayFromCH_expectedNullValue() {
        final int timeOfDayNull = TimeOfDayDataType.NULL;
        final Object expectedValue = null;

        NullableTimeOfDayMessage message = new NullableTimeOfDayMessage();
        message.setTimeOfDayField(timeOfDayNull);
        initSystemRequiredFields(message);

        Pair<DXTickStream, TableDeclaration> chSchemaByStream = loadAndReplicateData(message);
        ColumnDeclaration clickhouseColumn = getClickhouseColumn(chSchemaByStream.getLeft(), NullableTimeOfDayMessage.class, NullableTimeOfDayMessage::getTimeOfDayField);
        Map<String, Object> values = selectAllValues(chSchemaByStream.getRight()).get(0);
        Object actualValue = values.get(clickhouseColumn.getDbColumnName());

        systemRequiredFieldsCheck(message, values);
        assertEquals(expectedValue, actualValue);
    }

    // Tests for non nullable TimeOfDay data type

    @Timeout(10)
    @ParameterizedTest
    @ValueSource(
            strings = {"12:16:48.111", "12:16:48", "23:59:59.999", "00:00:00.000"}
    )
    void readNonNullableTimeOfDayFromCH_expectedNotNullValue(String argument) {
        final int expectedValue = toMillisOfDay(LocalTime.parse(argument));

        TimeOfDayMessage message = new TimeOfDayMessage();
        message.setTimeOfDayField(expectedValue);
        initSystemRequiredFields(message);

        Pair<DXTickStream, TableDeclaration> chSchemaByStream = loadAndReplicateData(message);
        ColumnDeclaration clickhouseColumn = getClickhouseColumn(chSchemaByStream.getLeft(), TimeOfDayMessage.class, TimeOfDayMessage::getTimeOfDayField);
        Map<String, Object> values = selectAllValues(chSchemaByStream.getRight()).get(0);
        int actualValue = (int) values.get(clickhouseColumn.getDbColumnName());

        systemRequiredFieldsCheck(message, values);
        assertEquals(expectedValue, actualValue);
    }
}

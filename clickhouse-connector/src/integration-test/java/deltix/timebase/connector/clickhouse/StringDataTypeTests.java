package deltix.timebase.connector.clickhouse;

import deltix.clickhouse.schema.ColumnDeclaration;
import deltix.clickhouse.schema.TableDeclaration;
import com.epam.deltix.qsrv.hf.tickdb.pub.DXTickStream;
import deltix.timebase.connector.clickhouse.timebase.NullableStringMessage;
import deltix.timebase.connector.clickhouse.timebase.StringMessage;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.sameInstance;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class StringDataTypeTests extends BaseStreamReplicatorTests {

    // tests for nullable String data type

    @Timeout(10)
    @ParameterizedTest
    @ValueSource(
            strings = {"value", "a b c", "", " "}
    )
    void readNullableStringFromCH_expectedNotEmptyValue(final String expectedValue) {
        NullableStringMessage message = new NullableStringMessage();
        message.setNullableStringField(expectedValue);
        initSystemRequiredFields(message);

        Pair<DXTickStream, TableDeclaration> chSchemaByStream = loadAndReplicateData(message);
        ColumnDeclaration clickhouseColumn = getClickhouseColumn(chSchemaByStream.getLeft(), NullableStringMessage.class, NullableStringMessage::getNullableStringField);
        Map<String, Object> values = selectAllValues(chSchemaByStream.getRight()).get(0);
        String actualValue = values.get(clickhouseColumn.getDbColumnName()).toString();

        systemRequiredFieldsCheck(message, values);
        assertEquals(expectedValue, actualValue);
    }

    @Test
    @Timeout(10)
    void readNullableStringFromCH_expectedNullValue() {
        final String expectedValue = null;

        NullableStringMessage message = new NullableStringMessage();
        message.setNullableStringField(expectedValue);
        initSystemRequiredFields(message);

        Pair<DXTickStream, TableDeclaration> chSchemaByStream = loadAndReplicateData(message);
        ColumnDeclaration clickhouseColumn = getClickhouseColumn(chSchemaByStream.getLeft(), NullableStringMessage.class, NullableStringMessage::getNullableStringField);
        Map<String, Object> values = selectAllValues(chSchemaByStream.getRight()).get(0);
        String actualValue = (String) values.get(clickhouseColumn.getDbColumnName());

        systemRequiredFieldsCheck(message, values);
        assertEquals(expectedValue, actualValue);
    }


    // tests for non nullable String data type

    @Timeout(10)
    @ParameterizedTest
    @ValueSource(
            strings = {"value", "a b c", "", " "}
    )
    void readNotNullableStringFromCH_expectedNotEmptyValue(final String expectedValue) {
        StringMessage message = new StringMessage();
        message.setStringField(expectedValue);
        initSystemRequiredFields(message);

        Pair<DXTickStream, TableDeclaration> chSchemaByStream = loadAndReplicateData(message);
        ColumnDeclaration clickhouseColumn = getClickhouseColumn(chSchemaByStream.getLeft(), StringMessage.class, StringMessage::getStringField);
        Map<String, Object> values = selectAllValues(chSchemaByStream.getRight()).get(0);
        String actualValue = (String) values.get(clickhouseColumn.getDbColumnName());

        systemRequiredFieldsCheck(message, values);
        assertEquals(expectedValue, actualValue);
    }

    @Test
    @Timeout(10)
    void writeNullToNonNullableStringInTB_expectedRuntimeException() {
        final String illegalValue = null;
        final String expectedErrorMessage = getFieldNotNullableMessage(StringMessage.class, StringMessage::getStringField);

        StringMessage message = new StringMessage();
        message.setStringField(illegalValue);
        initSystemRequiredFields(message);

        Exception exception = assertThrows(RuntimeException.class, () -> loadAndReplicateData(message));
        assertThat(exception.getCause().getClass(), sameInstance(IllegalArgumentException.class));
        assertEquals(expectedErrorMessage, exception.getCause().getMessage());
    }

}

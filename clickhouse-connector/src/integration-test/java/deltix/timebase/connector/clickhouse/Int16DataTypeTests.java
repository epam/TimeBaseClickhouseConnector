package deltix.timebase.connector.clickhouse;

import deltix.clickhouse.schema.ColumnDeclaration;
import deltix.clickhouse.schema.TableDeclaration;
import deltix.qsrv.hf.pub.md.IntegerDataType;
import deltix.qsrv.hf.tickdb.pub.DXTickStream;
import deltix.timebase.connector.clickhouse.timebase.Int16Message;
import deltix.timebase.connector.clickhouse.timebase.NullableInt16Message;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class Int16DataTypeTests extends BaseStreamReplicatorTests {

    // tests for nullable Int16 data type

    @Timeout(10)
    @ParameterizedTest
    @ValueSource(
            shorts = {0, 1, -1, 1000, Short.MAX_VALUE, Short.MIN_VALUE + 1}
    )
    void readNullableInt16FromCH_expectedNotNullValue(short expectedValue) {
        NullableInt16Message message = new NullableInt16Message();
        message.setNullableInt16Field(expectedValue);
        initSystemRequiredFields(message);

        Pair<DXTickStream, TableDeclaration> chSchemaByStream = loadAndReplicateData(message);
        ColumnDeclaration clickhouseColumn = getClickhouseColumn(chSchemaByStream.getLeft(), NullableInt16Message.class, NullableInt16Message::getNullableInt16Field);
        Map<String, Object> values = selectAllValues(chSchemaByStream.getRight()).get(0);
        short actualValue = (short) values.get(clickhouseColumn.getDbColumnName());

        systemRequiredFieldsCheck(message, values);
        assertEquals(expectedValue, actualValue);
    }

    @Test
    @Timeout(10)
    void readNullableInt16FromCH_expectedNullValue() {
        final short int16Null = IntegerDataType.INT16_NULL;
        final Object expectedValue = null;

        NullableInt16Message message = new NullableInt16Message();
        message.setNullableInt16Field(int16Null);
        initSystemRequiredFields(message);

        Pair<DXTickStream, TableDeclaration> chSchemaByStream = loadAndReplicateData(message);
        ColumnDeclaration clickhouseColumn = getClickhouseColumn(chSchemaByStream.getLeft(), NullableInt16Message.class, NullableInt16Message::getNullableInt16Field);
        Map<String, Object> values = selectAllValues(chSchemaByStream.getRight()).get(0);
        Short actualValue = (Short) values.get(clickhouseColumn.getDbColumnName());

        systemRequiredFieldsCheck(message, values);
        assertEquals(expectedValue, actualValue);
    }

    // tests for non nullable Int16 data type

    @Timeout(10)
    @ParameterizedTest
    @ValueSource(
            shorts = {0, 1, -1, 1000, Short.MAX_VALUE, Short.MIN_VALUE + 1}
    )
    void readNonNullableInt16FromCH_expectedNotNullValue(short expectedValue) {
        Int16Message message = new Int16Message();
        message.setInt16Field(expectedValue);
        initSystemRequiredFields(message);

        Pair<DXTickStream, TableDeclaration> chSchemaByStream = loadAndReplicateData(message);
        ColumnDeclaration clickhouseColumn = getClickhouseColumn(chSchemaByStream.getLeft(), Int16Message.class, Int16Message::getInt16Field);
        Map<String, Object> values = selectAllValues(chSchemaByStream.getRight()).get(0);
        short actualValue = (short) values.get(clickhouseColumn.getDbColumnName());

        systemRequiredFieldsCheck(message, values);
        assertEquals(expectedValue, actualValue);
    }
}

package deltix.timebase.connector.clickhouse;

import deltix.clickhouse.schema.ColumnDeclaration;
import deltix.clickhouse.schema.TableDeclaration;
import com.epam.deltix.qsrv.hf.pub.md.IntegerDataType;
import com.epam.deltix.qsrv.hf.tickdb.pub.DXTickStream;
import deltix.timebase.connector.clickhouse.timebase.Int32Message;
import deltix.timebase.connector.clickhouse.timebase.NullableInt32Message;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class Int32DataTypeTests extends BaseStreamReplicatorTests {

    // tests for nullable Int32 data type

    @Timeout(10)
    @ParameterizedTest
    @ValueSource(
            ints = {0, 1, -1, 100_000, Integer.MAX_VALUE, Integer.MIN_VALUE + 1}
    )
    void readNullableInt32FromCH_expectedNotNullValue(int expectedValue) {
        NullableInt32Message message = new NullableInt32Message();
        message.setNullableInt32Field(expectedValue);
        initSystemRequiredFields(message);

        Pair<DXTickStream, TableDeclaration> chSchemaByStream = loadAndReplicateData(message);
        ColumnDeclaration clickhouseColumn = getClickhouseColumn(chSchemaByStream.getLeft(), NullableInt32Message.class, NullableInt32Message::getNullableInt32Field);
        Map<String, Object> values = selectAllValues(chSchemaByStream.getRight()).get(0);
        int actualValue = (int) values.get(clickhouseColumn.getDbColumnName());

        systemRequiredFieldsCheck(message, values);
        assertEquals(expectedValue, actualValue);
    }

    @Test
    @Timeout(10)
    void readNullableInt32FromCH_expectedNullValue() {
        final int int32Null = IntegerDataType.INT32_NULL;
        final Object expectedValue = null;

        NullableInt32Message message = new NullableInt32Message();
        message.setNullableInt32Field(int32Null);
        initSystemRequiredFields(message);

        Pair<DXTickStream, TableDeclaration> chSchemaByStream = loadAndReplicateData(message);
        ColumnDeclaration clickhouseColumn = getClickhouseColumn(chSchemaByStream.getLeft(), NullableInt32Message.class, NullableInt32Message::getNullableInt32Field);
        Map<String, Object> values = selectAllValues(chSchemaByStream.getRight()).get(0);
        Integer actualValue = (Integer) values.get(clickhouseColumn.getDbColumnName());

        systemRequiredFieldsCheck(message, values);
        assertEquals(expectedValue, actualValue);
    }

    // tests for non nullable Int32 data type

    @Timeout(10)
    @ParameterizedTest
    @ValueSource(
            ints = {0, 1, -1, 100_000, Integer.MAX_VALUE, Integer.MIN_VALUE + 1}
    )
    void readNonNullableInt32FromCH_expectedNotNullValue(int expectedValue) {
        Int32Message message = new Int32Message();
        message.setInt32Field(expectedValue);
        initSystemRequiredFields(message);

        Pair<DXTickStream, TableDeclaration> chSchemaByStream = loadAndReplicateData(message);
        ColumnDeclaration clickhouseColumn = getClickhouseColumn(chSchemaByStream.getLeft(), Int32Message.class, Int32Message::getInt32Field);
        Map<String, Object> values = selectAllValues(chSchemaByStream.getRight()).get(0);
        int actualValue = (int) values.get(clickhouseColumn.getDbColumnName());

        systemRequiredFieldsCheck(message, values);
        assertEquals(expectedValue, actualValue);
    }
}

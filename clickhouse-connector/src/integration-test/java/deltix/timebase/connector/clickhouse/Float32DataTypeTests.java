package deltix.timebase.connector.clickhouse;

import deltix.clickhouse.schema.ColumnDeclaration;
import deltix.clickhouse.schema.TableDeclaration;
import com.epam.deltix.qsrv.hf.pub.md.FloatDataType;
import com.epam.deltix.qsrv.hf.tickdb.pub.DXTickStream;
import deltix.timebase.connector.clickhouse.timebase.Float32Message;
import deltix.timebase.connector.clickhouse.timebase.NullableFloat32Message;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class Float32DataTypeTests extends BaseStreamReplicatorTests {

    // tests for nullable Float32 data type

    @Timeout(10)
    @ParameterizedTest
    @ValueSource(
            floats = {0.0f, 1.0f, -1.0f, 100.123f, Float.MAX_VALUE, Float.MIN_VALUE, Float.POSITIVE_INFINITY, Float.NEGATIVE_INFINITY}
    )
    void readNullableFloat32FromCH_expectedNotNullValue(float expectedValue) {
        NullableFloat32Message message = new NullableFloat32Message();
        message.setNullableFloat32Field(expectedValue);
        initSystemRequiredFields(message);

        Pair<DXTickStream, TableDeclaration> chSchemaByStream = loadAndReplicateData(message);
        ColumnDeclaration clickhouseColumn = getClickhouseColumn(chSchemaByStream.getLeft(), NullableFloat32Message.class, NullableFloat32Message::getNullableFloat32Field);
        Map<String, Object> values = selectAllValues(chSchemaByStream.getRight()).get(0);
        float actualValue = (float) values.get(clickhouseColumn.getDbColumnName());

        systemRequiredFieldsCheck(message, values);
        assertEquals(expectedValue, actualValue);
    }

    @Test
    @Timeout(10)
    void readNullableFloat32FromCH_expectedNullValue() {
        final float floatNull = FloatDataType.IEEE32_NULL;
        final Object expectedValue = null;

        NullableFloat32Message message = new NullableFloat32Message();
        message.setNullableFloat32Field(floatNull);
        initSystemRequiredFields(message);

        Pair<DXTickStream, TableDeclaration> chSchemaByStream = loadAndReplicateData(message);
        ColumnDeclaration clickhouseColumn = getClickhouseColumn(chSchemaByStream.getLeft(), NullableFloat32Message.class, NullableFloat32Message::getNullableFloat32Field);
        Map<String, Object> values = selectAllValues(chSchemaByStream.getRight()).get(0);
        Float actualValue = (Float) values.get(clickhouseColumn.getDbColumnName());

        systemRequiredFieldsCheck(message, values);
        assertEquals(expectedValue, actualValue);
    }

    // tests for non nullable Float32 data type

    @Timeout(10)
    @ParameterizedTest
    @ValueSource(
            floats = {0.0f, 1.0f, -1.0f, 100.123f, Float.MAX_VALUE, Float.MIN_VALUE, Float.POSITIVE_INFINITY, Float.NEGATIVE_INFINITY}
    )
    void readNonNullableFloat32FromCH_expectedNotNullValue(float expectedValue) {
        Float32Message message = new Float32Message();
        message.setFloat32Field(expectedValue);
        initSystemRequiredFields(message);

        Pair<DXTickStream, TableDeclaration> chSchemaByStream = loadAndReplicateData(message);
        ColumnDeclaration clickhouseColumn = getClickhouseColumn(chSchemaByStream.getLeft(), Float32Message.class, Float32Message::getFloat32Field);
        Map<String, Object> values = selectAllValues(chSchemaByStream.getRight()).get(0);
        float actualValue = (float) values.get(clickhouseColumn.getDbColumnName());

        systemRequiredFieldsCheck(message, values);
        assertEquals(expectedValue, actualValue);
    }
}

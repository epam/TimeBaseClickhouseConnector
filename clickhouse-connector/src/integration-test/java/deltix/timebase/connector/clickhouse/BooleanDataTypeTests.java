package deltix.timebase.connector.clickhouse;

import deltix.clickhouse.schema.ColumnDeclaration;
import deltix.clickhouse.schema.TableDeclaration;
import com.epam.deltix.qsrv.hf.tickdb.pub.DXTickStream;
import deltix.timebase.connector.clickhouse.timebase.BooleanMessage;
import deltix.timebase.connector.clickhouse.timebase.NullableBooleanMessage;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class BooleanDataTypeTests extends BaseStreamReplicatorTests {

    // Tests for nullable Boolean data type

    @Timeout(10)
    @ParameterizedTest
    @ValueSource(
            booleans = {true, false}
    )
    void readNullableBooleanFromCH_expectedNotNullValue(boolean expectedValue) {
        NullableBooleanMessage message = new NullableBooleanMessage();
        message.setNullableBooleanField(expectedValue);
        initSystemRequiredFields(message);

        Pair<DXTickStream, TableDeclaration> chSchemaByStream = loadAndReplicateData(message);
        ColumnDeclaration clickhouseColumn = getClickhouseColumn(chSchemaByStream.getLeft(), NullableBooleanMessage.class, NullableBooleanMessage::isNullableBooleanField);
        Map<String, Object> values = selectAllValues(chSchemaByStream.getRight()).get(0);
        boolean actualValue = (boolean) values.get(clickhouseColumn.getDbColumnName());

        systemRequiredFieldsCheck(message, values);
        assertEquals(expectedValue, actualValue);
    }

    @Test
    @Timeout(10)
    void readNullableBooleanFromCH_expectedNullValue() {
        final Object expectedValue = null;

        NullableBooleanMessage message = new NullableBooleanMessage();
        message.nullifyNullableBooleanField();
        initSystemRequiredFields(message);

        Pair<DXTickStream, TableDeclaration> chSchemaByStream = loadAndReplicateData(message);
        ColumnDeclaration clickhouseColumn = getClickhouseColumn(chSchemaByStream.getLeft(), NullableBooleanMessage.class, NullableBooleanMessage::isNullableBooleanField);
        Map<String, Object> values = selectAllValues(chSchemaByStream.getRight()).get(0);
        Object actualValue = values.get(clickhouseColumn.getDbColumnName());

        systemRequiredFieldsCheck(message, values);
        assertEquals(expectedValue, actualValue);
    }

    // Tests for non nullable Boolean data type

    @Timeout(10)
    @ParameterizedTest
    @ValueSource(
            booleans = {true, false}
    )
    void readNonNullableBooleanFromCH_expectedNotNullValue(boolean expectedValue) {
        BooleanMessage message = new BooleanMessage();
        message.setBooleanField(expectedValue);
        initSystemRequiredFields(message);

        Pair<DXTickStream, TableDeclaration> chSchemaByStream = loadAndReplicateData(message);
        ColumnDeclaration clickhouseColumn = getClickhouseColumn(chSchemaByStream.getLeft(), BooleanMessage.class, BooleanMessage::isBooleanField);
        Map<String, Object> values = selectAllValues(chSchemaByStream.getRight()).get(0);
        boolean actualValue = (boolean) values.get(clickhouseColumn.getDbColumnName());

        systemRequiredFieldsCheck(message, values);
        assertEquals(expectedValue, actualValue);
    }
}

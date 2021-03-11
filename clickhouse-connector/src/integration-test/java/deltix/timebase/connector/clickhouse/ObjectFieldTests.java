package deltix.timebase.connector.clickhouse;

import deltix.clickhouse.schema.ColumnDeclaration;
import deltix.clickhouse.schema.TableDeclaration;
import deltix.qsrv.hf.pub.ExchangeCodec;
import deltix.qsrv.hf.tickdb.pub.DXTickStream;
import deltix.timebase.connector.clickhouse.timebase.*;
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

public class ObjectFieldTests extends BaseStreamReplicatorTests {
    private static final int MESSAGE_COUNT = 10;

    // tests for nullable String data type

    private static BestBidOfferTestMessage generateBBO(int value) {
        final BestBidOfferTestMessage message = new BestBidOfferTestMessage();
        initSystemRequiredFields(message);
        message.setSequenceNumber(value);
        message.setBidPrice(value);
        message.setBidSize(value);
        message.setBidNumOfOrders(value);
        message.setBidQuoteId(String.valueOf(value));
        message.setBidExchangeId(ExchangeCodec.codeToLong("840"));
        message.setOfferPrice(MESSAGE_COUNT - value);
        message.setOfferSize(MESSAGE_COUNT - value);
        message.setOfferNumOfOrders(MESSAGE_COUNT - value);
        message.setOfferQuoteId(String.valueOf(MESSAGE_COUNT - value));
        message.setOfferExchangeId(ExchangeCodec.codeToLong("840"));
        return message;
    }

    private static TradeTestMessage generateTrade(int value) {
        final TradeTestMessage message = new TradeTestMessage();
        initSystemRequiredFields(message);
        message.setSequenceNumber(value);
        message.setPrice(value);
        message.setSize(value);
        message.setExchangeId(ExchangeCodec.codeToLong("840"));
        message.setCondition(String.valueOf(value));
        return message;
    }

    @ParameterizedTest
    @ValueSource(
            strings = {"value"/*, "a b c", "", " "*/}
    )
    void readNullableOneLevelObject(final String expectedValue) {
        ObjectFieldMessage message = new ObjectFieldMessage();
        message.setStringField(expectedValue);
        message.setBboField(generateBBO(1));
        message.setTradeField(generateTrade(2));
        message.setMarketField(generateBBO(3));
        message.setMarketField(generateTrade(4));
        initSystemRequiredFields(message);

        Pair<DXTickStream, TableDeclaration> chSchemaByStream = loadAndReplicateData(message, ObjectFieldMessage.class, ObjectField2Message.class, TradeTestMessage.class, BestBidOfferTestMessage.class);
//        ColumnDeclaration clickhouseColumn = getClickhouseColumn(chSchemaByStream.getLeft(), ObjectFieldMessage.class, ObjectFieldMessage::getStringField);
//        Map<String, Object> values = selectAllValues(chSchemaByStream.getRight()).get(0);
//        String actualValue = values.get(clickhouseColumn.getDbColumnName()).toString();
//
//        systemRequiredFieldsCheck(message, values);
//        assertEquals(expectedValue, actualValue);
    }

    @ParameterizedTest
    @ValueSource(
            strings = {"value"/*, "a b c", "", " "*/}
    )
    void readNullableTwoLevelObject(final String expectedValue) {
        ObjectFieldMessage objectValue = new ObjectFieldMessage();
        objectValue.setStringField(expectedValue);
        objectValue.setBboField(generateBBO(1));
        objectValue.setTradeField(generateTrade(2));
        objectValue.setMarketField(generateBBO(3));
        objectValue.setMarketField(generateTrade(4));
        initSystemRequiredFields(objectValue);

        ObjectField2Message message = new ObjectField2Message();
        message.setObjectField(objectValue);
        initSystemRequiredFields(message);


        Pair<DXTickStream, TableDeclaration> chSchemaByStream = loadAndReplicateData(message, ObjectFieldMessage.class, ObjectField2Message.class, TradeTestMessage.class, BestBidOfferTestMessage.class);
//        ColumnDeclaration clickhouseColumn = getClickhouseColumn(chSchemaByStream.getLeft(), ObjectField2Message.class, ObjectField2Message::getObjectField);
//        Map<String, Object> values = selectAllValues(chSchemaByStream.getRight()).get(0);
//        String actualValue = values.get(clickhouseColumn.getDbColumnName()).toString();
//
//        systemRequiredFieldsCheck(objectValue, values);
//        assertEquals(expectedValue, actualValue);
    }
}

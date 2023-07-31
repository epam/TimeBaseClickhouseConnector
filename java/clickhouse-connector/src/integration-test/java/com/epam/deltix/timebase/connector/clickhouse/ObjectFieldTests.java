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

import com.epam.deltix.clickhouse.schema.ColumnDeclaration;
import com.epam.deltix.clickhouse.schema.TableDeclaration;
import com.epam.deltix.qsrv.hf.tickdb.pub.DXTickStream;
import com.epam.deltix.timebase.connector.clickhouse.timebase.BestBidOfferTestMessage;
import com.epam.deltix.timebase.connector.clickhouse.timebase.ObjectField2Message;
import com.epam.deltix.timebase.connector.clickhouse.timebase.ObjectFieldMessage;
import com.epam.deltix.timebase.connector.clickhouse.timebase.TradeTestMessage;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.lang.reflect.InvocationTargetException;
import java.util.Map;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class ObjectFieldTests extends BaseStreamReplicatorTests {
    private static final int MESSAGE_COUNT = 10;

    // tests for nullable String data type

    private static BestBidOfferTestMessage generateBBO(int value) {
        final BestBidOfferTestMessage message = new BestBidOfferTestMessage();
        initSystemRequiredFields(message);
        message.setSequenceNumber(value);
        message.setBidNumOfOrders(value);
        message.setBidQuoteId(String.valueOf(value));
        message.setOfferPrice(MESSAGE_COUNT + value);
        message.setOfferSize(MESSAGE_COUNT + value);
        message.setOfferNumOfOrders(MESSAGE_COUNT + value);
        message.setOfferQuoteId(String.valueOf(MESSAGE_COUNT + value));
        message.setIsNational((byte) (value > 0 ? 1 : 0));
        return message;
    }

    private static TradeTestMessage generateTrade(int value) {
        final TradeTestMessage message = new TradeTestMessage();
        initSystemRequiredFields(message);
        message.setSequenceNumber(value);
        message.setPrice(value);
        message.setSize(value);
        message.setCondition(String.valueOf(value));
        return message;
    }

    @ParameterizedTest
    @MethodSource("objectsSource")
    void readNullableOneLevelObject(final String expectedValue, BestBidOfferTestMessage bboField, TradeTestMessage tradeField) throws InvocationTargetException, IllegalAccessException {
        ObjectFieldMessage message = new ObjectFieldMessage();
        message.setStringField(expectedValue);
        message.setBboField(bboField);
        message.setTradeField(tradeField);
        initSystemRequiredFields(message);

        Pair<DXTickStream, TableDeclaration> chSchemaByStream = loadAndReplicateData(message, ObjectFieldMessage.class, TradeTestMessage.class, BestBidOfferTestMessage.class);
        ColumnDeclaration clickhouseColumn = getClickhouseColumn(chSchemaByStream.getLeft(), ObjectFieldMessage.class, ObjectFieldMessage::getStringField);
        Map<String, Object> values = selectAllValues(chSchemaByStream.getRight()).get(0);
        String actualValue = values.get(clickhouseColumn.getDbColumnName()).toString();

        assertEqualsObjectFields(values, Map.of("bboField", bboField, "tradeField", tradeField));
        systemRequiredFieldsCheck(message, values);
        assertEquals(expectedValue, actualValue);
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

    public Stream<Arguments> objectsSource() {
        return Stream.of(
                Arguments.of("val", generateBBO(123), generateTrade(321)),
                Arguments.of("", generateBBO(5), generateTrade(2).nullify())
        );
    }
}
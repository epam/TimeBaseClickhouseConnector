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
import com.epam.deltix.dfp.Decimal64Utils;
import com.epam.deltix.qsrv.hf.tickdb.pub.DXTickStream;
import com.epam.deltix.timebase.connector.clickhouse.timebase.Decimal64Message;
import com.epam.deltix.timebase.connector.clickhouse.timebase.NullableDecimal64Message;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.math.BigDecimal;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class Decimal64DataTypeTests extends BaseStreamReplicatorTests {

    // Tests for nullable Decimal64 data type

    @Timeout(10)
    @ParameterizedTest
    @ValueSource(
            strings = {"1", "1.123456789123", "100"}
    )
    void readNullableDecimal64FromCH_expectedNotNullValue(String argument) {
        final long expectedValue = Decimal64Utils.parse(argument);

        NullableDecimal64Message message = new NullableDecimal64Message();
        message.setDecimal64Field(expectedValue);
        initSystemRequiredFields(message);

        Pair<DXTickStream, TableDeclaration> chSchemaByStream = loadAndReplicateData(message);
        ColumnDeclaration clickhouseColumn = getClickhouseColumn(chSchemaByStream.getLeft(), NullableDecimal64Message.class, NullableDecimal64Message::getDecimal64Field);
        Map<String, Object> values = selectAllValues(chSchemaByStream.getRight()).get(0);
        long actualValue = getDecimal64Value((BigDecimal) values.get(clickhouseColumn.getDbColumnName()));

        systemRequiredFieldsCheck(message, values);
        assertTrue(Decimal64Utils.equals(expectedValue, actualValue));
    }

    @Test
    @Timeout(10)
    void readNullableDecimal64ZeroFromCH_expectedZeroValue() {
        final long expectedValue = Decimal64Utils.ZERO;

        NullableDecimal64Message message = new NullableDecimal64Message();
        message.setDecimal64Field(expectedValue);
        initSystemRequiredFields(message);

        Pair<DXTickStream, TableDeclaration> chSchemaByStream = loadAndReplicateData(message);
        ColumnDeclaration clickhouseColumn = getClickhouseColumn(chSchemaByStream.getLeft(), NullableDecimal64Message.class, NullableDecimal64Message::getDecimal64Field);
        Map<String, Object> values = selectAllValues(chSchemaByStream.getRight()).get(0);
        long actualValue = getDecimal64Value((BigDecimal) values.get(clickhouseColumn.getDbColumnName()));

        systemRequiredFieldsCheck(message, values);
        assertTrue(Decimal64Utils.equals(expectedValue, actualValue));
    }

    @Timeout(10)
    @ParameterizedTest
    @ValueSource(
            longs = {Decimal64Utils.MAX_VALUE, Decimal64Utils.POSITIVE_INFINITY}
    )
    void readNullableDecimal64MaxValueAndPositiveInfinityFromCH_expectedClickHouseMaxValue(long argument) {
        final BigDecimal expectedValue = new BigDecimal("99999999999999999999999999.999999999999");

        NullableDecimal64Message message = new NullableDecimal64Message();
        message.setDecimal64Field(argument);
        initSystemRequiredFields(message);

        Pair<DXTickStream, TableDeclaration> chSchemaByStream = loadAndReplicateData(message);
        ColumnDeclaration clickhouseColumn = getClickhouseColumn(chSchemaByStream.getLeft(), NullableDecimal64Message.class, NullableDecimal64Message::getDecimal64Field);
        Map<String, Object> values = selectAllValues(chSchemaByStream.getRight()).get(0);
        BigDecimal actualValue = (BigDecimal) values.get(clickhouseColumn.getDbColumnName());

        systemRequiredFieldsCheck(message, values);
        assertEquals(expectedValue, actualValue);
    }

    @Timeout(10)
    @ParameterizedTest
    @ValueSource(
            longs = {Decimal64Utils.MIN_VALUE, Decimal64Utils.NEGATIVE_INFINITY}
    )
    void readNullableDecimal64MinValueAndNegativeInfinityFromCH_expectedClickHouseMinValue(long argument) {
        final BigDecimal expectedValue = new BigDecimal("-99999999999999999999999999.999999999999");

        NullableDecimal64Message message = new NullableDecimal64Message();
        message.setDecimal64Field(argument);
        initSystemRequiredFields(message);

        Pair<DXTickStream, TableDeclaration> chSchemaByStream = loadAndReplicateData(message);
        ColumnDeclaration clickhouseColumn = getClickhouseColumn(chSchemaByStream.getLeft(), NullableDecimal64Message.class, NullableDecimal64Message::getDecimal64Field);
        Map<String, Object> values = selectAllValues(chSchemaByStream.getRight()).get(0);
        BigDecimal actualValue = (BigDecimal) values.get(clickhouseColumn.getDbColumnName());

        systemRequiredFieldsCheck(message, values);
        assertEquals(expectedValue, actualValue);
    }

    @Test
    @Timeout(10)
    void readNullableDecimal64FromCH_expectedNullValue() {
        final Object expectedValue = null;
        final long decimal64Null = Decimal64Utils.NULL;

        NullableDecimal64Message message = new NullableDecimal64Message();
        message.setDecimal64Field(decimal64Null);
        initSystemRequiredFields(message);

        Pair<DXTickStream, TableDeclaration> chSchemaByStream = loadAndReplicateData(message);
        ColumnDeclaration clickhouseColumn = getClickhouseColumn(chSchemaByStream.getLeft(), NullableDecimal64Message.class, NullableDecimal64Message::getDecimal64Field);
        Map<String, Object> values = selectAllValues(chSchemaByStream.getRight()).get(0);
        Object actualValue =  values.get(clickhouseColumn.getDbColumnName());

        systemRequiredFieldsCheck(message, values);
        assertEquals(expectedValue, actualValue);
    }

    // Tests for non nullable Decimal64 data type

    @Timeout(10)
    @ParameterizedTest
    @ValueSource(
            strings = {"1", "1.123456789123", "100"}
    )
    void readNonNullableDecimal64FromCH_expectedNotNullValue(String argument) {
        final long expectedValue = Decimal64Utils.parse(argument);

        Decimal64Message message = new Decimal64Message();
        message.setDecimal64Field(expectedValue);
        initSystemRequiredFields(message);

        Pair<DXTickStream, TableDeclaration> chSchemaByStream = loadAndReplicateData(message);
        ColumnDeclaration clickhouseColumn = getClickhouseColumn(chSchemaByStream.getLeft(), Decimal64Message.class, Decimal64Message::getDecimal64Field);
        Map<String, Object> values = selectAllValues(chSchemaByStream.getRight()).get(0);
        long actualValue = getDecimal64Value((BigDecimal) values.get(clickhouseColumn.getDbColumnName()));

        systemRequiredFieldsCheck(message, values);
        assertTrue(Decimal64Utils.equals(expectedValue, actualValue));
    }
}
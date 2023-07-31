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
import com.epam.deltix.qsrv.hf.pub.md.FloatDataType;
import com.epam.deltix.qsrv.hf.tickdb.pub.DXTickStream;
import com.epam.deltix.timebase.connector.clickhouse.timebase.Float64Message;
import com.epam.deltix.timebase.connector.clickhouse.timebase.NullableFloat64Message;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class Float64DataTypeTests extends BaseStreamReplicatorTests {

    // @PH: Double.MIN_VALUE as expected test value is temporarily not used because there is no way to enable "Precise" parser for floats in clickhouse.
    // More details about clickhouse parsers and issue about it: https://github.com/ClickHouse/ClickHouse/issues/4819

    // tests for nullable Float64 data type

    @Timeout(10)
    @ParameterizedTest
    @ValueSource(
            doubles = {0.0d, 1.0d, -1.0d, 100.123456789d, Double.MAX_VALUE, Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY}
    )
    void readNullableFloat64FromCH_expectedNotNullValue(final double expectedValue) {
        NullableFloat64Message message = new NullableFloat64Message();
        message.setNullableFloat64Field(expectedValue);
        initSystemRequiredFields(message);

        Pair<DXTickStream, TableDeclaration> chSchemaByStream = loadAndReplicateData(message);
        ColumnDeclaration clickhouseColumn = getClickhouseColumn(chSchemaByStream.getLeft(), NullableFloat64Message.class, NullableFloat64Message::getNullableFloat64Field);
        Map<String, Object> values = selectAllValues(chSchemaByStream.getRight()).get(0);
        double actualValue = (double) values.get(clickhouseColumn.getDbColumnName());

        systemRequiredFieldsCheck(message, values);
        assertEquals(expectedValue, actualValue);
    }

    @Test
    @Timeout(10)
    void readNullableFloat64FromCH_expectedNullValue() {
        final double doubleNull = FloatDataType.IEEE64_NULL;
        final Object expectedValue = null;

        NullableFloat64Message message = new NullableFloat64Message();
        message.setNullableFloat64Field(doubleNull);
        initSystemRequiredFields(message);

        Pair<DXTickStream, TableDeclaration> chSchemaByStream = loadAndReplicateData(message);
        ColumnDeclaration clickhouseColumn = getClickhouseColumn(chSchemaByStream.getLeft(), NullableFloat64Message.class, NullableFloat64Message::getNullableFloat64Field);
        Map<String, Object> values = selectAllValues(chSchemaByStream.getRight()).get(0);
        Double actualValue = (Double) values.get(clickhouseColumn.getDbColumnName());

        systemRequiredFieldsCheck(message, values);
        assertEquals(expectedValue, actualValue);
    }

    // tests for non nullable Float64 data type

    @Timeout(10)
    @ParameterizedTest
    @ValueSource(
            doubles = {0.0d, 1.0d, -1.0d, 100.123456789d, Double.MAX_VALUE, Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY}
    )
    void readNonNullableFloat64FromCH_expectedNotNullValue(final double expectedValue) {
        Float64Message message = new Float64Message();
        message.setFloat64Field(expectedValue);
        initSystemRequiredFields(message);

        Pair<DXTickStream, TableDeclaration> chSchemaByStream = loadAndReplicateData(message);
        ColumnDeclaration clickhouseColumn = getClickhouseColumn(chSchemaByStream.getLeft(), Float64Message.class, Float64Message::getFloat64Field);
        Map<String, Object> values = selectAllValues(chSchemaByStream.getRight()).get(0);
        double actualValue = (double) values.get(clickhouseColumn.getDbColumnName());

        systemRequiredFieldsCheck(message, values);
        assertEquals(expectedValue, actualValue);
    }
}
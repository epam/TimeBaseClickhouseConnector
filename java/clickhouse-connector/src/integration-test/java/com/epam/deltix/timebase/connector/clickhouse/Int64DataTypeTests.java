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
import com.epam.deltix.qsrv.hf.pub.md.IntegerDataType;
import com.epam.deltix.qsrv.hf.tickdb.pub.DXTickStream;
import com.epam.deltix.timebase.connector.clickhouse.timebase.Int64Message;
import com.epam.deltix.timebase.connector.clickhouse.timebase.NullableInt64Message;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class Int64DataTypeTests extends BaseStreamReplicatorTests {

    // tests for nullable Int64 data type

    @Timeout(10)
    @ParameterizedTest
    @ValueSource(
            longs = {0, 1, -1, 100_000_000_000L, Long.MAX_VALUE, Long.MIN_VALUE + 1}
    )
    void readNullableInt64FromCH_expectedNotNullValue(long expectedValue) {
        NullableInt64Message message = new NullableInt64Message();
        message.setNullableInt64Field(expectedValue);
        initSystemRequiredFields(message);

        Pair<DXTickStream, TableDeclaration> chSchemaByStream = loadAndReplicateData(message);
        ColumnDeclaration clickhouseColumn = getClickhouseColumn(chSchemaByStream.getLeft(), NullableInt64Message.class, NullableInt64Message::getNullableInt64Field);
        Map<String, Object> values = selectAllValues(chSchemaByStream.getRight()).get(0);
        long actualValue = (long) values.get(clickhouseColumn.getDbColumnName());

        systemRequiredFieldsCheck(message, values);
        assertEquals(expectedValue, actualValue);
    }

    @Test
    @Timeout(10)
    void readNullableInt64FromCH_expectedNullValue() {
        final long int64Null = IntegerDataType.INT64_NULL;
        final Object expectedValue = null;

        NullableInt64Message message = new NullableInt64Message();
        message.setNullableInt64Field(int64Null);
        initSystemRequiredFields(message);

        Pair<DXTickStream, TableDeclaration> chSchemaByStream = loadAndReplicateData(message);
        ColumnDeclaration clickhouseColumn = getClickhouseColumn(chSchemaByStream.getLeft(), NullableInt64Message.class, NullableInt64Message::getNullableInt64Field);
        Map<String, Object> values = selectAllValues(chSchemaByStream.getRight()).get(0);
        Object actualValue = values.get(clickhouseColumn.getDbColumnName());

        systemRequiredFieldsCheck(message, values);
        assertEquals(expectedValue, actualValue);
    }

    // tests for non nullable Int64 data type

    @Timeout(10)
    @ParameterizedTest
    @ValueSource(
            longs = {0, 1, -1, 100_000_000_000L, Long.MAX_VALUE, Long.MIN_VALUE + 1}
    )
    void readNonNullableInt64FromCH_expectedNotNullValue(long expectedValue) {
        Int64Message message = new Int64Message();
        message.setInt64Field(expectedValue);
        initSystemRequiredFields(message);

        Pair<DXTickStream, TableDeclaration> chSchemaByStream = loadAndReplicateData(message);
        ColumnDeclaration clickhouseColumn = getClickhouseColumn(chSchemaByStream.getLeft(), Int64Message.class, Int64Message::getInt64Field);
        Map<String, Object> values = selectAllValues(chSchemaByStream.getRight()).get(0);
        long actualValue = (long) values.get(clickhouseColumn.getDbColumnName());

        systemRequiredFieldsCheck(message, values);
        assertEquals(expectedValue, actualValue);
    }
}
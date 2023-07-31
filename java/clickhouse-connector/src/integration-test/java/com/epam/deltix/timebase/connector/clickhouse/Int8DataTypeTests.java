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
import com.epam.deltix.timebase.connector.clickhouse.timebase.Int8Message;
import com.epam.deltix.timebase.connector.clickhouse.timebase.NullableInt8Message;
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

public class Int8DataTypeTests extends BaseStreamReplicatorTests {

    // tests for nullable Int8 data type

    @Timeout(10)
    @ParameterizedTest
    @ValueSource(
            bytes = {0, 1, -1, 100, Byte.MAX_VALUE, Byte.MIN_VALUE + 1}
    )
    void readNullableInt8FromCH_expectedNotNullValue(byte expectedValue) {
        NullableInt8Message message = new NullableInt8Message();
        message.setNullableInt8Field(expectedValue);
        initSystemRequiredFields(message);

        Pair<DXTickStream, TableDeclaration> chSchemaByStream = loadAndReplicateData(message);
        ColumnDeclaration clickhouseColumn = getClickhouseColumn(chSchemaByStream.getLeft(), NullableInt8Message.class, NullableInt8Message::getNullableInt8Field);
        Map<String, Object> values = selectAllValues(chSchemaByStream.getRight()).get(0);
        byte actualValue = (byte) values.get(clickhouseColumn.getDbColumnName());

        systemRequiredFieldsCheck(message, values);
        assertEquals(expectedValue, actualValue);
    }

    @Test
    @Timeout(10)
    void readNullableInt8FromCH_expectedNullValue() {
        final byte int8Null = IntegerDataType.INT8_NULL;
        final Object expectedValue = null;

        NullableInt8Message message = new NullableInt8Message();
        message.setNullableInt8Field(int8Null);
        initSystemRequiredFields(message);

        Pair<DXTickStream, TableDeclaration> chSchemaByStream = loadAndReplicateData(message);
        ColumnDeclaration clickhouseColumn = getClickhouseColumn(chSchemaByStream.getLeft(), NullableInt8Message.class, NullableInt8Message::getNullableInt8Field);
        Map<String, Object> values = selectAllValues(chSchemaByStream.getRight()).get(0);
        Object actualValue = values.get(clickhouseColumn.getDbColumnName());

        systemRequiredFieldsCheck(message, values);
        assertEquals(expectedValue, actualValue);
    }

    @Test
    @Timeout(10)
    void readNullableMinInt8FromCH_expectedNullValue() {
        final byte int8MinValue = Byte.MIN_VALUE;
        final Object expectedValue = null;

        NullableInt8Message message = new NullableInt8Message();
        message.setNullableInt8Field(int8MinValue);
        initSystemRequiredFields(message);

        Pair<DXTickStream, TableDeclaration> chSchemaByStream = loadAndReplicateData(message);
        ColumnDeclaration clickhouseColumn = getClickhouseColumn(chSchemaByStream.getLeft(), NullableInt8Message.class, NullableInt8Message::getNullableInt8Field);
        Map<String, Object> values = selectAllValues(chSchemaByStream.getRight()).get(0);
        Object actualValue = values.get(clickhouseColumn.getDbColumnName());

        systemRequiredFieldsCheck(message, values);
        assertEquals(expectedValue, actualValue);
    }


    // tests for non nullable Int8 data type

    @Timeout(10)
    @ParameterizedTest
    @ValueSource(
            bytes = {0, 1, -1, 100, Byte.MAX_VALUE, Byte.MIN_VALUE + 1}
    )
    void readNonNullableInt8FromCH_expectedNotNullValue(byte expectedValue) {
        Int8Message message = new Int8Message();
        message.setInt8Field(expectedValue);
        initSystemRequiredFields(message);

        Pair<DXTickStream, TableDeclaration> chSchemaByStream = loadAndReplicateData(message);
        ColumnDeclaration clickhouseColumn = getClickhouseColumn(chSchemaByStream.getLeft(), Int8Message.class, Int8Message::getInt8Field);
        Map<String, Object> values = selectAllValues(chSchemaByStream.getRight()).get(0);
        byte actualValue = (byte) values.get(clickhouseColumn.getDbColumnName());

        systemRequiredFieldsCheck(message, values);
        assertEquals(expectedValue, actualValue);
    }

    @Test
    @Timeout(10)
    void writeNullToNonNullableInt8InTB_expectedRuntimeException() {
        final byte illegalValue = IntegerDataType.INT8_NULL;
        final String expectedErrorMessage = getFieldNotNullableMessage(Int8Message.class, Int8Message::getInt8Field);

        Int8Message message = new Int8Message();
        message.setInt8Field(illegalValue);
        initSystemRequiredFields(message);

        Exception exception = assertThrows(RuntimeException.class, () -> loadAndReplicateData(message));
        assertThat(exception.getCause().getClass(), sameInstance(IllegalArgumentException.class));
        assertEquals(expectedErrorMessage, exception.getCause().getMessage());
    }

}
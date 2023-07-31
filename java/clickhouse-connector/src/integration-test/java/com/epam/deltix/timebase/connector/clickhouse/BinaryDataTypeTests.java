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
import com.epam.deltix.timebase.connector.clickhouse.timebase.BinaryMessage;
import com.epam.deltix.timebase.connector.clickhouse.timebase.NullableBinaryMessage;
import com.epam.deltix.util.collections.generated.ByteArrayList;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class BinaryDataTypeTests extends BaseStreamReplicatorTests {

    // Tests for nullable Binary data type

    @Timeout(10)
    @ParameterizedTest
    @ValueSource(
            strings = {"binary", " "}
    )
    void readNullableBinaryFromCH_expectedNotNullValue(String argument) {
        final ByteArrayList expectedValue = new ByteArrayList(argument.getBytes());

        NullableBinaryMessage message = new NullableBinaryMessage();
        message.setBinaryField(expectedValue);
        initSystemRequiredFields(message);

        Pair<DXTickStream, TableDeclaration> chSchemaByStream = loadAndReplicateData(message);
        ColumnDeclaration clickhouseColumn = getClickhouseColumn(chSchemaByStream.getLeft(), NullableBinaryMessage.class, NullableBinaryMessage::getBinaryField);
        Map<String, Object> values = selectAllValues(chSchemaByStream.getRight()).get(0);
        ByteArrayList actualValue = new ByteArrayList(((String) values.get(clickhouseColumn.getDbColumnName())).getBytes());

        systemRequiredFieldsCheck(message, values);
        assertEquals(expectedValue, actualValue);
    }

    @Test
    @Timeout(10)
    void readNullableBinaryFromCH_expectedNullValue() {
        final ByteArrayList expectedValue = null;

        NullableBinaryMessage message = new NullableBinaryMessage();
        message.setBinaryField(expectedValue);
        initSystemRequiredFields(message);

        Pair<DXTickStream, TableDeclaration> chSchemaByStream = loadAndReplicateData(message);
        ColumnDeclaration clickhouseColumn = getClickhouseColumn(chSchemaByStream.getLeft(), NullableBinaryMessage.class, NullableBinaryMessage::getBinaryField);
        Map<String, Object> values = selectAllValues(chSchemaByStream.getRight()).get(0);
        ByteArrayList actualValue = ((ByteArrayList) values.get(clickhouseColumn.getDbColumnName()));

        systemRequiredFieldsCheck(message, values);
        assertEquals(expectedValue, actualValue);
    }

    // Tests for non nullable Binary data type

    @Timeout(10)
    @ParameterizedTest
    @ValueSource(
            strings = {"binary", " "}
    )
    void readNonNullableBinaryFromCH_expectedNotNullValue(String argument) {
        final ByteArrayList expectedValue = new ByteArrayList(argument.getBytes());

        BinaryMessage message = new BinaryMessage();
        message.setBinaryField(expectedValue);
        initSystemRequiredFields(message);

        Pair<DXTickStream, TableDeclaration> chSchemaByStream = loadAndReplicateData(message);
        ColumnDeclaration clickhouseColumn = getClickhouseColumn(chSchemaByStream.getLeft(), BinaryMessage.class, BinaryMessage::getBinaryField);
        Map<String, Object> values = selectAllValues(chSchemaByStream.getRight()).get(0);
        ByteArrayList actualValue = new ByteArrayList(((String) values.get(clickhouseColumn.getDbColumnName())).getBytes());

        systemRequiredFieldsCheck(message, values);
        assertEquals(expectedValue, actualValue);
    }
}
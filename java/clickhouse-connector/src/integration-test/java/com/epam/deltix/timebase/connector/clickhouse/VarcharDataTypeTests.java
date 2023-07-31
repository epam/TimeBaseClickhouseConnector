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
import com.epam.deltix.qsrv.hf.pub.ExchangeCodec;
import com.epam.deltix.qsrv.hf.pub.md.VarcharDataType;
import com.epam.deltix.qsrv.hf.tickdb.pub.DXTickStream;
import com.epam.deltix.timebase.connector.clickhouse.timebase.VarcharAlphanumericMessage;
import com.epam.deltix.util.collections.generated.LongArrayList;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.sql.SQLException;
import java.util.Map;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class VarcharDataTypeTests extends BaseStreamReplicatorTests {

    @Timeout(10)
    @ParameterizedTest
    @MethodSource("getAlphanumericValues")
    void readNullableCharFromCH_expectedNotNullValue(String expectedValue, String[] expectedArrayValues) throws SQLException {
        VarcharAlphanumericMessage message = new VarcharAlphanumericMessage();
        message.setAlphanumeric(ExchangeCodec.codeToLong(expectedValue));
        message.setAlphanumericArray(getArrayList(expectedArrayValues));
        initSystemRequiredFields(message);

        Pair<DXTickStream, TableDeclaration> chSchemaByStream = loadAndReplicateData(message);
        ColumnDeclaration clickhouseColumn = getClickhouseColumn(chSchemaByStream.getLeft(), VarcharAlphanumericMessage.class, VarcharAlphanumericMessage::getAlphanumeric);
        ColumnDeclaration clickhouseArrayColumn = getClickhouseColumn(chSchemaByStream.getLeft(), VarcharAlphanumericMessage.class, VarcharAlphanumericMessage::getAlphanumericArray);
        Map<String, Object> values = selectAllValues(chSchemaByStream.getRight()).get(0);
        String actualValue = (String) values.get(clickhouseColumn.getDbColumnName());
        String[] actualArrayValues = (String[])(values.get(clickhouseArrayColumn.getDbColumnName()));

        systemRequiredFieldsCheck(message, values);
        assertEquals(expectedValue, actualValue);
        assertArrayEquals(expectedArrayValues, actualArrayValues);
    }

    private LongArrayList getArrayList(String[] expectedArrayValues) {
        LongArrayList list = new LongArrayList();
        for (String expectedValue : expectedArrayValues) {
            if (expectedValue == null) {
                list.add(VarcharDataType.ALPHANUMERIC_NULL);
            } else {
                list.add(ExchangeCodec.codeToLong(expectedValue));
            }
        }
        return list;
    }

    public Stream<Arguments> getAlphanumericValues() {

       return Stream.of(
                Arguments.of("840", new String[]{"912", "840"}),
                Arguments.of(null, new String[]{"912", null, "444"})
        );
    }
}
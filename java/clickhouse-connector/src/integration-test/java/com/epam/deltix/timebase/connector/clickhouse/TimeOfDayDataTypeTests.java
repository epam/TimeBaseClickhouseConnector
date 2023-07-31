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
import com.epam.deltix.qsrv.hf.pub.md.TimeOfDayDataType;
import com.epam.deltix.qsrv.hf.tickdb.pub.DXTickStream;
import com.epam.deltix.timebase.connector.clickhouse.timebase.NullableTimeOfDayMessage;
import com.epam.deltix.timebase.connector.clickhouse.timebase.TimeOfDayMessage;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.time.LocalTime;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class TimeOfDayDataTypeTests extends BaseStreamReplicatorTests {

    // Tests for nullable TimeOfDay data type

    @Timeout(10)
    @ParameterizedTest
    @ValueSource(
            strings = {"12:16:48.111", "12:16:48", "23:59:59.999", "00:00:00.000"}
    )
    void readNullableTimeOfDayFromCH_expectedNotNullValue(String argument) {
        final int expectedValue = toMillisOfDay(LocalTime.parse(argument));

        NullableTimeOfDayMessage message = new NullableTimeOfDayMessage();
        message.setTimeOfDayField(expectedValue);
        initSystemRequiredFields(message);

        Pair<DXTickStream, TableDeclaration> chSchemaByStream = loadAndReplicateData(message);
        ColumnDeclaration clickhouseColumn = getClickhouseColumn(chSchemaByStream.getLeft(), NullableTimeOfDayMessage.class, NullableTimeOfDayMessage::getTimeOfDayField);
        Map<String, Object> values = selectAllValues(chSchemaByStream.getRight()).get(0);
        int actualValue = (int) values.get(clickhouseColumn.getDbColumnName());

        systemRequiredFieldsCheck(message, values);
        assertEquals(expectedValue, actualValue);
    }

    @Test
    @Timeout(10)
    void readNullableTimeOfDayFromCH_expectedNullValue() {
        final int timeOfDayNull = TimeOfDayDataType.NULL;
        final Object expectedValue = null;

        NullableTimeOfDayMessage message = new NullableTimeOfDayMessage();
        message.setTimeOfDayField(timeOfDayNull);
        initSystemRequiredFields(message);

        Pair<DXTickStream, TableDeclaration> chSchemaByStream = loadAndReplicateData(message);
        ColumnDeclaration clickhouseColumn = getClickhouseColumn(chSchemaByStream.getLeft(), NullableTimeOfDayMessage.class, NullableTimeOfDayMessage::getTimeOfDayField);
        Map<String, Object> values = selectAllValues(chSchemaByStream.getRight()).get(0);
        Object actualValue = values.get(clickhouseColumn.getDbColumnName());

        systemRequiredFieldsCheck(message, values);
        assertEquals(expectedValue, actualValue);
    }

    // Tests for non nullable TimeOfDay data type

    @Timeout(10)
    @ParameterizedTest
    @ValueSource(
            strings = {"12:16:48.111", "12:16:48", "23:59:59.999", "00:00:00.000"}
    )
    void readNonNullableTimeOfDayFromCH_expectedNotNullValue(String argument) {
        final int expectedValue = toMillisOfDay(LocalTime.parse(argument));

        TimeOfDayMessage message = new TimeOfDayMessage();
        message.setTimeOfDayField(expectedValue);
        initSystemRequiredFields(message);

        Pair<DXTickStream, TableDeclaration> chSchemaByStream = loadAndReplicateData(message);
        ColumnDeclaration clickhouseColumn = getClickhouseColumn(chSchemaByStream.getLeft(), TimeOfDayMessage.class, TimeOfDayMessage::getTimeOfDayField);
        Map<String, Object> values = selectAllValues(chSchemaByStream.getRight()).get(0);
        int actualValue = (int) values.get(clickhouseColumn.getDbColumnName());

        systemRequiredFieldsCheck(message, values);
        assertEquals(expectedValue, actualValue);
    }
}
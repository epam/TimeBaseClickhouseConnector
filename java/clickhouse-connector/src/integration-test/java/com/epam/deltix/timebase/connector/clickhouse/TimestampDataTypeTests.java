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
import com.epam.deltix.qsrv.hf.pub.md.DateTimeDataType;
import com.epam.deltix.qsrv.hf.tickdb.pub.DXTickStream;
import com.epam.deltix.timebase.connector.clickhouse.timebase.NullableTimestampMessage;
import com.epam.deltix.timebase.connector.clickhouse.timebase.TimestampMessage;
import com.epam.deltix.timebase.connector.clickhouse.util.ClickhouseUtil;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.text.ParseException;
import java.time.Instant;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.sameInstance;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class TimestampDataTypeTests extends BaseStreamReplicatorTests {

    // Tests for nullable Timestamp data type

    @Test
    @Timeout(10)
    void readNullableTimestampFromCH_expectedNotNullValue() throws ParseException {
        final long expectedValue = Instant.parse("2020-06-10T20:40:38.155Z").toEpochMilli();
        testWriteNullable(expectedValue, expectedValue);
    }

    @Test
    @Timeout(10)
    void readNullableTimestampFromCH_expectedNullValue() throws ParseException {
        final long timestampNull = DateTimeDataType.NULL;
        testWriteNullable(timestampNull, null);
    }

    // Tests for non nullable Timestamp data type

    @Test
    @Timeout(10)
    void readNonNullableTimestampFromCH_expectedNotNullValue() throws ParseException {
        final long expectedValue = Instant.parse("2020-06-10T20:40:38.155Z").toEpochMilli();
        testWrite(expectedValue, expectedValue);
    }

    @Test
    @Timeout(10)
    void readOutOfRange() throws ParseException {
        long expectedValue = Instant.parse("3000-06-10T20:40:38.155Z").toEpochMilli();
        testWrite(expectedValue, ClickhouseUtil.DATETIME_64_MAX_VALUE);

        expectedValue = Instant.parse("1800-06-10T20:40:00.000Z").toEpochMilli();
        testWrite(expectedValue, ClickhouseUtil.DATETIME_64_MIN_VALUE);
    }

    void testWrite(long value, long expected) throws ParseException {

        TimestampMessage message = new TimestampMessage();
        message.setTimestampField(value);
        initSystemRequiredFields(message);

        Pair<DXTickStream, TableDeclaration> chSchemaByStream = loadAndReplicateData(message);
        ColumnDeclaration clickhouseColumn = getClickhouseColumn(chSchemaByStream.getLeft(), TimestampMessage.class, TimestampMessage::getTimestampField);
        Map<String, Object> values = selectAllValues(chSchemaByStream.getRight()).get(0);
        long actualValue = getTimestamp((String) values.get(clickhouseColumn.getDbColumnName()));

        systemRequiredFieldsCheck(message, values);
        assertEquals(expected, actualValue);
    }

    void testWriteNullable(long value, Object expected) throws ParseException {

        NullableTimestampMessage message = new NullableTimestampMessage();
        message.setTimestampField(value);
        initSystemRequiredFields(message);

        Pair<DXTickStream, TableDeclaration> chSchemaByStream = loadAndReplicateData(message);
        ColumnDeclaration clickhouseColumn = getClickhouseColumn(chSchemaByStream.getLeft(), NullableTimestampMessage.class, NullableTimestampMessage::getTimestampField);
        Map<String, Object> values = selectAllValues(chSchemaByStream.getRight()).get(0);
        Object actualValue = values.get(clickhouseColumn.getDbColumnName());
        if (actualValue != null)
            actualValue = getTimestamp((String)actualValue);

        systemRequiredFieldsCheck(message, values);
        assertEquals(expected, actualValue);
    }

    @Test
    @Timeout(10)
    void writeNullToNonNullableTimestampInTB_expectedRuntimeException() {
        final long illegalValue = DateTimeDataType.NULL;
        final String expectedErrorMessage = getFieldNotNullableMessage(TimestampMessage.class, TimestampMessage::getTimestampField);

        TimestampMessage message = new TimestampMessage();
        message.setTimestampField(illegalValue);
        initSystemRequiredFields(message);

        Exception exception = assertThrows(RuntimeException.class, () -> loadAndReplicateData(message));
        assertThat(exception.getCause().getClass(), sameInstance(IllegalArgumentException.class));
        assertEquals(expectedErrorMessage, exception.getCause().getMessage());
    }
}
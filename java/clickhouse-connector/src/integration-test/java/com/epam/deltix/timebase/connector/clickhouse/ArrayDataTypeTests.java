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

import com.epam.deltix.clickhouse.schema.TableDeclaration;
import com.epam.deltix.qsrv.hf.pub.md.*;
import com.epam.deltix.qsrv.hf.tickdb.pub.DXTickStream;
import com.epam.deltix.timebase.connector.clickhouse.timebase.TestEnum;
import com.epam.deltix.timebase.connector.clickhouse.timebase.array.AllArrayTypes;
import com.epam.deltix.timebase.connector.clickhouse.timebase.array.ArrayBooleanTestMessage;
import com.epam.deltix.timebase.connector.clickhouse.timebase.array.ArrayIntTestMessage;
import com.epam.deltix.timebase.connector.clickhouse.timebase.array.ArrayStrTestMessage;
import com.epam.deltix.util.collections.generated.*;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;


public class ArrayDataTypeTests extends BaseStreamReplicatorTests {


    @Timeout(10)
    @Test
    void replicateStringArray() {
        ArrayStrTestMessage message = new ArrayStrTestMessage();
        String[] arrayValue_expectedValue = {"ADC", "zxc\nzxc\"xc\"", "  ", ""};
        message.setArrayValue(new ObjectArrayList<>(arrayValue_expectedValue));
        String[] arrayNullableValue_expectedValue = {"ADC", null, null, ""};
        message.setArrayNullableValue(new ObjectArrayList<>(arrayNullableValue_expectedValue));
        String[] nullableArrayValue_expectedValue = {};
        message.setNullableArrayValue(null);
        String[] nullableArrayNullableValue_expectedValue = {"ADC", "zxc\nzxc\"xc\"", null};
        message.setNullableArrayNullableValue(new ObjectArrayList<>(nullableArrayNullableValue_expectedValue));
        initSystemRequiredFields(message);

        Pair<DXTickStream, TableDeclaration> chSchemaByStream = loadAndReplicateData(message);
        String arrayValue_columnName = getDbColumnName(chSchemaByStream.getLeft(), ArrayStrTestMessage.class, ArrayStrTestMessage::getArrayValue);
        String arrayNullableValue_columnName = getDbColumnName(chSchemaByStream.getLeft(), ArrayStrTestMessage.class, ArrayStrTestMessage::getArrayNullableValue);
        String nullableArrayValue_columnName = getDbColumnName(chSchemaByStream.getLeft(), ArrayStrTestMessage.class, ArrayStrTestMessage::getNullableArrayValue);
        String nullableArrayNullableValue_columnName = getDbColumnName(chSchemaByStream.getLeft(), ArrayStrTestMessage.class, ArrayStrTestMessage::getNullableArrayNullableValue);
        TableDeclaration tableDeclaration = chSchemaByStream.getRight();
        System.out.println(tableDeclaration.getTableIdentity().getDatabaseName());
        System.out.println(tableDeclaration.getTableIdentity().getTableName());
        Map<String, Object> values = selectAllValues(tableDeclaration).get(0);
        String[] arrayValue_actualValue = getArrayValue(values, arrayValue_columnName, String[].class);
        String[] arrayNullableValue_actualValue = getArrayValue(values, arrayNullableValue_columnName, String[].class);
        String[] nullableArrayValue_actualValue = getArrayValue(values, nullableArrayValue_columnName, String[].class);
        String[] nullableArrayNullableValue_actualValue = getArrayValue(values, nullableArrayNullableValue_columnName, String[].class);

        assertArrayEquals(arrayValue_expectedValue, arrayValue_actualValue);
        assertArrayEquals(arrayNullableValue_expectedValue, arrayNullableValue_actualValue);
        assertArrayEquals(nullableArrayValue_expectedValue, nullableArrayValue_actualValue);
        assertArrayEquals(nullableArrayNullableValue_expectedValue, nullableArrayNullableValue_actualValue);
        systemRequiredFieldsCheck(message, values);
    }


    @Timeout(10)
    @Test
    void replicateBooleanArray() {
        ArrayBooleanTestMessage message = new ArrayBooleanTestMessage();
        boolean[] arrayValue_expectedValue = {true, false, false, true};
        message.setArrayValue(new BooleanArrayList(arrayValue_expectedValue));
        byte[] arrayNullableValue_expectedValue = {1, BooleanDataType.NULL, BooleanDataType.NULL, 0};
        message.setArrayNullableValue(new ByteArrayList(arrayNullableValue_expectedValue));
        byte[] nullableArrayValue_expectedValue = {};
        message.setNullableArrayValue(null);
        byte[] nullableArrayNullableValue_expectedValue = {1, BooleanDataType.NULL, 0, 1};
        message.setNullableArrayNullableValue(new ByteArrayList(nullableArrayNullableValue_expectedValue));
        initSystemRequiredFields(message);

        Pair<DXTickStream, TableDeclaration> chSchemaByStream = loadAndReplicateData(message);
        String arrayValue_columnName = getDbColumnName(chSchemaByStream.getLeft(), ArrayBooleanTestMessage.class, ArrayBooleanTestMessage::getArrayValue);
        String arrayNullableValue_columnName = getDbColumnName(chSchemaByStream.getLeft(), ArrayBooleanTestMessage.class, ArrayBooleanTestMessage::getArrayNullableValue);
        String nullableArrayValue_columnName = getDbColumnName(chSchemaByStream.getLeft(), ArrayBooleanTestMessage.class, ArrayBooleanTestMessage::getNullableArrayValue);
        String nullableArrayNullableValue_columnName = getDbColumnName(chSchemaByStream.getLeft(), ArrayBooleanTestMessage.class, ArrayBooleanTestMessage::getNullableArrayNullableValue);
        TableDeclaration tableDeclaration = chSchemaByStream.getRight();

        Map<String, Object> values = selectAllValues(tableDeclaration).get(0);
        Byte[] arrayValue_actualValue = (Byte[]) values.get(arrayValue_columnName);
        Byte[] arrayNullableValue_actualValue = (Byte[]) values.get(arrayNullableValue_columnName);
        Byte[] nullableArrayValue_actualValue = (Byte[]) values.get(nullableArrayValue_columnName);
        Byte[] nullableArrayNullableValue_actualValue = (Byte[]) values.get(nullableArrayNullableValue_columnName);

        assertArrayEquals(new Byte[]{1, 0, 0, 1}, arrayValue_actualValue);
        assertArrayEquals(new Byte[]{1, null, null, 0}, arrayNullableValue_actualValue);
        assertArrayEquals(new Byte[]{}, nullableArrayValue_actualValue);
        assertArrayEquals(new Byte[]{1, null, 0, 1}, nullableArrayNullableValue_actualValue);

        systemRequiredFieldsCheck(message, values);
    }

    @Timeout(10)
    @Test
    void replicateIntArray() {
        ArrayIntTestMessage message = new ArrayIntTestMessage();

        int[] arrayValue_expectedValue = {Integer.MAX_VALUE, 0, -5, 5, Integer.MIN_VALUE + 1};
        message.setArrayValue(new IntegerArrayList(arrayValue_expectedValue));
        int[] arrayNullableValue_expectedValue = {1, IntegerDataType.INT32_NULL, IntegerDataType.INT32_NULL, -1};
        message.setArrayNullableValue(new IntegerArrayList(arrayNullableValue_expectedValue));
        int[] nullableArrayValue_expectedValue = {};
        message.setNullableArrayValue(null);
        int[] nullableArrayNullableValue_expectedValue = {1, -1, 0, IntegerDataType.INT32_NULL};
        message.setNullableArrayNullableValue(new IntegerArrayList(nullableArrayNullableValue_expectedValue));
        initSystemRequiredFields(message);

        Pair<DXTickStream, TableDeclaration> chSchemaByStream = loadAndReplicateData(message);
        String arrayValue_columnName = getDbColumnName(chSchemaByStream.getLeft(), ArrayIntTestMessage.class, ArrayIntTestMessage::getArrayValue);
        String arrayNullableValue_columnName = getDbColumnName(chSchemaByStream.getLeft(), ArrayIntTestMessage.class, ArrayIntTestMessage::getArrayNullableValue);
        String nullableArrayValue_columnName = getDbColumnName(chSchemaByStream.getLeft(), ArrayIntTestMessage.class, ArrayIntTestMessage::getNullableArrayValue);
        String nullableArrayNullableValue_columnName = getDbColumnName(chSchemaByStream.getLeft(), ArrayIntTestMessage.class, ArrayIntTestMessage::getNullableArrayNullableValue);
        TableDeclaration tableDeclaration = chSchemaByStream.getRight();

        Map<String, Object> values = selectAllValues(tableDeclaration).get(0);
        Integer[] arrayValue_actualValue = (Integer[]) values.get(arrayValue_columnName);
        Integer[] arrayNullableValue_actualValue = getArrayValue(values, arrayNullableValue_columnName, Integer[].class);
        Integer[] nullableArrayValue_actualValue = getArrayValue(values, nullableArrayValue_columnName, Integer[].class);
        Integer[] nullableArrayNullableValue_actualValue = getArrayValue(values, nullableArrayNullableValue_columnName, Integer[].class);

        assertArrayEquals(new Integer[]{Integer.MAX_VALUE, 0, -5, 5, Integer.MIN_VALUE + 1}, arrayValue_actualValue);
        assertArrayEquals(new Integer[]{1, null, null, -1}, arrayNullableValue_actualValue);
        assertArrayEquals(new Integer[]{}, nullableArrayValue_actualValue);
        assertArrayEquals(new Integer[]{1, -1, 0, null}, nullableArrayNullableValue_actualValue);
        systemRequiredFieldsCheck(message, values);
    }

    @Timeout(10)
    @Test
    void replicateArrayTest() {
        AllArrayTypes message = new AllArrayTypes();
        message.byteArray = new ByteArrayList(new byte[]{Byte.MAX_VALUE, 0, IntegerDataType.INT8_NULL, Byte.MIN_VALUE + 1});
        message.shortArray = new ShortArrayList(new short[]{Short.MAX_VALUE, 0, IntegerDataType.INT16_NULL, Short.MIN_VALUE + 1});
        message.longArray = new LongArrayList(new long[]{Long.MAX_VALUE, 0, IntegerDataType.INT64_NULL, Long.MIN_VALUE + 1});
        message.doubleArray = new DoubleArrayList(new double[]{Double.MAX_VALUE, 0, FloatDataType.IEEE64_NULL, Double.MIN_VALUE + 1});
        message.floatArray = new FloatArrayList(new float[]{Float.MAX_VALUE, 0, FloatDataType.IEEE32_NULL, Float.MIN_VALUE + 1});
        message.timestampList = new LongArrayList(new long[]{0, DateTimeDataType.NULL, DateTimeDataType.staticParse("2022-01-01 12:00:00.000"), 100});
        message.timeOfDayList = new IntegerArrayList(new int[]{0, TimeOfDayDataType.NULL, TimeOfDayDataType.staticParse("12:00:00")});
        message.enumArray = new ObjectArrayList<>(new CharSequence[]{TestEnum.FIRST.name(), null, TestEnum.FIRST.name(), TestEnum.DEFAULT.name()});
        initSystemRequiredFields(message);

        Pair<DXTickStream, TableDeclaration> chSchemaByStream = loadAndReplicateData(message);
        TableDeclaration tableDeclaration = chSchemaByStream.getRight();

        Map<String, Object> values = selectAllValues(tableDeclaration).get(0);

        Map<String, Object> expectedValues = new HashMap<>() {{
            put("AllArrayTypes_doubleArray", new Double[]{Double.MAX_VALUE, (double) 0, null, Double.MIN_VALUE + 1});
            put("AllArrayTypes_floatArray", new Float[]{Float.MAX_VALUE, (float) 0, null, Float.MIN_VALUE + 1});
            put("AllArrayTypes_enumArray", new String[]{"FIRST", null, "FIRST", "DEFAULT"});
            put("AllArrayTypes_timeOfDayList", new Integer[]{0, null, TimeOfDayDataType.staticParse("12:00:00")});
            put("AllArrayTypes_longArray", new Long[]{Long.MAX_VALUE, 0L, null, Long.MIN_VALUE + 1});
            put("AllArrayTypes_shortArray", new Short[]{Short.MAX_VALUE, 0, null, Short.MIN_VALUE + 1});
            put("AllArrayTypes_byteArray", new Byte[]{Byte.MAX_VALUE, 0, null, Byte.MIN_VALUE + 1});
            put("AllArrayTypes_timestampList", new String[]{"1970-01-01 00:00:00.000", null, "2022-01-01 12:00:00.000", "1970-01-01 00:00:00.100"});
        }};
        expectedValues.forEach((k, v) -> {
                Object actualValue = values.get(k);
                assertArrayEquals((Object[]) v, (Object[]) actualValue);
        });

        systemRequiredFieldsCheck(message, values);
    }

}
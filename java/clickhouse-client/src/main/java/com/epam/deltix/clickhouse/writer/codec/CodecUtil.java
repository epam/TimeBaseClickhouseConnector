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
package com.epam.deltix.clickhouse.writer.codec;

import com.clickhouse.data.ClickHouseDataType;
import com.epam.deltix.clickhouse.util.ParseHelper;
import com.epam.deltix.dfp.Decimal64;

import java.math.BigDecimal;
import java.sql.*;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;

public class CodecUtil {
    public final static String INTERNAL_NAME = "com/epam/deltix/clickhouse/writer/codec/CodecUtil";

    public final static String PROCESS_ENUM_NAME = "processEnum";
    public final static String PROCESS_ENUM_DESC = "(Ljava/sql/PreparedStatement;ILjava/lang/Enum;)I";

    public final static String PROCESS_STRING_NAME = "processString";
    public final static String PROCESS_STRING_DESC = "(Ljava/sql/PreparedStatement;ILjava/lang/CharSequence;)I";

    public final static String PROCESS_BOOLEAN_NAME = "processBoolean";
    public final static String PROCESS_BOOLEAN_DESC = "(Ljava/sql/PreparedStatement;ILjava/lang/Boolean;)I";
    public final static String PROCESS_PRIMITIVE_BOOLEAN_DESC = "(Ljava/sql/PreparedStatement;IZ)I";

    public final static String PROCESS_BYTE_NAME = "processByte";
    public final static String PROCESS_BYTE_DESC = "(Ljava/sql/PreparedStatement;ILjava/lang/Byte;)I";
    public final static String PROCESS_PRIMITIVE_BYTE_DESC = "(Ljava/sql/PreparedStatement;IB)I";

    public final static String PROCESS_SHORT_NAME = "processShort";
    public final static String PROCESS_SHORT_DESC = "(Ljava/sql/PreparedStatement;ILjava/lang/Short;)I";
    public final static String PROCESS_PRIMITIVE_SHORT_DESC = "(Ljava/sql/PreparedStatement;IS)I";

    public final static String PROCESS_INTEGER_NAME = "processInteger";
    public final static String PROCESS_INTEGER_DESC = "(Ljava/sql/PreparedStatement;ILjava/lang/Integer;)I";
    public final static String PROCESS_PRIMITIVE_INTEGER_DESC = "(Ljava/sql/PreparedStatement;II)I";

    public final static String PROCESS_LONG_NAME = "processLong";
    public final static String PROCESS_LONG_DESC = "(Ljava/sql/PreparedStatement;ILjava/lang/Long;)I";
    public final static String PROCESS_PRIMITIVE_LONG_DESC = "(Ljava/sql/PreparedStatement;IJ)I";

    public final static String PROCESS_INSTANT_AS_LONG_NAME = "processInstantAsLong";
    public final static String PROCESS_INSTANT_NAME = "processInstant";
    public final static String PROCESS_INSTANT_DESC = "(Ljava/sql/PreparedStatement;ILjava/time/Instant;)I";

    public final static String PROCESS_DURATION_AS_LONG_NAME = "processDurationAsLong";
    public final static String PROCESS_DURATION_DESC = "(Ljava/sql/PreparedStatement;ILjava/time/Duration;)I";

    public final static String PROCESS_FLOAT_NAME = "processFloat";
    public final static String PROCESS_FLOAT_DESC = "(Ljava/sql/PreparedStatement;ILjava/lang/Float;)I";
    public final static String PROCESS_PRIMITIVE_FLOAT_DESC = "(Ljava/sql/PreparedStatement;IF)I";

    public final static String PROCESS_DOUBLE_NAME = "processDouble";
    public final static String PROCESS_DOUBLE_DESC = "(Ljava/sql/PreparedStatement;ILjava/lang/Double;)I";
    public final static String PROCESS_PRIMITIVE_DOUBLE_DESC = "(Ljava/sql/PreparedStatement;ID)I";

    public final static String PROCESS_DECIMAL64_NAME = "processDecimal64";
    public final static String PROCESS_DECIMAL64_DESC = "(Ljava/sql/PreparedStatement;ILdeltix/dfp/Decimal64;)I";

    public final static String PROCESS_BIG_DECIMAL_NAME = "processBigDecimal";
    public final static String PROCESS_BIG_DECIMAL_DESC = "(Ljava/sql/PreparedStatement;ILjava/math/BigDecimal;)I";

    public final static String PROCESS_DATE_NAME = "processDate";
    public final static String PROCESS_DATE_BY_LONG_DESC = "(Ljava/sql/PreparedStatement;ILjava/lang/Long;)I";
    public final static String PROCESS_DATE_BY_LOCAL_DATE_DESC = "(Ljava/sql/PreparedStatement;ILjava/time/LocalDate;)I";
    public final static String PROCESS_DATE_BY_PRIMITIVE_LONG_DESC = "(Ljava/sql/PreparedStatement;IJ)I";

    public final static String PROCESS_ARRAY_NAME = "processArray";
    public final static String PROCESS_ARRAY_DESC = "(Ljava/sql/PreparedStatement;ILjava/lang/Object;Lru/yandex/clickhouse/domain/ClickHouseDataType;)I";

    public final static String VALIDATE_VALUE_FOR_NULL_NAME = "validateValueForNull";
    public final static String VALIDATE_VALUE_FOR_NULL_DESC = "(Ljava/lang/Object;Ljava/lang/String;)V";

    public final static String MAKE_LIST_NOT_NULL_NAME = "makeListNotNull";
    public final static String MAKE_LIST_NOT_NULL_DESC = "(Ljava/util/List;)Ljava/util/List;";

    // todo remove after support conversion toBigDecimal in Decimal64 class.
    private static final int DEFAULT_DECIMAL_SCALE = 12;

    public static final SimpleDateFormat TIMESTAMP_FORMAT_MS = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");


    public static int getSqlElementType(Class<?> elementType) {
        if (ParseHelper.convertibleToStringDataType(elementType)) {
            return Types.VARCHAR;
        } else if (ParseHelper.convertibleToUInt8DataType(elementType)) {
            return Types.BOOLEAN;
        } else if (ParseHelper.convertibleToInt8DataType(elementType)) {
            return Types.TINYINT;
        } else if (ParseHelper.convertibleToInt16DataType(elementType)) {
            return Types.SMALLINT;
        } else if (ParseHelper.convertibleToInt32DataType(elementType)) {
            return Types.INTEGER;
        } else if (ParseHelper.convertibleToInt64DataType(elementType)) {
            return Types.BIGINT;
        } else if (ParseHelper.convertibleToFloat32DataType(elementType)) {
            return Types.FLOAT;
        } else if (ParseHelper.convertibleToFloat64DataType(elementType)) {
            return Types.DOUBLE;
        } else if (ParseHelper.convertibleToDecimalDataType(elementType)) {
            return Types.DECIMAL;
        } else if (elementType.isEnum())
            return Types.VARCHAR;

        // todo support date/time
/*        else if (Date.class.isAssignableFrom(elementType)) {
            return Types.DATE;
        } else if (Time.class.isAssignableFrom(elementType)) {
            return Types.TIME;
        }*/

        throw new UnsupportedOperationException(String.format("Unsupported type: %s", elementType));
    }

    public static ClickHouseDataType getClickHouseDataType(Class<?> elementType) {
        if (ParseHelper.isStringDataType(elementType)) {
            return ClickHouseDataType.String;
        } else if (ParseHelper.isUInt8DataType(elementType)) {
            return ClickHouseDataType.UInt8;
        } else if (ParseHelper.isInt8DataType(elementType)) {
            return ClickHouseDataType.Int8;
        } else if (ParseHelper.isInt16DataType(elementType)) {
            return ClickHouseDataType.Int16;
        } else if (ParseHelper.isInt32DataType(elementType)) {
            return ClickHouseDataType.Int32;
        } else if (ParseHelper.isInt64DataType(elementType)) {
            return ClickHouseDataType.Int64;
        } else if (ParseHelper.isFloat32DataType(elementType)) {
            return ClickHouseDataType.Float32;
        } else if (ParseHelper.isFloat64DataType(elementType)) {
            return ClickHouseDataType.Float64;
        } else if (ParseHelper.isDecimalDataType(elementType)) {
            return ClickHouseDataType.Decimal;
        } else if (ParseHelper.isDateTimeDataType(elementType)) {
            return ClickHouseDataType.DateTime;
        } else if (ParseHelper.isDateDataType(elementType)) {
            return ClickHouseDataType.Date;
        } else if (elementType.isEnum())
            return ClickHouseDataType.Enum8;

        throw new UnsupportedOperationException(String.format("Unsupported type: %s", elementType));
    }


    public static Date getDate(long value) {
        return new Date(value);
    }

    public static Date getDate(Long value) {
        return new Date(value);
    }

    public static void validateValueForNull(Object value, String name) {
        if (value == null)
            throw new IllegalArgumentException(String.format("Field `%s` should not be null.", name));
    }

    // used by code generation. don't remove

    public static int processString(PreparedStatement ps, int parameterIndex, CharSequence value) throws SQLException {
        if (processNullable(ps, parameterIndex, value, Types.VARCHAR))
            parameterIndex++;
        else
            ps.setString(parameterIndex++, value.toString());

        return parameterIndex;
    }

    public static int processEnum(PreparedStatement ps, int parameterIndex, Enum value) throws SQLException {
        if (processNullable(ps, parameterIndex, value, Types.VARCHAR))
            parameterIndex++;
        else
            ps.setString(parameterIndex++, value.name());

        return parameterIndex;
    }

    public static int processBoolean(PreparedStatement ps, int parameterIndex, Boolean value) throws SQLException {
        if (processNullable(ps, parameterIndex, value, Types.BOOLEAN))
            parameterIndex++;
        else
            ps.setBoolean(parameterIndex++, value);

        return parameterIndex;
    }

    public static int processBoolean(PreparedStatement ps, int parameterIndex, boolean value) throws SQLException {
        ps.setBoolean(parameterIndex++, value);
        return parameterIndex;
    }

    public static int processByte(PreparedStatement ps, int parameterIndex, Byte value) throws SQLException {
        if (processNullable(ps, parameterIndex, value, Types.TINYINT))
            parameterIndex++;
        else
            ps.setByte(parameterIndex++, value);

        return parameterIndex;
    }

    public static int processByte(PreparedStatement ps, int parameterIndex, byte value) throws SQLException {
        ps.setByte(parameterIndex++, value);
        return parameterIndex;
    }

    public static int processShort(PreparedStatement ps, int parameterIndex, Short value) throws SQLException {
        if (processNullable(ps, parameterIndex, value, Types.SMALLINT))
            parameterIndex++;
        else
            ps.setShort(parameterIndex++, value);

        return parameterIndex;
    }

    public static int processShort(PreparedStatement ps, int parameterIndex, short value) throws SQLException {
        ps.setShort(parameterIndex++, value);
        return parameterIndex;
    }

    public static int processInteger(PreparedStatement ps, int parameterIndex, Integer value) throws SQLException {
        if (processNullable(ps, parameterIndex, value, Types.INTEGER))
            parameterIndex++;
        else
            ps.setInt(parameterIndex++, value);

        return parameterIndex;
    }

    public static int processInteger(PreparedStatement ps, int parameterIndex, int value) throws SQLException {
        ps.setInt(parameterIndex++, value);
        return parameterIndex;
    }

    public static int processLong(PreparedStatement ps, int parameterIndex, Long value) throws SQLException {
        if (processNullable(ps, parameterIndex, value, Types.BIGINT))
            parameterIndex++;
        else
            ps.setLong(parameterIndex++, value);

        return parameterIndex;
    }

    public static int processLong(PreparedStatement ps, int parameterIndex, long value) throws SQLException {
        ps.setLong(parameterIndex++, value);
        return parameterIndex;
    }

    public static int processInstantAsLong(PreparedStatement ps, int parameterIndex, Instant value) throws SQLException {
        if (processNullable(ps, parameterIndex, value, Types.BIGINT))
            parameterIndex++;
        else
            ps.setLong(parameterIndex++, value.toEpochMilli());

        return parameterIndex;
    }

    public static int processInstant(PreparedStatement ps, int parameterIndex, Instant value) throws SQLException {
        if (processNullable(ps, parameterIndex, value, Types.TIMESTAMP))
            parameterIndex++;
        else
            // @MS A work-around since clickhouse client (0.2.3) does not support the millisecond resolution for timestamps.
            // Simply serialize as String.
            //ps.setTimestamp(parameterIndex++, new Timestamp(value.toEpochMilli()));
            ps.setString(parameterIndex++, TIMESTAMP_FORMAT_MS.format(new java.util.Date(value.toEpochMilli())));

        return parameterIndex;
    }

    public static int processDurationAsLong(PreparedStatement ps, int parameterIndex, Duration value) throws SQLException {
        if (processNullable(ps, parameterIndex, value, Types.BIGINT))
            parameterIndex++;
        else
            ps.setLong(parameterIndex++, value.toMillis());

        return parameterIndex;
    }

    public static int processFloat(PreparedStatement ps, int parameterIndex, Float value) throws SQLException {
        if (processNullable(ps, parameterIndex, value, Types.FLOAT))
            parameterIndex++;
        else
            ps.setFloat(parameterIndex++, value);

        return parameterIndex;
    }

    public static int processFloat(PreparedStatement ps, int parameterIndex, float value) throws SQLException {
        ps.setFloat(parameterIndex++, value);
        return parameterIndex;
    }

    public static int processDouble(PreparedStatement ps, int parameterIndex, Double value) throws SQLException {
        if (processNullable(ps, parameterIndex, value, Types.DOUBLE))
            parameterIndex++;
        else
            ps.setDouble(parameterIndex++, value);

        return parameterIndex;
    }

    public static int processDouble(PreparedStatement ps, int parameterIndex, double value) throws SQLException {
        ps.setDouble(parameterIndex++, value);
        return parameterIndex;
    }

    public static int processDecimal64(PreparedStatement ps, int parameterIndex, Decimal64 value) throws SQLException {
        if (processNullable(ps, parameterIndex, value, Types.DECIMAL))
            parameterIndex++;
        else
            ps.setBigDecimal(parameterIndex++, getBigDecimal(value, DEFAULT_DECIMAL_SCALE));

        return parameterIndex;
    }

    public static int processBigDecimal(PreparedStatement ps, int parameterIndex, BigDecimal value) throws SQLException {
        if (processNullable(ps, parameterIndex, value, Types.DECIMAL))
            parameterIndex++;
        else
            ps.setBigDecimal(parameterIndex++, value);

        return parameterIndex;
    }

    public static int processDate(PreparedStatement ps, int parameterIndex, Long value) throws SQLException {
        if (processNullable(ps, parameterIndex, value, Types.DATE))
            parameterIndex++;
        else
            ps.setDate(parameterIndex++, new Date(value));

        return parameterIndex;
    }

    public static int processDate(PreparedStatement ps, int parameterIndex, LocalDate value) throws SQLException {
        if (processNullable(ps, parameterIndex, value, Types.DATE))
            parameterIndex++;
        else
            ps.setDate(parameterIndex++, Date.valueOf(value));

        return parameterIndex;
    }

    public static int processDate(PreparedStatement ps, int parameterIndex, long value) throws SQLException {
        ps.setDate(parameterIndex++, new Date(value));
        return parameterIndex;
    }

    public static int processArray(PreparedStatement ps, int parameterIndex, Object array, ClickHouseDataType sqlType) throws SQLException {

        ps.setObject(parameterIndex++, array);

        return parameterIndex;
    }

    public static <T> List<T> makeListNotNull(List<T> value) {
        if (value == null)
            return new ArrayList<>();

        return value;
    }


    private static BigDecimal getBigDecimal(Decimal64 value, int scale) {
        long fixedValue = value.toFixedPoint(scale);

        return BigDecimal.valueOf(fixedValue, scale);
    }

    private static boolean processNullable(PreparedStatement ps, int parameterIndex,
                                           Object value, int sqlType) throws SQLException {
        if (value == null) {
            ps.setNull(parameterIndex, sqlType);
            return true;
        }

        return false;
    }
}
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
package com.epam.deltix.clickhouse.util;

import com.epam.deltix.clickhouse.schema.*;
import com.epam.deltix.clickhouse.schema.types.*;
import com.epam.deltix.clickhouse.writer.IntrospectionType;
import com.epam.deltix.clickhouse.writer.Introspector;
import com.epam.deltix.clickhouse.schema.engines.MergeTreeEngine;
import com.epam.deltix.dfp.Decimal64;
import com.epam.deltix.gflog.api.Log;
import com.epam.deltix.gflog.api.LogFactory;
import org.apache.commons.lang3.NotImplementedException;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.util.*;
import java.util.stream.Collectors;

public class ParseHelper {
    private static final Log LOG = LogFactory.getLog(ParseHelper.class);

    private static final int DEFAULT_DECIMAL_PRECISION = 38;
    private static final int DEFAULT_DECIMAL_SCALE = 12;
    private static final String DEFAULT_ENUM_GET_NUMBER_METHOD = "getNumber";

    public static SqlDataType getDataType(Field field) {
        Class<?> fieldType = field.getType();
        ClickhouseDataType dataTypeAnnotation = field.getAnnotation(ClickhouseDataType.class);
        SchemaArrayElement arrayAnnotation = field.getAnnotation(SchemaArrayElement.class);
        Partition partitionAnnotation = field.getAnnotation(Partition.class);
        Index indexAnnotation = field.getAnnotation(Index.class);

        return getDataType(fieldType, arrayAnnotation, dataTypeAnnotation,
                partitionAnnotation, indexAnnotation, IntrospectionType.BY_FIELDS);
    }

    public static SqlDataType getDataType(Method method) {
        Class<?> methodReturnType = method.getReturnType();
        ClickhouseDataType dataTypeAnnotation = method.getAnnotation(ClickhouseDataType.class);
        SchemaArrayElement arrayAnnotation = method.getAnnotation(SchemaArrayElement.class);
        Partition partitionAnnotation = method.getAnnotation(Partition.class);
        Index indexAnnotation = method.getAnnotation(Index.class);

        return getDataType(methodReturnType, arrayAnnotation, dataTypeAnnotation,
                partitionAnnotation, indexAnnotation, IntrospectionType.BY_GETTERS);
    }

    public static MergeTreeEngine getMergeTreeEngine(TableDeclaration tableDeclaration) {
        List<ColumnDeclaration> columnDeclarations = tableDeclaration.getColumns();

        return new MergeTreeEngine(getIndexColumn(columnDeclarations), getPrimaryKeyColumns(columnDeclarations));
    }

    public static MergeTreeEngine getMergeTreeEngine(TableDeclaration tableDeclaration, int granularityIndex) {
        List<ColumnDeclaration> columnDeclarations = tableDeclaration.getColumns();

        return new MergeTreeEngine(getIndexColumn(columnDeclarations), getPrimaryKeyColumns(columnDeclarations), granularityIndex);
    }


    private static SqlDataType getDataType(Class<?> currentType,
                                           SchemaArrayElement arrayAnnotation,
                                           ClickhouseDataType dataTypeAnnotation,
                                           Partition partitionAnnotation,
                                           Index indexAnnotation,
                                           IntrospectionType introspectionType) {
        SqlDataType dataType;

        if (dataTypeAnnotation != null && (dataTypeAnnotation.value() == DataTypes.NESTED)) {
            if (arrayAnnotation != null)
                validateNestedDataType(dataTypeAnnotation.value(), currentType);
            else
                throw illegalNestedAnnotation(currentType);
        }

        DataTypes requestedType = dataTypeAnnotation == null ? null : dataTypeAnnotation.value();

        if (arrayAnnotation != null) {
            Class<?> nestedType = arrayAnnotation.nestedType();

            if (isSimpleType(nestedType)) {
                dataType = parseSimpleDataType(requestedType, nestedType);
                dataType = wrapWithNullableDataTypeIfApplicable(nestedType, dataType, partitionAnnotation, indexAnnotation);

                dataType = new ArraySqlType(dataType);
            } else {
                dataType = new NestedDataType(Introspector.getColumnDeclarations(nestedType, introspectionType));
            }
        } else {
            Class<?> componentType = currentType.getComponentType();
            if (componentType != null) {
                dataType = parseSimpleDataType(requestedType, componentType);
                dataType = new ArraySqlType(dataType);
            } else {
                dataType = parseSimpleDataType(requestedType, currentType);
                dataType = wrapWithNullableDataTypeIfApplicable(currentType, dataType, partitionAnnotation, indexAnnotation);
            }
        }

        return dataType;
    }

    private static SqlDataType parseSimpleDataType(DataTypes requestedType, Class<?> currentType) {
        if (requestedType == null)
            return getDefaultDataTypeByClass(currentType);

        switch (requestedType) {
            case ENUM8:
            case ENUM16:
                validateEnumDataType(requestedType, currentType);
                List<Enum8DataType.Enum8Value> values = getEnumValues(currentType);
                return new Enum8DataType(values);

            case STRING:
            case FIXED_STRING:
                validateStringDataType(requestedType, currentType);
                return new StringDataType();

            case UINT8:
                validateUInt8DataType(requestedType, currentType);
                return new UInt8DataType();
            case UINT16:
            case UINT32:
            case UINT64:
                throw unsupportedDataTypeException(requestedType, currentType);

            case INT8:
                validateInt8DataType(requestedType, currentType);
                return new Int8DataType();
            case INT16:
                validateInt16DataType(requestedType, currentType);
                return new Int16DataType();
            case INT32:
                validateInt32DataType(requestedType, currentType);
                return new Int32DataType();
            case INT64:
                validateInt64DataType(requestedType, currentType);
                return new Int64DataType();

            case FLOAT32:
                validateFloat32DataType(requestedType, currentType);
                return new Float32DataType();
            case FLOAT64:
                validateFloat64DataType(requestedType, currentType);
                return new Float64DataType();

            case DECIMAL:
                validateDecimalDataType(requestedType, currentType);
                return new DecimalDataType(DEFAULT_DECIMAL_PRECISION, DEFAULT_DECIMAL_SCALE);
            case DECIMAL32:
                validateDecimalDataType(requestedType, currentType);
                return new Decimal32DataType(DEFAULT_DECIMAL_SCALE);
            case DECIMAL64:
                validateDecimalDataType(requestedType, currentType);
                return new Decimal64DataType(DEFAULT_DECIMAL_SCALE);
            case DECIMAL128:
                validateDecimalDataType(requestedType, currentType);
                return new Decimal128DataType(DEFAULT_DECIMAL_SCALE);

            case DATE:
                validateDateDataType(requestedType, currentType);
                return new DateDataType();

            case DATE_TIME:
                validateDateTimeDataType(requestedType, currentType);
                return new DateTimeDataType();

            case DATE_TIME64:
                validateDateTimeDataType(requestedType, currentType);
                return new DateTime64DataType();

            default:
                throw unsupportedDataTypeException(requestedType);

        }
    }

    private static SqlDataType getDefaultDataTypeByClass(Class<?> clazz) {
        if (isStringDataType(clazz))
            return new StringDataType();
        else if (isDateTimeDataType(clazz))
            return new DateTime64DataType();
        else if (isDateDataType(clazz))
            return new DateDataType();
        else if (isInt64DataType(clazz))
            return new Int64DataType();
        else if (isInt32DataType(clazz))
            return new Int32DataType();
        else if (isInt16DataType(clazz))
            return new Int16DataType();
        else if (isInt8DataType(clazz))
            return new Int8DataType();
        else if (isFloat64DataType(clazz))
            return new Float64DataType();
        else if (isFloat32DataType(clazz))
            return new Float32DataType();
        else if (isUInt8DataType(clazz))
            return new UInt8DataType();
        else if (isDecimalDataType(clazz))
            return new Decimal128DataType(DEFAULT_DECIMAL_SCALE);
        else if (clazz.isEnum())
            return new Enum8DataType(getEnumValues(clazz));

        //todo support UUIDDataType
         /*else if (clazz.equals(UUID.class)) {
            return new UUIDDataType();
        }*/

        throw notImplementedException(clazz);
    }

    private static List<Enum8DataType.Enum8Value> getEnumValues(Class<?> clazz) {
        List<Enum8DataType.Enum8Value> values = new ArrayList<>();
        Object[] enumConstants = clazz.getEnumConstants();

        try {
            Method getNumberMethod = clazz.getDeclaredMethod(DEFAULT_ENUM_GET_NUMBER_METHOD);

            for (Object enumConstant : enumConstants) {
                byte byteValue;
                try {
                    byteValue = (byte) ((Integer) getNumberMethod.invoke(enumConstant)).intValue();
                } catch (IllegalAccessException | InvocationTargetException e) {
                    LOG.error()
                            .append("Error while access time to the method ")
                            .append(DEFAULT_ENUM_GET_NUMBER_METHOD)
                            .append('.')
                            .append(e)
                            .commit();
                    throw new IllegalStateException();
                }

                Enum8DataType.Enum8Value value = new Enum8DataType.Enum8Value(enumConstant.toString(), byteValue);

                values.add(value);
            }

            return values;
        } catch (NoSuchMethodException e) {
            // log level `debug` instead of `error`,
            // because there is no possibility to use the method "getNumber" in all enum classes now
            LOG.debug()
                    .append("Method ")
                    .append(DEFAULT_ENUM_GET_NUMBER_METHOD)
                    .append(" is not implemented in class ")
                    .append(clazz.getSimpleName())
                    .append('.')
                    .commit();
        }

        for (Object enumConstant : enumConstants) {
            byte byteValue = (byte) ((Enum) enumConstant).ordinal();

            Enum8DataType.Enum8Value value = new Enum8DataType.Enum8Value(enumConstant.toString(), byteValue);

            values.add(value);
        }

        return values;
    }

    private static boolean isSimpleType(Class<?> clazz) {
        return convertibleToStringDataType(clazz) || convertibleToInt64DataType(clazz) ||
                convertibleToInt32DataType(clazz) || convertibleToInt16DataType(clazz) ||
                convertibleToInt8DataType(clazz) || convertibleToUInt8DataType(clazz) ||
                convertibleToDecimalDataType(clazz) || convertibleToFloat64DataType(clazz) ||
                convertibleToFloat32DataType(clazz) || clazz.isEnum();
    }

    private static SqlDataType wrapWithNullableDataTypeIfApplicable(Class<?> currentType, SqlDataType dataType,
                                                                    Partition partitionAnnotation, Index indexAnnotation) {
        if (!currentType.isPrimitive() && partitionAnnotation == null && indexAnnotation == null)
            return new NullableDataType(dataType);

        return dataType;
    }


    private static ColumnDeclaration getIndexColumn(List<ColumnDeclaration> columnDeclarations) {
        return columnDeclarations.stream()
                .filter(ColumnDeclaration::isPartition)
                .findFirst()
                .get();
    }

    private static List<ColumnDeclaration> getPrimaryKeyColumns(List<ColumnDeclaration> columnDeclarations) {
        return columnDeclarations.stream()
                .filter(ColumnDeclaration::isIndex)
                .collect(Collectors.toList());
    }

    public static boolean isStringDataType(Class<?> clazz) {
        return CharSequence.class.isAssignableFrom(clazz);
    }

    public static boolean convertibleToStringDataType(Class<?> clazz) {
        return CharSequence.class.isAssignableFrom(clazz);
    }

    public static boolean isDateTimeDataType(Class<?> clazz) {
        return clazz.equals(Instant.class);
    }

    public static boolean convertibleToDateTimeDataType(Class<?> clazz) {
        return clazz.equals(Instant.class) ||
                clazz.equals(Long.class) || clazz.equals(long.class);
    }

    public static boolean isDateDataType(Class<?> clazz) {
        return clazz.equals(LocalDate.class);
    }

    public static boolean convertibleToDateDataType(Class<?> clazz) {
        return clazz.equals(LocalDate.class) ||
                clazz.equals(Long.class) || clazz.equals(long.class);
    }

    public static boolean isInt64DataType(Class<?> clazz) {
        return clazz.equals(Long.class) || clazz.equals(long.class);
    }

    public static boolean convertibleToInt64DataType(Class<?> clazz) {
        return clazz.equals(Long.class) || clazz.equals(long.class) ||
                clazz.equals(Instant.class) || clazz.equals(Duration.class);
    }

    public static boolean isInt32DataType(Class<?> clazz) {
        return clazz.equals(Integer.class) || clazz.equals(int.class);
    }

    public static boolean convertibleToInt32DataType(Class<?> clazz) {
        return clazz.equals(Integer.class) || clazz.equals(int.class);
    }

    public static boolean isInt16DataType(Class<?> clazz) {
        return clazz.equals(Short.class) || clazz.equals(short.class);
    }

    public static boolean convertibleToInt16DataType(Class<?> clazz) {
        return clazz.equals(Short.class) || clazz.equals(short.class);
    }

    public static boolean isInt8DataType(Class<?> clazz) {
        return clazz.equals(Byte.class) || clazz.equals(byte.class);
    }

    public static boolean convertibleToInt8DataType(Class<?> clazz) {
        return clazz.equals(Byte.class) || clazz.equals(byte.class);
    }

    public static boolean isFloat64DataType(Class<?> clazz) {
        return clazz.equals(Double.class) || clazz.equals(double.class);
    }

    public static boolean convertibleToFloat64DataType(Class<?> clazz) {
        return clazz.equals(Double.class) || clazz.equals(double.class) ||
                clazz.equals(Float.class) || clazz.equals(float.class);
    }

    public static boolean isFloat32DataType(Class<?> clazz) {
        return clazz.equals(Float.class) || clazz.equals(float.class);
    }

    public static boolean convertibleToFloat32DataType(Class<?> clazz) {
        return clazz.equals(Float.class) || clazz.equals(float.class);
    }

    public static boolean isUInt8DataType(Class<?> clazz) {
        return clazz.equals(Boolean.class) || clazz.equals(boolean.class);
    }

    public static boolean convertibleToUInt8DataType(Class<?> clazz) {
        return clazz.equals(Boolean.class) || clazz.equals(boolean.class);
    }

    public static boolean isDecimalDataType(Class<?> clazz) {
        return clazz.equals(Decimal64.class) || clazz.equals(BigDecimal.class);
    }

    public static boolean convertibleToDecimalDataType(Class<?> clazz) {
        return clazz.equals(Decimal64.class) || clazz.equals(BigDecimal.class);
    }


    private static void validateStringDataType(DataTypes requestedType, Class<?> currentType) {
        if (!convertibleToStringDataType(currentType))
            throw unsupportedDataTypeException(requestedType, currentType);
    }

    private static void validateUInt8DataType(DataTypes requestedType, Class<?> currentType) {
        if (!convertibleToUInt8DataType(currentType))
            throw unsupportedDataTypeException(requestedType, currentType);
    }

    private static void validateInt64DataType(DataTypes requestedType, Class<?> currentType) {
        if (!convertibleToInt64DataType(currentType))
            throw unsupportedDataTypeException(requestedType, currentType);
    }

    private static void validateInt32DataType(DataTypes requestedType, Class<?> currentType) {
        if (!convertibleToInt32DataType(currentType))
            throw unsupportedDataTypeException(requestedType, currentType);
    }

    private static void validateInt16DataType(DataTypes requestedType, Class<?> currentType) {
        if (!convertibleToInt16DataType(currentType))
            throw unsupportedDataTypeException(requestedType, currentType);
    }

    private static void validateInt8DataType(DataTypes requestedType, Class<?> currentType) {
        if (!convertibleToInt8DataType(currentType))
            throw unsupportedDataTypeException(requestedType, currentType);
    }

    private static void validateFloat64DataType(DataTypes requestedType, Class<?> currentType) {
        if (!convertibleToFloat64DataType(currentType))
            throw unsupportedDataTypeException(requestedType, currentType);
    }

    private static void validateFloat32DataType(DataTypes requestedType, Class<?> currentType) {
        if (!convertibleToFloat32DataType(currentType))
            throw unsupportedDataTypeException(requestedType, currentType);
    }

    private static void validateDecimalDataType(DataTypes requestedType, Class<?> currentType) {
        if (!convertibleToDecimalDataType(currentType))
            throw unsupportedDataTypeException(requestedType, currentType);
    }

    private static void validateEnumDataType(DataTypes requestedType, Class<?> currentType) {
        if (!currentType.isEnum())
            throw unsupportedDataTypeException(requestedType, currentType);
    }

    private static void validateDateDataType(DataTypes requestedType, Class<?> currentType) {
        if (!convertibleToDateDataType(currentType))
            throw unsupportedDataTypeException(requestedType, currentType);
    }

    private static void validateDateTimeDataType(DataTypes requestedType, Class<?> currentType) {
        if (!convertibleToDateTimeDataType(currentType))
            throw unsupportedDataTypeException(requestedType, currentType);
    }

    private static void validateNestedDataType(DataTypes requestedType, Class<?> currentType) {
        if (!List.class.isAssignableFrom(currentType))
            throw unsupportedDataTypeException(requestedType, currentType);
    }


    private static RuntimeException unsupportedDataTypeException(DataTypes dataType, Class<?> currentType) {
        throw new IllegalArgumentException(String.format("Unsupported `%s` data type for class `%s`.", dataType, currentType.getName()));
    }

    private static RuntimeException unsupportedDataTypeException(DataTypes dataType) {
        throw new IllegalArgumentException(String.format("Unsupported data type: (%s)", dataType));
    }

    private static RuntimeException illegalNullableType(DataTypes value) {
        throw new IllegalArgumentException(String.format("Nullable data type can't store composite data type: %s", value.getSqlDefinition()));
    }

    private static RuntimeException illegalNestedAnnotation(Class<?> currentType) {
        throw new IllegalArgumentException(String.format("SchemaArrayElement annotation should not be null for `%s`", currentType.toString()));
    }

    private static RuntimeException notImplementedException(Class<?> valueType) {
        return new NotImplementedException(String.format("Unknown class: %s", valueType.getSimpleName()));
    }
}
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
package com.epam.deltix.clickhouse.schema.types;

import com.epam.deltix.clickhouse.schema.SqlElement;

import java.util.Arrays;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

public enum DataTypes implements SqlElement {
    UINT8("UInt8"),
    UINT16("UInt16"),
    UINT32("UInt32"),
    UINT64("UInt64"),
    INT8("Int8"),
    INT16("Int16"),
    INT32("Int32"),
    INT64("Int64"),
    FLOAT32("Float32"),
    FLOAT64("Float64"),
    DECIMAL("Decimal"),
    DECIMAL32("Decimal32"),
    DECIMAL64("Decimal64"),
    DECIMAL128("Decimal128"),
    STRING("String"),
    FIXED_STRING("FixedString"),
    DATE("Date"),
    DATE_TIME("DateTime"),
    DATE_TIME64("DateTime64"),
    ENUM8("Enum8"),
    ENUM16("Enum16"),
    ARRAY("Array"),
    TUPLE("Tuple"),
    NULLABLE("Nullable"),
    NESTED("Nested"),
    NOTHING("Nothing");

    private static final Map<String, DataTypes> typesByName;

    static {
        typesByName = Arrays.stream(DataTypes.class.getEnumConstants())
            .collect(Collectors.toMap(DataTypes::getSqlDefinition, Function.identity()));
    }


    DataTypes(String sqlTypeName) {
        this.sqlTypeName = sqlTypeName;
    }

    private String sqlTypeName;

    public static DataTypes findByName(String name) {
        if (name == null)
            throw new IllegalArgumentException("name is null");

        DataTypes result = typesByName.get(name);
        if (result == null)
            throw new IllegalArgumentException(String.format("Data type '%s' not defined.", name));

        return result;
    }

    public String getSqlDefinition() {
        return sqlTypeName;
    }
}
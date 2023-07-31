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

public class NullableDataType extends BaseDataType {

    private final SqlDataType nestedType;

    public NullableDataType(SqlDataType nestedType) {
        super(DataTypes.NULLABLE);

        if (nestedType == null)
            throw new IllegalArgumentException("Nested type is null");

        if (nestedType.getType() == DataTypes.NULLABLE ||
            nestedType.getType() == DataTypes.NESTED ||
            nestedType.getType() == DataTypes.TUPLE ||
            nestedType.getType() == DataTypes.ARRAY)
            throw new IllegalArgumentException(String.format("Illegal nested type '%s'", nestedType.getSqlDefinition()));

        this.nestedType = nestedType;
    }

    public SqlDataType getNestedType() {
        return nestedType;
    }

    @Override
    public String getSqlDefinition() {
        return String.format("%s(%s)", type.getSqlDefinition(), nestedType.getSqlDefinition());
    }
}
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
package com.epam.deltix.clickhouse.schema;

import com.epam.deltix.clickhouse.models.ExpressionDeclaration;
import com.epam.deltix.clickhouse.schema.types.SqlDataType;

public class ColumnDeclaration implements SqlElement, ExpressionDeclaration {
    private final String name;
    private final SqlDataType dataType;
    private final String defaultExpression;
    private final boolean partition;
    private final boolean index;

    public ColumnDeclaration(String name, SqlDataType dataType) {
        this(name, dataType, null, false, false);
    }

    public ColumnDeclaration(String name, SqlDataType dataType, String defaultExpression) {
        this(name, dataType, defaultExpression, false, false);
    }

    public ColumnDeclaration(String name, SqlDataType dataType, boolean partition, boolean index) {
        this(name, dataType, null, partition, index);
    }

    public ColumnDeclaration(String name, SqlDataType dataType, String defaultExpression, boolean partition, boolean index) {
        if (name == null)
            throw new IllegalArgumentException("Argument 'name' is null");

        if (dataType == null)
            throw new IllegalArgumentException("Argument 'dataType' is null");

        this.name = name;
        this.dataType = dataType;
        this.defaultExpression = defaultExpression;
        this.partition = partition;
        this.index = index;
    }

    public String getDefaultExpression() {
        return defaultExpression;
    }

    public boolean isPartition() {
        return partition;
    }

    public boolean isIndex() {
        return index;
    }

    @Override
    public String getSqlDefinition() {
        return String.format("`%s` %s", name, dataType.getSqlDefinition());
    }

    @Override
    public String getDbColumnName() {
        return name;
    }

    @Override
    public SqlDataType getDbDataType() {
        return dataType;
    }

    @Override
    public String toString() {
        return getSqlDefinition();
    }
}
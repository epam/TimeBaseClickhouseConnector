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

import com.epam.deltix.clickhouse.schema.ColumnDeclaration;

import java.util.Collections;
import java.util.List;

public class ObjectDataType extends BaseDataType {

    private final String columnName;
    private final List<ColumnDeclaration> columns;

    public ObjectDataType(String columnName, List<ColumnDeclaration> columns) {
        super(DataTypes.NOTHING);
        this.columnName = columnName;

        this.columns = Collections.unmodifiableList(columns);
    }

    public List<ColumnDeclaration> getColumns() {
        return columns;
    }


    @Override
    public String getSqlDefinition() {
        StringBuilder sb = new StringBuilder();

        for (int i = 0; i < columns.size(); ++i) {
            if (i > 0)
                sb.append(", ");

            sb.append(String.format("`%s_%s` %s", columnName, columns.get(i).getDbColumnName(), columns.get(i).getDbDataType()));
        }

        return sb.toString();
    }

    public String getColumnName() {
        return columnName;
    }
}
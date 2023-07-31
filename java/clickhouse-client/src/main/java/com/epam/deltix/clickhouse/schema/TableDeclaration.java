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

import com.epam.deltix.clickhouse.models.TableIdentity;

import java.util.Collections;
import java.util.List;

public class TableDeclaration implements SqlElement {
    private final TableIdentity tableIdentity;
    private final List<ColumnDeclaration> columns;

    public TableDeclaration(TableIdentity tableIdentity, List<ColumnDeclaration> columns) {
        if (columns == null)
            throw new IllegalArgumentException("Argument 'columns' is null");

        this.tableIdentity = tableIdentity;
        this.columns = Collections.unmodifiableList(columns);
    }

    public TableIdentity getTableIdentity() {
        return tableIdentity;
    }

    public List<ColumnDeclaration> getColumns() {
        return columns;
    }

    @Override
    public String getSqlDefinition() {
        StringBuilder sb = new StringBuilder();

        sb.append(tableIdentity.toString());
        sb.append('(');

        for (int i = 0; i < columns.size(); ++i) {
            if (i > 0)
                sb.append(", ");

            sb.append(columns.get(i).getSqlDefinition());
        }

        sb.append(')');
        return sb.toString();
    }

    @Override
    public String toString() {
        return getSqlDefinition();
    }
}
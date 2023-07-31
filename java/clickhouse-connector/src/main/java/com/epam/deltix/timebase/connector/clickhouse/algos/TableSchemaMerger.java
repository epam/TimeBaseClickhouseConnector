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
package com.epam.deltix.timebase.connector.clickhouse.algos;

import com.epam.deltix.clickhouse.schema.ColumnDeclaration;
import com.epam.deltix.clickhouse.schema.TableDeclaration;
import com.epam.deltix.clickhouse.schema.types.ObjectDataType;
import com.epam.deltix.clickhouse.schema.types.SqlDataType;
import com.epam.deltix.gflog.api.Log;
import com.epam.deltix.gflog.api.LogFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

public class TableSchemaMerger {

    protected static final Log LOG = LogFactory.getLog(TableSchemaMerger.class);
    private final Map<String, ColumnDeclaration> actualTableColumns;
    private final List<ColumnDeclaration> expectedTableColumns;
    private List<ColumnDeclaration> filterColumns;
    private final String tableName;
    private final boolean enableSchemaValidation;

    private boolean hasChanges;


    public TableSchemaMerger(TableDeclaration expectedTable, TableDeclaration actualTableDefinition) {
        this(actualTableDefinition.getTableIdentity().getTableName(), expectedTable.getColumns(), actualTableDefinition.getColumns(), true);
    }

    public TableSchemaMerger(String tableName, List<ColumnDeclaration> expectedTableColumns, List<ColumnDeclaration> actualTableColumnsDefinition, boolean enableSchemaValidation) {
        this.tableName = tableName;
        this.expectedTableColumns = expectedTableColumns;
        this.actualTableColumns = actualTableColumnsDefinition.stream().collect(Collectors.toMap(ColumnDeclaration::getDbColumnName, Function.identity()));
        this.enableSchemaValidation = enableSchemaValidation;
    }

    public boolean mergeSchema() {
        filterColumns = filterColumns(expectedTableColumns, null);
        return hasChanges;
    }

    private List<ColumnDeclaration> filterColumns(List<ColumnDeclaration> expectedTableColumns, String parentName) {
        List<ColumnDeclaration> filterColumns = new ArrayList<>();
        for (ColumnDeclaration column : expectedTableColumns) {
            SqlDataType dbDataType = column.getDbDataType();
            if (dbDataType instanceof ObjectDataType) {
                ObjectDataType objectDataType = (ObjectDataType) dbDataType;
                List<ColumnDeclaration> innerFilterColumns = filterColumns(objectDataType.getColumns(), parentName == null ? column.getDbColumnName() : parentName + SchemaProcessor.COLUMN_NAME_PART_SEPARATOR + column.getDbColumnName());
                filterColumns.add(new ColumnDeclarationEx(column.getDbColumnName(),
                        new ObjectDataType(objectDataType.getColumnName(), innerFilterColumns)));
            } else {
                String columnName = parentName == null ? column.getDbColumnName() : parentName + SchemaProcessor.COLUMN_NAME_PART_SEPARATOR + column.getDbColumnName();
                ColumnDeclaration actualColumn = actualTableColumns.get(columnName);
                if (actualColumn != null) {
                    if (actualColumn.getDbDataType().getSqlDefinition().equals(column.getDbDataType().getSqlDefinition())) {
                        filterColumns.add(column);
                    } else {
                        String message = String.format("Existing table: '%s' does not match the types for column: '%s'. Expected type: %s",
                                tableName, columnName, column.getDbDataType().getSqlDefinition());
                        LOG.error(message);
                        throw new IllegalArgumentException(message);
                    }
                } else {
                    hasChanges = true;
                    if (enableSchemaValidation) {
                        LOG.warn("Cannot find column '%s' in table '%s'. Column data will NOT be replicated.").with(columnName).with(tableName);
                    }
                }
            }
        }
        return filterColumns;
    }

    public List<ColumnDeclaration> getFilterColumns() {
        return filterColumns;
    }
}
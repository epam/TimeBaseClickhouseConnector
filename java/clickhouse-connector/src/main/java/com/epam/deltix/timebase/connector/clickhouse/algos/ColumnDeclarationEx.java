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

import com.clickhouse.data.ClickHouseDataType;
import com.epam.deltix.clickhouse.schema.ColumnDeclaration;
import com.epam.deltix.clickhouse.schema.types.SqlDataType;
import com.epam.deltix.clickhouse.schema.types.NestedDataType;
import com.epam.deltix.clickhouse.schema.types.ObjectDataType;
import org.apache.commons.lang3.tuple.Pair;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class ColumnDeclarationEx extends ColumnDeclaration {
    public static String nestedClassDelimiter = "_";

    private final ClickHouseDataType clickHouseDataType;
    private final Object defaultValue;
    private int statementIndex;

    public ColumnDeclarationEx(String name, SqlDataType dataType) {
        super(name, dataType);
        this.clickHouseDataType = SchemaProcessor.convertDataTypeToRawClickhouseDataType(dataType);
        this.defaultValue = SchemaProcessor.convertDataTypeToDefaultValue(dataType);
    }

    public ColumnDeclarationEx(String name, SqlDataType dataType, boolean partition, boolean index) {
        super(name, dataType, partition, index);
        this.clickHouseDataType = SchemaProcessor.convertDataTypeToRawClickhouseDataType(dataType);
        this.defaultValue = SchemaProcessor.convertDataTypeToDefaultValue(dataType);
    }

    public static List<Pair<String, String>> getColumnSqlDefinitions(ColumnDeclaration column) {
        final SqlDataType dataType = column.getDbDataType();
        if (dataType instanceof ObjectDataType) {
            ObjectDataType objectDataType = (ObjectDataType) dataType;
            List<Pair<String, String>> result = new ArrayList<Pair<String, String>>();
            for (ColumnDeclaration c : objectDataType.getColumns()) {
//                final List<Pair<String, String>> pairs = getColumnSqlDefinitions(c);
                for (Pair<String, String> pair : getColumnSqlDefinitions(c))
                    result.add(Pair.of(column.getDbColumnName() + nestedClassDelimiter + pair.getLeft(), pair.getRight()));
            }
            return result;
            //return objectDataType.getColumns().stream().flatMap(column1 -> getColumnSqlDefinitions(column1).stream()).collect(Collectors.toList());
        } else
            return List.of(Pair.of(column.getDbColumnName(), column.getDbDataType().getSqlDefinition()));
    }

    public static List<ColumnDeclarationEx> getColumnsDeep(ColumnDeclarationEx column) {
        final SqlDataType dataType = column.getDbDataType();
        if (dataType instanceof ObjectDataType) {
            ObjectDataType objectDataType = (ObjectDataType) dataType;
            List<ColumnDeclarationEx> result = new ArrayList<ColumnDeclarationEx>();
            for (ColumnDeclaration c : objectDataType.getColumns()) {
                for (ColumnDeclarationEx c1 : getColumnsDeep((ColumnDeclarationEx) c))
                    //result.add(new ColumnDeclarationEx(column.getDbColumnName() + nestedClassDelimiter + c1.getDbColumnName(), c1.getDbDataType()));
                    result.add(c1);
            }
            return result;
        } else if (dataType instanceof NestedDataType) {
            NestedDataType objectDataType = (NestedDataType) dataType;
            return getNestedColumns(objectDataType);
        } else
            return List.of(column);
    }

    private static List<ColumnDeclarationEx> getNestedColumns(NestedDataType dataType) {
        return dataType.getColumns().stream()
                .map(c -> (ColumnDeclarationEx) c)
                .flatMap(columnDeclarationEx -> {
                    SqlDataType dbDataType = columnDeclarationEx.getDbDataType();
                    if (dbDataType instanceof ObjectDataType) {
                        return ((ObjectDataType) dbDataType).getColumns()
                                .stream()
                                .map(c -> (ColumnDeclarationEx) c);
                    } else {
                        return Stream.of(columnDeclarationEx);
                    }
                })
                .collect(Collectors.toList());
    }

    public static List<ColumnDeclarationEx> getColumnsDeep(List<ColumnDeclarationEx> columns) {
        List<ColumnDeclarationEx> result = new ArrayList<>();
        for (ColumnDeclarationEx c : columns) {
            for (ColumnDeclarationEx c1 : getColumnsDeep(c))
                result.add(c1);
        }
        return result;
    }

    public static List<ColumnDeclarationEx> getColumnsDeepForDefinition(ColumnDeclarationEx column) {
        final SqlDataType dataType = column.getDbDataType();
        if (dataType instanceof ObjectDataType) {
            ObjectDataType objectDataType = (ObjectDataType) dataType;
            List<ColumnDeclarationEx> result = new ArrayList<>();
            for (ColumnDeclaration c : objectDataType.getColumns()) {
                for (ColumnDeclarationEx c1 : getColumnsDeepForDefinition((ColumnDeclarationEx) c)) {
                    final ColumnDeclarationEx newColumn = new ColumnDeclarationEx(column.getDbColumnName() + nestedClassDelimiter + c1.getDbColumnName(), c1.getDbDataType());
                    newColumn.setStatementIndex(c1.getStatementIndex());
                    result.add(newColumn);
                }
            }
            return result;
        } else
            return List.of(column);
    }

    public static List<ColumnDeclarationEx> getColumnsDeepForDefinition(List<ColumnDeclarationEx> columns) {
        List<ColumnDeclarationEx> result = new ArrayList<>();
        for (ColumnDeclarationEx c : columns) {
            for (ColumnDeclarationEx c1 : getColumnsDeepForDefinition(c))
                result.add(c1);
        }
        return result;
    }

    public ColumnDeclarationEx deepCopy() {
        SqlDataType dataType = getDbDataType();

        if (dataType instanceof ObjectDataType) {
            final ObjectDataType objectDataType = (ObjectDataType) dataType;
            final List<ColumnDeclaration> columns = objectDataType.getColumns().stream().map(c -> ((ColumnDeclarationEx) c).deepCopy()).collect(Collectors.toList());
            dataType = new ObjectDataType(objectDataType.getColumnName(), columns);
        } else if (dataType instanceof NestedDataType) {
            final NestedDataType nestedDataType = (NestedDataType) dataType;
            final List<ColumnDeclaration> columns = nestedDataType.getColumns().stream().map(c -> ((ColumnDeclarationEx) c).deepCopy()).collect(Collectors.toList());
            dataType = new NestedDataType(columns);
        }

        return new ColumnDeclarationEx(getDbColumnName(), dataType, isPartition(), isIndex());
    }

    public int getStatementIndex() {
        return statementIndex;
    }

    public void setStatementIndex(int statementIndex) {
        this.statementIndex = statementIndex;
    }

    //    @Override
//    public String getSqlDefinition() {
//        final List<Pair<String, String>> pairs = getColumnSqlDefinitions(this);
//        StringBuilder sb = new StringBuilder();
//
//        for (int i = 0; i < pairs.size(); ++i) {
//            if (i > 0)
//                sb.append(", ");
//
//            final Pair<String, String> pair = pairs.get(i);
//            sb.append(String.format("`%s` %s", pair.getLeft(), pair.getRight()));
//        }
//
//        return sb.toString();
//    }

    @Override
    public String getSqlDefinition() {
        final List<ColumnDeclarationEx> columns = getColumnsDeepForDefinition(this);
        StringBuilder sb = new StringBuilder();

        for (int i = 0; i < columns.size(); ++i) {
            if (i > 0)
                sb.append(", ");

            final ColumnDeclarationEx columnDeclarationEx = columns.get(i);
            sb.append(String.format("`%s` %s", columnDeclarationEx.getDbColumnName(), columnDeclarationEx.getDbDataType()));
        }

        return sb.toString();
    }

    public ClickHouseDataType getClickHouseDataType() {
        return clickHouseDataType;
    }

    public Object getDefaultValue() {
        return defaultValue;
    }

    @Override
    public String toString() {
        return "{ si=" + statementIndex +
                super.toString() +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof ColumnDeclarationEx)) return false;

        ColumnDeclarationEx that = (ColumnDeclarationEx) o;

        return (that.getDbColumnName().equals(getDbColumnName()) &&
                that.getDbDataType().getSqlDefinition().equals(getDbDataType().getSqlDefinition()));
    }

    @Override
    public int hashCode() {
        int result = getDbColumnName().hashCode();
        result = 31 * result + getDbDataType().getSqlDefinition().hashCode();
        return result;
    }
}
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

import com.epam.deltix.clickhouse.models.ExpressionDeclaration;
import com.epam.deltix.clickhouse.models.TableIdentity;
import com.epam.deltix.clickhouse.schema.ColumnDeclaration;
import com.epam.deltix.clickhouse.schema.TableDeclaration;
import com.epam.deltix.clickhouse.schema.engines.Engine;
import com.epam.deltix.clickhouse.schema.types.SqlDataType;
import com.epam.deltix.clickhouse.schema.types.NestedDataType;
import com.epam.deltix.clickhouse.schema.types.ObjectDataType;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.List;

public class SqlQueryHelper {
    public static final String SHOW_TABLES = "SHOW TABLES";

    public static String getDescribeQuery(TableIdentity tableIdentity) {
        StringBuilder sb = new StringBuilder("DESCRIBE ");
        buildTableName(tableIdentity, sb);

        return sb.toString();
    }

    public static String getCreateDatabaseQuery(String databaseName, boolean ifNotExists) {
        StringBuilder sb = new StringBuilder("CREATE DATABASE ");
        if (ifNotExists)
            sb.append("IF NOT EXISTS ");
        sb.append(databaseName);
        return sb.toString();
    }

    public static String getCreateTableQuery(TableDeclaration tableDeclaration, Engine engineDeclaration, boolean createIfNotExists) {
        String createIfNotExistsStatement = createIfNotExists ? " IF NOT EXISTS " : "";
        return String.format("CREATE TABLE %s %s ENGINE = %s", createIfNotExistsStatement, tableDeclaration.getSqlDefinition(), engineDeclaration.getSqlDefinition());
    }

    public static String getRenameTableQuery(TableIdentity from, TableIdentity to) {
        StringBuilder sb = new StringBuilder("RENAME TABLE ");

        buildTableName(from, sb);
        sb.append(" TO ");
        buildTableName(to, sb);

        return sb.toString();
    }

    public static String getExistsTableQuery(TableIdentity tableIdentity) {
        StringBuilder sb = new StringBuilder("EXISTS TABLE ");
        buildTableName(tableIdentity, sb);

        return sb.toString();
    }


    public static String getDropTableQuery(TableIdentity tableIdentity, boolean ifExists) {
        StringBuilder sb = new StringBuilder("DROP TABLE ");
        if (ifExists)
            sb.append("IF EXISTS ");

        buildTableName(tableIdentity, sb);

        return sb.toString();
    }

    public static String getInsertIntoQuery(TableDeclaration tableDeclaration) {
        return getInsertIntoQuery(tableDeclaration.getTableIdentity(), tableDeclaration.getColumns());
    }

    public static String getInsertIntoQuery(TableIdentity tableIdentity, List<? extends ExpressionDeclaration> expressions) {
        StringBuilder sb = new StringBuilder("INSERT INTO ");
        buildTableName(tableIdentity, sb);
        sb.append(" (");

        int paramIndex = 0;
        for (ExpressionDeclaration expression : expressions) {
            if (paramIndex > 0)
                sb.append(", ");

            paramIndex = appendParamMaskAndGetParamIndex(sb, expression, paramIndex);
        }
        sb.append(')');

        sb.append(" VALUES(");
        for (int i = 0; i < paramIndex; ++i) {
            if (i > 0)
                sb.append(", ");

            sb.append('?');
        }
        sb.append(')');

        return sb.toString();
    }

    public static List<String> getQueryExpressions(List<? extends ExpressionDeclaration> expressions) {
        List<String> result = new ArrayList<>();

        for (ExpressionDeclaration expression : expressions) {
            if (expression.getDbDataType() instanceof NestedDataType) {
                for (ExpressionDeclaration nestedExpression : ((NestedDataType) expression.getDbDataType()).getColumns())
                    result.add(String.format("%s.%s", expression.getDbColumnName(), nestedExpression.getDbColumnName()));
            } else {
                result.add(expression.getDbColumnName());
            }
        }

        return result;
    }

    public static StringBuilder buildTableName(TableIdentity tableIdentity, StringBuilder sb) {
        appendDatabaseNamePrefix(sb, tableIdentity.getDatabaseName());
        sb.append(tableIdentity.getTableName());

        return sb;
    }


    private static int appendParamMaskAndGetParamIndex(StringBuilder sb, ExpressionDeclaration expression, int paramIndex) {
        if (expression.getDbDataType() instanceof NestedDataType) {
            List<ColumnDeclaration> nestedColumns = ((NestedDataType) expression.getDbDataType()).getColumns();
            for (int i = 0; i < nestedColumns.size(); i++) {
                if (i > 0)
                    sb.append(", ");

                ColumnDeclaration nestedColumn = nestedColumns.get(i);

                SqlDataType nestedType = nestedColumn.getDbDataType();
                if (nestedType instanceof ObjectDataType) {
                    List<ColumnDeclaration> objectColumns = ((ObjectDataType) nestedType).getColumns();
                    for (int i1 = 0; i1 < objectColumns.size(); i1++) {
                        if (i1 > 0)
                            sb.append(", ");

                        ColumnDeclaration objectColumn = objectColumns.get(i1);

                        sb.append('`');
                        sb.append(expression.getDbColumnName());
                        sb.append('.');
                        sb.append(nestedColumn.getDbColumnName());
                        sb.append('_');
                        sb.append(objectColumn.getDbColumnName());
                        sb.append('`');
                        paramIndex++;
                    }
                } else {
                    sb.append('`');
                    sb.append(expression.getDbColumnName());
                    sb.append('.');
                    sb.append(nestedColumn.getDbColumnName());
                    sb.append('`');
                    paramIndex++;
                }
            }
        } else {
            sb.append('`');
            sb.append(expression.getDbColumnName());
            sb.append('`');
            paramIndex++;
        }

        return paramIndex;
    }

    private static void appendDatabaseNamePrefix(StringBuilder sb, String dataBaseName) {
        if (StringUtils.isNotBlank(dataBaseName)) {
            sb.append(dataBaseName);
            sb.append('.');
        }
    }
}
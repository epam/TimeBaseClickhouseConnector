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
package com.epam.deltix.clickhouse;

import com.epam.deltix.clickhouse.schema.types.ArraySqlType;
import com.epam.deltix.clickhouse.schema.types.NestedDataType;
import com.epam.deltix.clickhouse.schema.types.SqlDataType;
import com.epam.deltix.clickhouse.parser.ParseProcessor;
import com.epam.deltix.clickhouse.schema.ColumnDeclaration;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.ResultSetExtractor;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class DescribeTableResultSetExtractor implements ResultSetExtractor<List<ColumnDeclaration>> {
    private static final String FIELD_NAME = "name";
    private static final String FIELD_TYPE = "type";
    private static final String FIELD_DEFAULT_TYPE = "default_type";
    private static final String FIELD_DEFAULT_EXPRESSION = "default_expression";

    @Override
    public List<ColumnDeclaration> extractData(ResultSet rs) throws SQLException, DataAccessException {

        List<ColumnDeclaration> columns = new ArrayList<>();
        //Map<String, ColumnDeclaration> nestedColumns = new HashMap<>();

        String currentNestedColumn = null;
        List<ColumnDeclaration> currentNestedDataType = null;

        while (rs.next()) {
            String name = rs.getString(FIELD_NAME);
            String type = rs.getString(FIELD_TYPE);
            SqlDataType dataType = ParseProcessor.parseDataType(type);
            String defaultExpression = rs.getString(FIELD_DEFAULT_EXPRESSION);

            // check for nested
            String[] nameParts = name.split("\\.");

            if (nameParts.length == 1) {

                if (currentNestedColumn != null) {
                    columns.add(new ColumnDeclaration(currentNestedColumn, new NestedDataType(currentNestedDataType)));
                    currentNestedColumn = null;
                    currentNestedDataType = null;
                }

                ColumnDeclaration column = new ColumnDeclaration(name, dataType, defaultExpression);
                columns.add(column);
            } else if (nameParts.length == 2) {
                if (currentNestedColumn == null) {
                    currentNestedColumn = nameParts[0];
                    currentNestedDataType = new ArrayList<>();
                } else if (!currentNestedColumn.equals(nameParts[0])) {
                    columns.add(new ColumnDeclaration(currentNestedColumn, new NestedDataType(currentNestedDataType)));
                    currentNestedColumn = nameParts[0];
                    currentNestedDataType = new ArrayList<>();
                }

                if (!(dataType instanceof ArraySqlType))
                    throw new IllegalStateException(String.format("Unexpected data type for nested column '%s' (type)", name));

                SqlDataType elementType = ((ArraySqlType)dataType).getElementType();
                currentNestedDataType.add(new ColumnDeclaration(nameParts[1], elementType, defaultExpression));
            } else {
                throw new IllegalStateException(String.format("Incorrect column name syntax '%s'", name));
            }
        }

        if (currentNestedColumn != null) {
            columns.add(new ColumnDeclaration(currentNestedColumn, new NestedDataType(currentNestedDataType)));
        }

        return columns;
    }
}
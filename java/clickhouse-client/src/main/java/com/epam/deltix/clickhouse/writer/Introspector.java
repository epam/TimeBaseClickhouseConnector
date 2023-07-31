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
package com.epam.deltix.clickhouse.writer;

import com.epam.deltix.clickhouse.models.ClickhouseTableIdentity;
import com.epam.deltix.clickhouse.models.TableIdentity;
import com.epam.deltix.clickhouse.schema.*;
import com.epam.deltix.clickhouse.schema.types.SqlDataType;
import com.epam.deltix.clickhouse.util.ReflectionUtil;
import com.epam.deltix.clickhouse.util.ParseHelper;
import org.apache.commons.lang3.tuple.Pair;
import java.beans.PropertyDescriptor;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.*;
import java.util.stream.Collectors;

public class Introspector {
    private static final String FIELD_DEFAULT_EXPRESSION = "";

    public static <T> TableDeclaration getTableDeclaration(Class<T> clazz, IntrospectionType introspectionType) {
        DataBaseName dataBaseNameAnnotation = clazz.getAnnotation(DataBaseName.class);
        TableName tableNameAnnotation = clazz.getAnnotation(TableName.class);

        String dataBaseName = dataBaseNameAnnotation != null ? dataBaseNameAnnotation.value() : null;
        String tableName = tableNameAnnotation != null ? tableNameAnnotation.value() : clazz.getSimpleName();

        return getTableDeclaration(ClickhouseTableIdentity.of(dataBaseName, tableName), clazz, introspectionType);
    }

    public static <T> TableDeclaration getTableDeclaration(TableIdentity tableIdentity, Class<T> clazz,
                                                           IntrospectionType introspectionType) {
        List<ColumnDeclaration> columns = getColumnDeclarations(clazz, introspectionType);

        return new TableDeclaration(tableIdentity, columns);
    }

    public static <T> List<ColumnDeclaration> getColumnDeclarations(Class<T> clazz, IntrospectionType introspectionType) {
        switch (introspectionType) {
            case BY_FIELDS:
                return getColumnDeclarationsByFields(clazz);
            case BY_GETTERS:
                return getColumnDeclarationsByGetMethods(clazz);

            default:
                throw new UnsupportedOperationException(String.format("Unknown introspection type '%s'", introspectionType));
        }
    }

    public static <T> List<ColumnDeclaration> getColumnDeclarationsByFields(Class<T> clazz) {
        return Arrays.stream(clazz.getFields())
                .filter(item -> !Modifier.isStatic(item.getModifiers()))
                .filter(item -> item.getAnnotation(SchemaIgnore.class) == null)
                .map(field -> {
                    SqlDataType dataType = ParseHelper.getDataType(field);
                    String resultName;

                    String fieldExpression;
                    String fieldName = field.getName();

                    SchemaElement schemaElement = field.getAnnotation(SchemaElement.class);
                    if (schemaElement != null) {
                        String tempName = schemaElement.name();
                        resultName = tempName.isEmpty() ? fieldName : tempName;
                        fieldExpression = schemaElement.defaultExpression();
                    } else {
                        resultName = fieldName;
                        fieldExpression = FIELD_DEFAULT_EXPRESSION;
                    }

                    Partition partition = field.getAnnotation(Partition.class);
                    boolean partitionValue = partition != null;

                    Index index = field.getAnnotation(Index.class);
                    boolean indexValue = index != null;

                    return new ColumnDeclaration(resultName, dataType, fieldExpression, partitionValue, indexValue);
                })
                .collect(Collectors.toList());
    }

    public static <T> List<ColumnDeclaration> getColumnDeclarationsByGetMethods(Class<T> clazz) {
        return Arrays.stream(clazz.getMethods())
                .filter(item -> item.getAnnotation(SchemaIgnore.class) == null)
                .filter(item -> !item.getDeclaringClass().equals(Object.class))
                .map(item -> Pair.of(item, ReflectionUtil.getPropertyByGetMethod(item)))
                .filter(item -> item.getRight() != null)
                .map(item -> {
                    Method method = item.getLeft();
                    PropertyDescriptor property = item.getRight();

                    SqlDataType dataType = ParseHelper.getDataType(method);

                    String name;
                    String fieldExpression;

                    SchemaElement schemaElement = method.getAnnotation(SchemaElement.class);
                    if (schemaElement != null) {
                        name = schemaElement.name();
                        fieldExpression = schemaElement.defaultExpression();
                    } else {
                        name = property.getName();
                        fieldExpression = FIELD_DEFAULT_EXPRESSION;
                    }

                    Partition partition = method.getAnnotation(Partition.class);
                    boolean partitionValue = partition != null;

                    Index index = method.getAnnotation(Index.class);
                    boolean indexValue = index != null;

                    return new ColumnDeclaration(name, dataType, fieldExpression, partitionValue, indexValue);
                })
                .collect(Collectors.toList());
    }

}
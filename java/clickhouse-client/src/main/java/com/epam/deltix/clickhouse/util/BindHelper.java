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

import com.epam.deltix.clickhouse.schema.*;
import com.epam.deltix.clickhouse.writer.IntrospectionType;
import com.epam.deltix.clickhouse.models.BindDeclaration;
import com.epam.deltix.clickhouse.schema.types.SqlDataType;
import com.epam.deltix.clickhouse.schema.types.DataTypes;
import net.bytebuddy.jar.asm.Type;
import org.apache.commons.lang3.tuple.Pair;

import java.beans.PropertyDescriptor;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

public class BindHelper {

    public static <T> List<BindDeclaration> getBindDeclarations(Class<T> clazz,
                                                                TableDeclaration tableDeclaration,
                                                                IntrospectionType introspectionType) {
        switch (introspectionType) {
            case BY_FIELDS:
                return getBindDeclarationsByFields(clazz, tableDeclaration);
            case BY_GETTERS:
                return getBindDeclarationsByGetMethods(clazz, tableDeclaration);

            default:
                throw new UnsupportedOperationException(String.format("Unknown introspection type '%s'", introspectionType));
        }
    }

    public static <T> List<BindDeclaration> getBindDeclarationsByFields(Class<T> clazz, TableDeclaration tableDeclaration) {
        return getBindDeclarationsByFields(clazz, tableDeclaration.getColumns());
    }

    public static <T> List<BindDeclaration> getBindDeclarationsByFields(Class<T> clazz, List<ColumnDeclaration> columnDeclarations) {
        Map<String, Field> clazzFieldsByName = Arrays.stream(clazz.getFields())
                .filter(item -> !Modifier.isStatic(item.getModifiers()))
                .filter(item -> item.getAnnotation(SchemaIgnore.class) == null)
                .collect(Collectors.toMap(item -> {
                    SchemaElement schemaElement = item.getAnnotation(SchemaElement.class);
                    String fieldName = item.getName();

                    return getSchemaElementNameIfNotNull(schemaElement, fieldName);
                }, Function.identity()));

        List<BindDeclaration> binds = new ArrayList<>();

        for (ColumnDeclaration column : columnDeclarations) {
            String dbColumnName = column.getDbColumnName();
            Field field = clazzFieldsByName.get(dbColumnName);

            if (field != null) {
                SqlDataType dbDataType = column.getDbDataType();

                validateBindTypes(clazz, field, dbDataType);

                BindDeclaration bindDeclaration = new BindDeclaration(
                        field,
                        null,
                        dbColumnName,
                        dbDataType);

                binds.add(bindDeclaration);
            }
        }

        return binds;
    }

    public static <T> List<BindDeclaration> getBindDeclarationsByGetMethods(Class<T> clazz, TableDeclaration tableDeclaration) {
        return getBindDeclarationsByGetMethods(clazz, tableDeclaration.getColumns());
    }

    public static <T> List<BindDeclaration> getBindDeclarationsByGetMethods(Class<T> clazz, List<ColumnDeclaration> columnDeclarations) {
        Map<String, PropertyDescriptor> getPropertiesByName = Arrays.stream(clazz.getMethods())
                .filter(item -> item.getAnnotation(SchemaIgnore.class) == null)
                .filter(item -> !item.getDeclaringClass().equals(Object.class))
                .map(item -> Pair.of(item, ReflectionUtil.getPropertyByGetMethod(item)))
                .filter(item -> item.getRight() != null)
                .collect(Collectors.toMap(item -> {
                    Method method = item.getLeft();
                    PropertyDescriptor property = item.getRight();

                    SchemaElement schemaElement = method.getAnnotation(SchemaElement.class);
                    String propertyName = property.getName();

                    return getSchemaElementNameIfNotNull(schemaElement, propertyName);
                }, Pair::getRight));


        List<BindDeclaration> binds = new ArrayList<>();

        for (ColumnDeclaration column : columnDeclarations) {
            String dbColumnName = column.getDbColumnName();
            PropertyDescriptor property = getPropertiesByName.get(dbColumnName);

            if (property != null) {
                Method readMethod = property.getReadMethod();
                SqlDataType dbDataType = column.getDbDataType();

                validateBindTypes(clazz, readMethod, dbDataType);

                BindDeclaration bindDeclaration = new BindDeclaration(
                        null,
                        readMethod,
                        dbColumnName,
                        dbDataType);

                binds.add(bindDeclaration);
            }
        }

        return binds;
    }

    public final static Function<BindDeclaration, String> fieldName = item -> item.getBeanField().getName();
    public final static Function<BindDeclaration, String> methodName = item -> item.getBeanReadMethod().getName();

    public final static Function<BindDeclaration, String> fieldDescriptor = item ->  Type.getDescriptor(item.getBeanField().getType());
    public final static Function<BindDeclaration, String> methodDescriptor = item -> Type.getMethodDescriptor(item.getBeanReadMethod());

    public final static Function<BindDeclaration, Class<?>> fieldType = item -> item.getBeanField().getType();
    public final static Function<BindDeclaration, Class<?>> methodType = item -> item.getBeanReadMethod().getReturnType();

    public final static Function<BindDeclaration, SchemaArrayElement> fieldArrayElement = item -> item.getBeanField().getAnnotation(SchemaArrayElement.class);
    public final static Function<BindDeclaration, SchemaArrayElement> methodArrayElement = item -> item.getBeanReadMethod().getAnnotation(SchemaArrayElement.class);

    public final static Function<BindDeclaration, String> memberName = item -> item.getBeanReadMethod() != null ? methodName.apply(item) : fieldName.apply(item);
    public final static Function<BindDeclaration, String> memberDescriptor = item -> item.getBeanReadMethod() != null ? methodDescriptor.apply(item) : fieldDescriptor.apply(item);
    public final static Function<BindDeclaration, Class<?>> memberType = item -> item.getBeanReadMethod() != null ? methodType.apply(item) : fieldType.apply(item);
    public final static Function<BindDeclaration, SchemaArrayElement> memberArrayElement = item -> item.getBeanReadMethod() != null ? methodArrayElement.apply(item) : fieldArrayElement.apply(item);


    private static String getSchemaElementNameIfNotNull(SchemaElement schemaElement, String propertyName) {
        if (schemaElement != null) {
            String schemaName = schemaElement.name();
            return schemaName.isEmpty() ? propertyName : schemaName;
        } else {
            return propertyName;
        }
    }

    private static <T> void validateBindTypes(Class<T> clazz, Field field, SqlDataType dbDataType) {
        DataTypes clazzType = ParseHelper.getDataType(field).getType();

        validateBindTypes(clazz.getName(), clazzType, field.getName(), field.getType(), dbDataType);
    }

    private static <T> void validateBindTypes(Class<T> clazz, Method method, SqlDataType dbDataType) {
        DataTypes clazzType = ParseHelper.getDataType(method).getType();

        validateBindTypes(clazz.getName(), clazzType, method.getName(), method.getReturnType(), dbDataType);
    }

    private static void validateBindTypes(String clazzName, DataTypes clazzType, String propertyName, Class<?> propertyType, SqlDataType dbDataType) {
        DataTypes columnType = dbDataType.getType();

        if (clazzType != columnType)
            throw bindPropertyException(clazzName, propertyName, propertyType, columnType);
    }

    private static RuntimeException bindPropertyException(String clazzName, String propertyName, Class<?> propertyType, DataTypes columnType) {
        throw new IllegalArgumentException(String.format("Can't bind data types for `%s.%s`. " +
                "Property type: %s, column type: %s.", clazzName, propertyName, propertyType, columnType));
    }
}
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
package com.epam.deltix.clickhouse.models;

import com.epam.deltix.clickhouse.schema.types.SqlDataType;

import java.lang.reflect.Field;
import java.lang.reflect.Method;

public class BindDeclaration implements ExpressionDeclaration {
    private Field beanField;
    private Method beanReadMethod;

    private String dbColumnName;
    private SqlDataType dbDataType;

    public BindDeclaration() {
    }

    public BindDeclaration(Field beanField, Method beanReadMethod,
                           String dbColumnName, SqlDataType dbDataType) {
        this.beanField = beanField;
        this.beanReadMethod = beanReadMethod;
        this.dbColumnName = dbColumnName;
        this.dbDataType = dbDataType;
    }

    public Field getBeanField() {
        return beanField;
    }

    public void setBeanField(Field beanField) {
        this.beanField = beanField;
    }

    public Method getBeanReadMethod() {
        return beanReadMethod;
    }

    public void setBeanReadMethod(Method beanReadMethod) {
        this.beanReadMethod = beanReadMethod;
    }

    @Override
    public String getDbColumnName() {
        return dbColumnName;
    }

    public void setDbColumnName(String dbColumnName) {
        this.dbColumnName = dbColumnName;
    }

    @Override
    public SqlDataType getDbDataType() {
        return dbDataType;
    }

    public void setDbDataType(SqlDataType dbDataType) {
        this.dbDataType = dbDataType;
    }
}
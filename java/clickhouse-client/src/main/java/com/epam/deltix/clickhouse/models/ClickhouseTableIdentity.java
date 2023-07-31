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

import org.apache.commons.lang3.StringUtils;

public class ClickhouseTableIdentity implements TableIdentity {
    private String databaseName;
    private String tableName;


    private ClickhouseTableIdentity(String databaseName, String tableName) {
        this.databaseName = databaseName;
        this.tableName = tableName;
    }


    public static ClickhouseTableIdentity of(final String tableName) {
        return of(null, tableName);
    }

    public static ClickhouseTableIdentity of(final String databaseName, final String tableName) {
        if (StringUtils.isBlank(tableName))
            throw new IllegalArgumentException(String.format("Table name not specified. Actual value: '%s'", tableName));

        return new ClickhouseTableIdentity(databaseName, tableName);
    }

    @Override
    public String getDatabaseName() {
        return databaseName;
    }

    @Override
    public String getTableName() {
        return tableName;
    }

    @Override
    public String toString() {
        return StringUtils.isNotBlank(databaseName) ? String.format("%s.%s", databaseName, tableName) : tableName;
    }
}
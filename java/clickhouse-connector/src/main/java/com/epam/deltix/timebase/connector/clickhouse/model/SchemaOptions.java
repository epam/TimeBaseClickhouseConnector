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
package com.epam.deltix.timebase.connector.clickhouse.model;

import com.epam.deltix.qsrv.hf.pub.md.RecordClassSet;

import java.util.Map;

public class SchemaOptions {

    private final RecordClassSet tbSchema;
    private final Map<String, String> mapping;
    private final WriteMode writeMode;
    private final ColumnNamingScheme columnNamingScheme;
    private final boolean includePartitionColumn;

    public SchemaOptions(RecordClassSet tbSchema, Map<String, String> mapping, WriteMode writeMode,
                         ColumnNamingScheme columnNamingScheme, boolean includePartitionColumn) {
        this.tbSchema = tbSchema;
        this.mapping = mapping;
        this.writeMode = writeMode;
        this.columnNamingScheme = columnNamingScheme;
        this.includePartitionColumn = includePartitionColumn;
    }

    public RecordClassSet getTbSchema() {
        return tbSchema;
    }

    public Map<String, String> getMapping() {
        return mapping;
    }

    public WriteMode getWriteMode() {
        return writeMode;
    }

    public ColumnNamingScheme getColumnNamingScheme() {
        return columnNamingScheme;
    }

    public boolean isIncludePartitionColumn() {
        return includePartitionColumn;
    }
}
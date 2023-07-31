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

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public class ReplicationRequest {

    private String table;
    private String key;
    private boolean splitByTypes = false;
    private ColumnNamingScheme columnNamingScheme;
    private Map<String, String> typeTableMapping = new HashMap<>();
    private WriteMode writeMode = WriteMode.APPEND;
    private Boolean includePartitionColumn;

    public boolean isSplitByTypes() {
        return splitByTypes;
    }

    public void setSplitByTypes(boolean splitByTypes) {
        this.splitByTypes = splitByTypes;
    }

    public Map<String, String> getTypeTableMapping() {
        return typeTableMapping;
    }

    public void setTypeTableMapping(Map<String, String> typeTableMapping) {
        this.typeTableMapping = typeTableMapping;
    }


    /**
     * Clickhouse target table name
     */
    public String getTable() {
        return table;
    }

    public void setTable(String table) {
        this.table = table;
        if (getKey() == null) {
            setKey(table);
        }
    }

    /**
     * Replication ID
     */
    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
        if (getTable() == null) {
            setTable(key);
        }
    }
    public WriteMode getWriteMode() {
        return writeMode;
    }

    public void setWriteMode(WriteMode writeMode) {
        this.writeMode = writeMode;
    }

    public ColumnNamingScheme getColumnNamingScheme() {
        return columnNamingScheme;
    }

    public void setColumnNamingScheme(ColumnNamingScheme columnNamingScheme) {
        this.columnNamingScheme = columnNamingScheme;
    }

    public Boolean getIncludePartitionColumn() {
        return includePartitionColumn;
    }

    public void setIncludePartitionColumn(Boolean includePartitionColumn) {
        this.includePartitionColumn = includePartitionColumn;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        ReplicationRequest that = (ReplicationRequest) o;

        if (!key.equals(that.key)) return false;
        if (splitByTypes != that.splitByTypes) return false;
        if (!table.equals(that.table)) return false;
        if (!Objects.equals(typeTableMapping, that.typeTableMapping))
            return false;
        return writeMode == that.writeMode;
    }

    @Override
    public int hashCode() {
        int result = table.hashCode();
        result = 31 * result + (key.hashCode());
        result = 31 * result + (splitByTypes ? 1 : 0);
        result = 31 * result + (typeTableMapping != null ? typeTableMapping.hashCode() : 0);
        result = 31 * result + (writeMode != null ? writeMode.hashCode() : 0);
        return result;
    }

}
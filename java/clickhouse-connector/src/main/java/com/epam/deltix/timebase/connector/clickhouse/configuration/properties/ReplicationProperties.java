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
package com.epam.deltix.timebase.connector.clickhouse.configuration.properties;

import com.epam.deltix.timebase.connector.clickhouse.model.ColumnNamingScheme;
import com.epam.deltix.timebase.connector.clickhouse.model.QueryRequest;
import com.epam.deltix.timebase.connector.clickhouse.model.StreamRequest;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;

@Component
@ConfigurationProperties(prefix = "replication")
public class ReplicationProperties {
    private List<StreamRequest> streams = new ArrayList<>();
    private List<QueryRequest> queries = new ArrayList<>();
    private ColumnNamingScheme columnNamingScheme = ColumnNamingScheme.NAME_AND_DATATYPE;
    private boolean includePartitionColumn = true;
    private int flushMessageCount = 10_000;
    private long flushTimeoutMs = 60_000;
    private long pollingIntervalMs = 60_000;

    public List<QueryRequest> getQueries() {
        return queries;
    }

    public void setQueries(List<QueryRequest> queries) {
        this.queries = queries;
    }

    public List<StreamRequest> getStreams() {
        return streams;
    }

    public void setStreams(List<StreamRequest> streams) {
        this.streams = streams;
    }

    public int getFlushMessageCount() {
        return flushMessageCount;
    }

    public void setFlushMessageCount(int flushMessageCount) {
        this.flushMessageCount = flushMessageCount;
    }

    public long getFlushTimeoutMs() {
        return flushTimeoutMs;
    }

    public void setFlushTimeoutMs(long flushTimeoutMs) {
        this.flushTimeoutMs = flushTimeoutMs;
    }

    public long getPollingIntervalMs() {
        return pollingIntervalMs;
    }

    public void setPollingIntervalMs(long pollingIntervalMs) {
        this.pollingIntervalMs = pollingIntervalMs;
    }

    public ColumnNamingScheme getColumnNamingScheme() {
        return columnNamingScheme;
    }

    public void setColumnNamingScheme(ColumnNamingScheme columnNamingScheme) {
        this.columnNamingScheme = columnNamingScheme;
    }

    public boolean isIncludePartitionColumn() {
        return includePartitionColumn;
    }

    public void setIncludePartitionColumn(boolean includePartitionColumn) {
        this.includePartitionColumn = includePartitionColumn;
    }
}
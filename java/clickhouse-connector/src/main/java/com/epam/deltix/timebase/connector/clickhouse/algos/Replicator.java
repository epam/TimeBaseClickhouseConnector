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

import com.epam.deltix.clickhouse.ClickhouseClient;
import com.epam.deltix.clickhouse.schema.TableDeclaration;
import com.epam.deltix.gflog.api.Log;
import com.epam.deltix.gflog.api.LogFactory;
import com.epam.deltix.qsrv.hf.tickdb.pub.DXTickDB;
import com.epam.deltix.timebase.connector.clickhouse.configuration.properties.ClickhouseProperties;
import org.apache.commons.lang3.tuple.Pair;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.ResultSetExtractor;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Collection;
import java.util.function.Consumer;

public abstract class Replicator implements Runnable {

    public static final String ALL_TYPES = "ALL_TYPES";

    protected static final Log LOG = LogFactory.getLog(Replicator.class);
    protected static final DateTimeFormatter DATE_TIME_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS")
            .withZone(ZoneOffset.UTC);

    protected final DXTickDB tickDb;
    protected final ClickhouseClient clickhouseClient;
    protected final ClickhouseProperties clickhouseProperties;
    protected final Consumer<Replicator> onStopped;
    protected final int flushMessageCount;
    protected final long flushTimeoutMs;

    public Replicator(DXTickDB tickDb, ClickhouseClient clickhouseClient, ClickhouseProperties clickhouseProperties,
                      Consumer<Replicator> onStopped, int flushMessageCount, long flushTimeoutMs) {
        this.tickDb = tickDb;
        this.clickhouseClient = clickhouseClient;
        this.clickhouseProperties = clickhouseProperties;
        this.onStopped = onStopped;

        if (flushMessageCount <= 0)
            throw new IllegalArgumentException("Illegal flushMessageCount");
        if (flushTimeoutMs <= 0)
            throw new IllegalArgumentException("Illegal flushTimeoutMs");

        this.flushMessageCount = flushMessageCount;
        this.flushTimeoutMs = flushTimeoutMs;
    }

    public abstract void stop();

    public abstract String getKey();

    protected Pair<Instant, Instant> getTimeInterval(TableDeclaration clickhouseTable) {
        final String minTimestampAlias = "minTimestamp";
        final String maxTimestampAlias = "maxTimestamp";
        String selectQuery = String.format("SELECT min(%s) as %s, max(%s) AS %s FROM %s",
                SchemaProcessor.TIMESTAMP_COLUMN_NAME, minTimestampAlias, SchemaProcessor.TIMESTAMP_COLUMN_NAME, maxTimestampAlias,
                clickhouseTable.getTableIdentity().toString());
        LOG.debug()
                .append(selectQuery)
                .commit();

        Pair<Instant, Instant> result = clickhouseClient.getJdbcTemplate().query(selectQuery, new ResultSetExtractor<Pair<Instant, Instant>>() {
            @Override
            public Pair<Instant, Instant> extractData(ResultSet rs) throws SQLException, DataAccessException {

                if (rs.next()) {
                    Timestamp minTimestamp = getTimestamp(rs, minTimestampAlias);
                    Timestamp maxTimestamp = getTimestamp(rs, maxTimestampAlias);

                    if (minTimestamp == null && maxTimestamp == null)
                        return null;

                    return Pair.of(minTimestamp.toInstant(), maxTimestamp.toInstant());
                }

                return null;
            }
        });

        return result;
    }

    protected Long getMaxTimestamp(TableDeclaration clickhouseTable) {
        final String maxTimestampAlias = "maxTimestamp";
        String selectQuery = String.format("SELECT max(%s) AS %s FROM %s",
                SchemaProcessor.TIMESTAMP_COLUMN_NAME, maxTimestampAlias, clickhouseTable.getTableIdentity().toString());
        LOG.debug()
                .append(selectQuery)
                .commit();
        return clickhouseClient.getJdbcTemplate().query(selectQuery, rs -> {
            if (rs.next()) {
                Timestamp maxTimestamp = getTimestamp(rs, maxTimestampAlias);
                if (maxTimestamp == null)
                    return Long.MIN_VALUE;
                return maxTimestamp.getTime();
            }
            return Long.MIN_VALUE;
        });
    }

    protected long findLastTimestamp(Collection<TableDeclaration> clickhouseTables) {
        return clickhouseTables.stream().mapToLong(this::getMaxTimestamp).max().orElse(Long.MIN_VALUE);
    }

    protected String formatToDateTime3(long timestamp) {
        return DATE_TIME_FORMATTER.format(Instant.ofEpochMilli(timestamp));
    }

    protected Timestamp getTimestamp(ResultSet resultSet, String columnName) throws SQLException {
        String timestampString = resultSet.getString(columnName);

        Timestamp timestamp = "0000-00-00 00:00:00".equals(timestampString) ||
                "0000-00-00 00:00:00.000".equals(timestampString) ||
                "0000-00-00 00:00:00.000000".equals(timestampString) ||
                "0000-00-00 00:00:00.000000000".equals(timestampString) ?
                null :
                resultSet.getTimestamp(columnName);

        return timestamp;
    }

    protected void truncateData(Collection<TableDeclaration> clickhouseTables, long timestamp) {
        if (timestamp != Long.MIN_VALUE){
            for (TableDeclaration clickhouseTable : clickhouseTables) {
                LOG.info()
                        .append("Table `")
                        .append(clickhouseTable.getTableIdentity().toString())
                        .append("` ")
                        .append("truncate data to ")
                        .append(formatToDateTime3(timestamp))
                        .commit();

                String deleteTailQuery = String.format("ALTER TABLE %s DELETE WHERE %s = toDateTime64('%s',9)",
                        clickhouseTable.getTableIdentity().toString(), SchemaProcessor.TIMESTAMP_COLUMN_NAME, formatToDateTime3(timestamp));
                LOG.debug()
                        .append(deleteTailQuery)
                        .commit();

                clickhouseClient.getJdbcTemplate().execute(deleteTailQuery);
            }
        }
    }
}
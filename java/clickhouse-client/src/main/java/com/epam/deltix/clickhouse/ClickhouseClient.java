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

import com.epam.deltix.clickhouse.models.TableIdentity;
import com.epam.deltix.clickhouse.util.CheckedConsumer;
import com.epam.deltix.clickhouse.util.SqlQueryHelper;
import com.epam.deltix.clickhouse.writer.IntrospectionType;
import com.epam.deltix.clickhouse.writer.Introspector;
import com.epam.deltix.clickhouse.schema.ColumnDeclaration;
import com.epam.deltix.clickhouse.schema.TableDeclaration;
import com.epam.deltix.clickhouse.schema.engines.Engine;
import com.epam.deltix.clickhouse.selector.QuerySource;
import com.epam.deltix.clickhouse.selector.TableSelectQuery;
import com.epam.deltix.clickhouse.selector.sources.TableSource;
import com.epam.deltix.gflog.api.Log;
import com.epam.deltix.gflog.api.LogFactory;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.support.rowset.SqlRowSet;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;

public class ClickhouseClient implements AutoCloseable {


    private static final Log LOG = LogFactory.getLog(ClickhouseClient.class);


    private final DataSource clickhouseDataSource;
    private final ClickhouseClientSettings settings;

    private final JdbcTemplate jdbcTemplate;

    public ClickhouseClient(final DataSource clickhouseDataSource) {
        this(clickhouseDataSource, ClickhouseClientSettings.DEFAULT);
    }

    public ClickhouseClient(final DataSource clickhouseDataSource, ClickhouseClientSettings settings) {

        if (clickhouseDataSource == null)
            throw new IllegalArgumentException("clickhouseDataSource is NULL");

        if (settings == null)
            throw new IllegalArgumentException("settings is NULL");

        this.clickhouseDataSource = clickhouseDataSource;
        this.settings = settings;
        this.jdbcTemplate = new JdbcTemplate(clickhouseDataSource);
    }

    public ClickhouseClientSettings getSettings() {
        return settings;
    }

    public void dropTable(TableIdentity tableIdentity) {
        dropTable(tableIdentity, false);
    }

    public void dropTable(TableIdentity tableIdentity, boolean ifExists) {
        jdbcTemplate.execute(SqlQueryHelper.getDropTableQuery(tableIdentity, ifExists));
    }

    public void renameTable(TableIdentity from, TableIdentity to) {
        jdbcTemplate.execute(SqlQueryHelper.getRenameTableQuery(from, to));
    }

    public boolean existsTable(TableIdentity tableIdentity) {
        SqlRowSet rowSet = jdbcTemplate.queryForRowSet(SqlQueryHelper.getExistsTableQuery(tableIdentity));

        if (rowSet.next())
            return rowSet.getBoolean(1);

        return false;
    }

    public void createDatabase(String databaseName, boolean createIfNotExists) throws SQLException {
        String createDatabaseQuery = SqlQueryHelper.getCreateDatabaseQuery(databaseName, createIfNotExists);
        executeExpression(createDatabaseQuery);
    }

    public void createTable(TableDeclaration tableDeclaration, Engine engineDeclaration, boolean createIfNotExists) throws SQLException {
        String query = SqlQueryHelper.getCreateTableQuery(tableDeclaration, engineDeclaration, createIfNotExists);
        executeExpression(query);
    }

    public TableDeclaration describeTable(TableIdentity tableIdentity) {
        List<ColumnDeclaration> columns = jdbcTemplate.query(SqlQueryHelper.getDescribeQuery(tableIdentity), new DescribeTableResultSetExtractor());
        return new TableDeclaration(tableIdentity, columns);
    }

    public <T> TableDeclaration describeClass(Class<T> clazz, IntrospectionType introspectionType) {
        return Introspector.getTableDeclaration(clazz, introspectionType);
    }

    public JdbcTemplate getJdbcTemplate() {
        return jdbcTemplate;
    }

    public DataSource getDataSource() {
        return clickhouseDataSource;
    }

    public Connection getConnection() throws SQLException {
        return clickhouseDataSource.getConnection();
    }

    public TableSelectQuery createSelectQuery(TableIdentity tableIdentity) {
        return createSelectQuery(new TableSource(clickhouseDataSource, tableIdentity));
    }

    public TableSelectQuery createSelectQuery(QuerySource querySource) {
        return new TableSelectQuery(this, querySource);
    }

    @Override
    public void close() {
        //jdbcTemplate.
    }

    public void executeExpression(String sqlExpression) throws SQLException {
        LOG.debug()
                .append("Execute SQL query ")
                .append(System.lineSeparator())
                .append(sqlExpression)
                .commit();

        executeInSqlConnection(connection -> {
            connection.createStatement().execute(sqlExpression);
        });
    }

    public void executeInSqlConnection(CheckedConsumer<Connection> func) throws SQLException {
        try {
            try (Connection connection = getConnection()) {
                func.accept(connection);
            }
        } catch (SQLException ex) {
            LOG.error()
                    .append("Error while executing SQL expression.")
                    .append(System.lineSeparator())
                    .append(ex)
                    .commit();

            throw ex;
        }
    }
}
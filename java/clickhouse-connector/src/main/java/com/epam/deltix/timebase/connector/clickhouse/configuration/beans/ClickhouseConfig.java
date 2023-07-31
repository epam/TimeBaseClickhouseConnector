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
package com.epam.deltix.timebase.connector.clickhouse.configuration.beans;

import com.clickhouse.client.config.ClickHouseDefaults;
import com.clickhouse.jdbc.ClickHouseDataSource;
import com.epam.deltix.clickhouse.ClickhouseClient;
import com.epam.deltix.timebase.connector.clickhouse.configuration.properties.ClickhouseProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Lazy;
import org.springframework.jdbc.core.ArgumentPreparedStatementSetter;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.PreparedStatementSetter;

import javax.sql.DataSource;

import java.sql.SQLException;
import java.util.Properties;

import static com.clickhouse.client.config.ClickHouseClientOption.USE_OBJECTS_IN_ARRAYS;

@Configuration
public class ClickhouseConfig {
    private final static Logger logger = LoggerFactory.getLogger(ClickhouseConfig.class);

    @Autowired
    @Bean(name="clickHouseDataSource")
    @Lazy
    public DataSource clickHouseDataSource(ClickhouseProperties clickhouseProperties) throws SQLException {
        final Properties clickHouseProperties = new Properties();
        clickHouseProperties.put(USE_OBJECTS_IN_ARRAYS.getKey(), true);
        clickHouseProperties.put(ClickHouseDefaults.USER.getKey(), clickhouseProperties.getUsername());
        clickHouseProperties.put(ClickHouseDefaults.PASSWORD.getKey(), clickhouseProperties.getPassword());
        return new ClickHouseDataSource(clickhouseProperties.getUrl(), clickHouseProperties);
    }

    @Autowired
    @Bean(name="clickHouseJdbcTemplate")
    @Lazy()
    public JdbcTemplate clickHouseJdbcTemplate(@Qualifier("clickHouseDataSource") DataSource clickHouseDataSource) {
        return new JdbcTemplate(clickHouseDataSource) {
            @Override
            protected PreparedStatementSetter newArgPreparedStatementSetter(Object[] args) {
                return new ArgumentPreparedStatementSetter(args) {
                };
            }
        };
    }

//    @Autowired
//    @Bean(name="clickHouseNamedParameterJdbcTemplate")
//    public NamedParameterJdbcTemplate clickHouseNamedParameterJdbcTemplate(@Qualifier("clickHouseJdbcTemplate") JdbcTemplate jdbcTemplate) {
//        return new NamedParameterJdbcTemplate(jdbcTemplate) {
//            @Override
//            public <T> List<T> query(String sql, SqlParameterSource paramSource, RowMapper<T> rowMapper)
//                    throws DataAccessException {
//                final long startTime = logger.isDebugEnabled() ? System.currentTimeMillis() : 0;
//                List<T> result = super.query(sql, paramSource, rowMapper);
//                if (logger.isDebugEnabled()) {
//                    logger.debug("Query was executed and data extracted in {} ms, Query: [{}]", System.currentTimeMillis() - startTime, sql);
//                }
//                return result;
//            }
//        };
//    }

    @Autowired
    @Bean(name="clickHouseClient")
    @Lazy
    public ClickhouseClient clickHouseClient(@Qualifier("clickHouseDataSource") DataSource clickHouseDataSource) {
        return new ClickhouseClient(clickHouseDataSource);
    }
}
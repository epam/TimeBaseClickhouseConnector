package deltix.timebase.connector.clickhouse.configuration.beans;

import deltix.clickhouse.ClickhouseClient;
import deltix.timebase.connector.clickhouse.configuration.properties.ClickhouseProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.ArgumentPreparedStatementSetter;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.PreparedStatementSetter;
import ru.yandex.clickhouse.ClickHouseDataSource;
import ru.yandex.clickhouse.settings.ClickHouseProperties;

import javax.sql.DataSource;

@Configuration
public class ClickhouseConfig {
    private final static Logger logger = LoggerFactory.getLogger(ClickhouseConfig.class);

    @Autowired
    @Bean(name="clickHouseDataSource")
    public DataSource clickHouseDataSource(ClickhouseProperties clickhouseProperties) {
        final ClickHouseProperties clickHouseProperties = new ClickHouseProperties();
        clickHouseProperties.setUser(clickhouseProperties.getUsername());
        clickHouseProperties.setPassword(clickhouseProperties.getPassword());
        clickHouseProperties.setSocketTimeout(clickhouseProperties.getTimeout() * 1000);
        return new ClickHouseDataSource(clickhouseProperties.getUrl(), clickHouseProperties);
    }

    @Autowired
    @Bean(name="clickHouseJdbcTemplate")
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
    public ClickhouseClient clickHouseClient(@Qualifier("clickHouseDataSource") DataSource clickHouseDataSource) {
        return new ClickhouseClient(clickHouseDataSource);
    }
}

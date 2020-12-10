package org.apache.ibatis.r2dbc.impl;

import io.r2dbc.pool.ConnectionPool;
import io.r2dbc.pool.ConnectionPoolConfiguration;
import io.r2dbc.spi.ConnectionFactories;
import io.r2dbc.spi.ConnectionFactory;
import org.apache.ibatis.r2dbc.ReactiveSqlSession;
import org.apache.ibatis.r2dbc.ReactiveSqlSessionFactory;
import org.apache.ibatis.reflection.MetaObject;
import org.apache.ibatis.session.Configuration;

import javax.sql.DataSource;
import java.io.IOException;
import java.time.Duration;
import java.util.Properties;

/**
 * Default ReactiveSqlSessionFactory
 *
 * @author linux_china
 */
public class DefaultReactiveSqlSessionFactory implements ReactiveSqlSessionFactory {
    private final Configuration configuration;
    private final ConnectionFactory connectionFactory;
    private final ReactiveSqlSession sqlSession;

    public DefaultReactiveSqlSessionFactory(Configuration configuration) {
        this.configuration = configuration;
        DataSource dataSource = configuration.getEnvironment().getDataSource();
        MetaObject metaObject = configuration.newMetaObject(dataSource);
        String username = (String) metaObject.getValue("username");
        String password = (String) metaObject.getValue("password");
        String jdbcUrl = (String) metaObject.getValue("url");
        String r2dbcUrl = (String) configuration.getVariables().get("r2dbc.url");
        if (r2dbcUrl == null && jdbcUrl != null) {
            String credential = username + (password == null || password.isEmpty() ? "" : ":" + password);
            r2dbcUrl = jdbcUrl.replace("jdbc:", "r2dbc:");
            r2dbcUrl = r2dbcUrl.replace("r2dbc:mysql:", "r2dbc:mariadb:");
            r2dbcUrl = r2dbcUrl.replace("//", "//" + credential + "@");
        }
        assert r2dbcUrl != null;
        ConnectionFactory connectionFactory = ConnectionFactories.get(r2dbcUrl);
        this.connectionFactory = connectionPool(connectionFactory);
        this.sqlSession = new DefaultReactiveSqlSession(configuration, this.connectionFactory);
    }

    public DefaultReactiveSqlSessionFactory(Configuration configuration, ConnectionFactory connectionFactory) {
        this.configuration = configuration;
        this.connectionFactory = connectionFactory;
        this.sqlSession = new DefaultReactiveSqlSession(configuration, this.connectionFactory);
    }

    @Override
    public ReactiveSqlSession openSession() {
        return this.sqlSession;
    }

    @Override
    public Configuration getConfiguration() {
        return this.configuration;
    }

    @Override
    public ConnectionFactory getConnectionFactory() {
        return this.connectionFactory;
    }

    @Override
    public void close() throws IOException {
        if (this.connectionFactory instanceof ConnectionPool) {
            ConnectionPool connectionPool = ((ConnectionPool) this.connectionFactory);
            if (!connectionPool.isDisposed()) {
                connectionPool.dispose();
            }
        }
    }

    private ConnectionPool connectionPool(ConnectionFactory connectionFactory) {
        if (connectionFactory instanceof ConnectionPool) {
            return (ConnectionPool) connectionFactory;
        }
        int initialSize = 1;
        int maxSize = 10;
        Duration maxIdleTime = Duration.ofMinutes(30);
        Properties variables = configuration.getVariables();
        if (variables.containsKey("r2dbc.pool.initial-size")) {
            initialSize = Integer.parseInt(variables.getProperty("r2dbc.pool.initial-size"));
        }
        if (variables.containsKey("r2dbc.pool.max-size")) {
            maxSize = Integer.parseInt(variables.getProperty("r2dbc.pool.max-size"));
        }
        if (variables.containsKey("r2dbc.pool.max-idle-time")) {
            maxIdleTime = Duration.ofMinutes(Integer.parseInt(variables.getProperty("r2dbc.pool.max-idle-time")));
        }
        ConnectionPoolConfiguration configuration = ConnectionPoolConfiguration.builder(connectionFactory)
                .maxSize(maxSize)
                .initialSize(initialSize)
                .maxIdleTime(maxIdleTime)
                .build();
        return new ConnectionPool(configuration);
    }
}

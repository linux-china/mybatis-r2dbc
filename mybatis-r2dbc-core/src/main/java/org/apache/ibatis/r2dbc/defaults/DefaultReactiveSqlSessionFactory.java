package org.apache.ibatis.r2dbc.defaults;

import io.r2dbc.spi.ConnectionFactory;
import org.apache.ibatis.r2dbc.ReactiveSqlSession;
import org.apache.ibatis.r2dbc.ReactiveSqlSessionFactory;
import org.apache.ibatis.r2dbc.connection.DefaultTransactionSupportConnectionFactory;
import org.apache.ibatis.r2dbc.delegate.R2dbcMybatisConfiguration;
import org.apache.ibatis.r2dbc.executor.DefaultReactiveMybatisExecutor;
import org.apache.ibatis.r2dbc.executor.ReactiveMybatisExecutor;

import java.io.Closeable;
import java.util.Objects;

/**
 * @author chenggang
 * @date 12/8/21.
 */
public class DefaultReactiveSqlSessionFactory implements ReactiveSqlSessionFactory {

    private final R2dbcMybatisConfiguration configuration;
    private final ReactiveSqlSession reactiveSqlSession;

    private DefaultReactiveSqlSessionFactory(R2dbcMybatisConfiguration configuration, ReactiveMybatisExecutor reactiveMybatisExecutor) {
        this.configuration = configuration;
        this.reactiveSqlSession = new DefaultReactiveSqlSession(this.configuration, reactiveMybatisExecutor);
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    @Override
    public ReactiveSqlSession openSession() {
        return this.reactiveSqlSession;
    }

    @Override
    public R2dbcMybatisConfiguration getConfiguration() {
        return this.configuration;
    }

    @Override
    public void close() throws Exception {
        if (this.configuration.getConnectionFactory() instanceof Closeable) {
            Closeable closeableConnectionFactory = ((Closeable) this.configuration.getConnectionFactory());
            closeableConnectionFactory.close();
        }
    }

    public static class Builder {

        private R2dbcMybatisConfiguration r2dbcMybatisConfiguration;
        private ConnectionFactory connectionFactory;
        private ReactiveMybatisExecutor reactiveMybatisExecutor;
        private boolean usingDefaultConnectionFactoryProxy = true;

        /**
         * Target R2dbcMybatisConfiguration Must Not Be Null
         *
         * @param r2dbcMybatisConfiguration
         * @return
         */
        public Builder withR2dbcMybatisConfiguration(R2dbcMybatisConfiguration r2dbcMybatisConfiguration) {
            Objects.requireNonNull(r2dbcMybatisConfiguration, "R2dbcMybatisConfiguration Could not be null");
            this.r2dbcMybatisConfiguration = r2dbcMybatisConfiguration;
            return this;
        }

        /**
         * Specific ConnectionFactory Must Not Be Null
         *
         * @param connectionFactory
         * @return
         */
        public Builder withConnectionFactory(ConnectionFactory connectionFactory) {
            Objects.requireNonNull(connectionFactory, "ConnectionFactory Could not be null");
            this.connectionFactory = connectionFactory;
            return this;
        }

        /**
         * Specific ReactiveMybatisExecutor
         *
         * @param reactiveMybatisExecutor
         * @return
         */
        public Builder withReactiveMybatisExecutor(ReactiveMybatisExecutor reactiveMybatisExecutor) {
            Objects.requireNonNull(reactiveMybatisExecutor, "ReactiveMybatisExecutor Could not be null");
            this.reactiveMybatisExecutor = reactiveMybatisExecutor;
            return this;
        }

        /**
         * whether using default connection factory proxy or not ,default is true
         *
         * @param usingDefault
         * @return
         */
        public Builder withDefaultConnectionFactoryProxy(boolean usingDefault) {
            this.usingDefaultConnectionFactoryProxy = usingDefault;
            return this;
        }

        /**
         * build DefaultReactiveSqlSessionFactory
         *
         * @return
         */
        public DefaultReactiveSqlSessionFactory build() {
            Objects.requireNonNull(this.r2dbcMybatisConfiguration, "R2dbcMybatisConfiguration Could not be null");
            if (Objects.isNull(this.r2dbcMybatisConfiguration.getConnectionFactory()) && Objects.isNull(this.connectionFactory)) {
                throw new IllegalArgumentException("ConnectionFactory Could not be null");
            }
            if (Objects.nonNull(this.connectionFactory)) {
                this.r2dbcMybatisConfiguration.setConnectionFactory(this.connectionFactory);
            }
            if (Objects.nonNull(this.reactiveMybatisExecutor)) {
                return new DefaultReactiveSqlSessionFactory(this.r2dbcMybatisConfiguration, this.reactiveMybatisExecutor);
            }
            if (usingDefaultConnectionFactoryProxy) {
                ConnectionFactory transactionSupportConnectionFactory = new DefaultTransactionSupportConnectionFactory(this.r2dbcMybatisConfiguration.getConnectionFactory());
                this.r2dbcMybatisConfiguration.setConnectionFactory(transactionSupportConnectionFactory);
                return new DefaultReactiveSqlSessionFactory(this.r2dbcMybatisConfiguration, new DefaultReactiveMybatisExecutor(this.r2dbcMybatisConfiguration));
            }
            return new DefaultReactiveSqlSessionFactory(this.r2dbcMybatisConfiguration, new DefaultReactiveMybatisExecutor(this.r2dbcMybatisConfiguration));
        }
    }
}

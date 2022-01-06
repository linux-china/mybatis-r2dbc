package org.apache.ibatis.r2dbc.delegate;

import io.r2dbc.spi.ConnectionFactory;
import org.apache.ibatis.mapping.Environment;
import org.apache.ibatis.mapping.MappedStatement;
import org.apache.ibatis.r2dbc.ReactiveSqlSession;
import org.apache.ibatis.r2dbc.executor.support.R2dbcStatementLog;
import org.apache.ibatis.r2dbc.executor.support.R2dbcStatementLogFactory;
import org.apache.ibatis.r2dbc.executor.type.R2dbcTypeHandlerAdapter;
import org.apache.ibatis.r2dbc.executor.type.R2dbcTypeHandlerAdapterRegistry;
import org.apache.ibatis.session.Configuration;

import java.io.InputStream;
import java.io.Reader;
import java.io.StringReader;
import java.sql.SQLXML;
import java.util.HashSet;
import java.util.Set;

/**
 * @author chenggang
 * @date 12/8/21.
 */
public class R2dbcMybatisConfiguration extends Configuration {

    private final R2dbcMapperRegistry r2dbcMapperRegistry = new R2dbcMapperRegistry(this);
    private final R2dbcTypeHandlerAdapterRegistry r2dbcTypeHandlerAdapterRegistry = new R2dbcTypeHandlerAdapterRegistry(this);
    private final R2dbcStatementLogFactory r2dbcStatementLogFactory = new R2dbcStatementLogFactory(this);
    private final Set<Class> notSupportedDataTypes = new HashSet<>();
    private ConnectionFactory connectionFactory;

    public R2dbcMybatisConfiguration() {
        this.loadNotSupportedDataTypes();
    }

    public R2dbcMybatisConfiguration(Environment environment) {
        super(environment);
        this.loadNotSupportedDataTypes();
    }

    /**
     * load not supported data types
     */
    private void loadNotSupportedDataTypes() {
        this.notSupportedDataTypes.add(InputStream.class);
        this.notSupportedDataTypes.add(SQLXML.class);
        this.notSupportedDataTypes.add(Reader.class);
        this.notSupportedDataTypes.add(StringReader.class);
    }

    public <T> T getMapper(Class<T> type, ReactiveSqlSession reactiveSqlSession) {
        return r2dbcMapperRegistry.getMapper(type, reactiveSqlSession);
    }

    public ConnectionFactory getConnectionFactory() {
        return connectionFactory;
    }

    public void setConnectionFactory(ConnectionFactory connectionFactory) {
        this.connectionFactory = connectionFactory;
    }

    @Override
    public <T> void addMapper(Class<T> type) {
        this.r2dbcMapperRegistry.addMapper(type);
    }

    @Override
    public void addMappers(String packageName, Class<?> superType) {
        this.r2dbcMapperRegistry.addMappers(packageName, superType);
    }

    @Override
    public void addMappers(String packageName) {
        this.r2dbcMapperRegistry.addMappers(packageName);
    }

    @Override
    public boolean hasMapper(Class<?> type) {
        return this.r2dbcMapperRegistry.hasMapper(type);
    }

    @Override
    public void addMappedStatement(MappedStatement ms) {
        super.addMappedStatement(ms);
        this.r2dbcStatementLogFactory.initR2dbcStatementLog(ms);
    }

    public void addR2dbcTypeHandlerAdapter(R2dbcTypeHandlerAdapter r2dbcTypeHandlerAdapter) {
        this.r2dbcTypeHandlerAdapterRegistry.register(r2dbcTypeHandlerAdapter);
    }

    public void addR2dbcTypeHandlerAdapter(String packageName) {
        this.r2dbcTypeHandlerAdapterRegistry.register(packageName);
    }

    public R2dbcTypeHandlerAdapterRegistry getR2dbcTypeHandlerAdapterRegistry() {
        return r2dbcTypeHandlerAdapterRegistry;
    }

    public void setNotSupportedJdbcType(Class clazz) {
        this.notSupportedDataTypes.add(clazz);
    }

    public Set<Class> getNotSupportedDataTypes() {
        return this.notSupportedDataTypes;
    }

    /**
     * get r2dbc statement log
     * @param mappedStatement target MappedStatement
     * @return the R2dbcStatementLog
     */
    public R2dbcStatementLog getR2dbcStatementLog(MappedStatement mappedStatement) {
        return this.r2dbcStatementLogFactory.getR2dbcStatementLog(mappedStatement);
    }

    /**
     * get r2dbc statement log factory
     * @return the R2dbcStatementLogFactory
     */
    public R2dbcStatementLogFactory getR2dbcStatementLogFactory() {
        return r2dbcStatementLogFactory;
    }
}

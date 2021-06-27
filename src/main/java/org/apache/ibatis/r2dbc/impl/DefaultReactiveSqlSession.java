package org.apache.ibatis.r2dbc.impl;

import io.r2dbc.spi.*;
import org.apache.ibatis.mapping.*;
import org.apache.ibatis.r2dbc.ReactiveSqlSession;
import org.apache.ibatis.r2dbc.binding.MapperProxyFactory;
import org.apache.ibatis.r2dbc.type.R2DBCTypeHandler;
import org.apache.ibatis.r2dbc.type.TypeHandlerRegistry;
import org.apache.ibatis.reflection.MetaObject;
import org.apache.ibatis.reflection.factory.ObjectFactory;
import org.apache.ibatis.session.Configuration;
import org.apache.ibatis.session.RowBounds;
import org.apache.ibatis.type.TypeException;
import org.apache.ibatis.type.TypeHandler;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.*;

/**
 * reactive sql session default implementation
 *
 * @author linux_china
 */
@SuppressWarnings("unchecked")
public class DefaultReactiveSqlSession implements ReactiveSqlSession {
    private final List<Class<?>> NUMBER_TYPES = Arrays.asList(byte.class, short.class, int.class, long.class, float.class, double.class,
            Byte.class, Short.class, Integer.class, Long.class, Float.class, Double.class);
    private final TypeHandlerRegistry typeHandlerRegistry = new TypeHandlerRegistry();
    private final Configuration configuration;
    private final ObjectFactory objectFactory;
    private final ConnectionFactory connectionFactory;
    private final boolean metricsEnabled;

    public DefaultReactiveSqlSession(Configuration configuration, ConnectionFactory connectionFactory) {
        this.configuration = configuration;
        this.objectFactory = this.configuration.getObjectFactory();
        //register r2dbc type handlers
        registerTypeHandlers(configuration);
        //noinspection unchecked
        this.connectionFactory = connectionFactory;
        //metrics enabled
        this.metricsEnabled = Boolean.parseBoolean(configuration.getVariables().getProperty("metrics.enabled", "false"));
    }

    private void registerTypeHandlers(Configuration configuration) {
        for (TypeHandler<?> typeHandler : configuration.getTypeHandlerRegistry().getTypeHandlers()) {
            if (typeHandler instanceof R2DBCTypeHandler) {
                R2DBCTypeHandler<?> r2DBCTypeHandler = (R2DBCTypeHandler<?>) typeHandler;
                typeHandlerRegistry.register(r2DBCTypeHandler);
            }
        }
    }

    @Override
    public <T> Mono<T> selectOne(String statementId, Object parameter) {
        MappedStatement mappedStatement = configuration.getMappedStatement(statementId);
        BoundSql boundSql = mappedStatement.getBoundSql(parameter);
        Mono<T> rowSelected = getConnection().flatMap(connection -> {
            Statement statement = connection.createStatement(boundSql.getSql());
            if (parameter != null) {
                fillParams(statement, boundSql, parameter);
            }
            ResultMap resultMap = mappedStatement.getResultMaps().get(0);
            return executeFluxStatement(connection, statement)
                    .flatMap(result -> result.map((row, rowMetadata) -> (T) convertRowToResult(row, rowMetadata, resultMap)))
                    .last();
        });
        if (metricsEnabled) {
            return rowSelected.name(statementId).metrics();
        } else {
            return rowSelected;
        }
    }

    @Override
    public <T> Flux<T> select(String statementId, Object parameter) {
        MappedStatement mappedStatement = configuration.getMappedStatement(statementId);
        BoundSql boundSql = mappedStatement.getBoundSql(parameter);
        Flux<T> rowsSelected = getConnection().flatMapMany(connection -> {
            Statement statement = connection.createStatement(boundSql.getSql());
            if (parameter != null) {
                fillParams(statement, boundSql, parameter);
            }
            ResultMap resultMap = mappedStatement.getResultMaps().get(0);
            return executeFluxStatement(connection, statement)
                    .flatMap(result -> result.map((row, rowMetadata) -> (T) convertRowToResult(row, rowMetadata, resultMap)));
        });
        if (metricsEnabled) {
            return rowsSelected.name(statementId).metrics();
        } else {
            return rowsSelected;
        }
    }

    @Override
    public <T> Flux<T> select(String statementId, Object parameter, RowBounds rowBounds) {
        return (Flux<T>) select(statementId, parameter).skip(rowBounds.getOffset()).take(rowBounds.getLimit(),true);
    }

    @Override
    public Mono<Integer> insert(String statementId, Object parameter) {
        MappedStatement mappedStatement = configuration.getMappedStatement(statementId);
        BoundSql boundSql = mappedStatement.getBoundSql(parameter);
        Mono<Integer> rowsUpdated = getConnection().flatMap(connection -> {
            Statement statement = connection.createStatement(boundSql.getSql());
            final boolean useGeneratedKeys = mappedStatement.getKeyGenerator() != null && mappedStatement.getKeyProperties() != null;
            if (useGeneratedKeys) {
                statement.returnGeneratedValues(mappedStatement.getKeyProperties());
            }
            if (parameter != null) {
                fillParams(statement, boundSql, parameter);
            }
            return executeMonoStatement(connection, statement)
                    .flatMap(result -> {
                        if (!useGeneratedKeys) {
                            return Mono.from(result.getRowsUpdated());
                        } else {
                            return Mono.from(result.map((row, rowMetadata) -> {
                                MetaObject parameterMetaObject = configuration.newMetaObject(parameter);
                                for (String keyProperty : mappedStatement.getKeyProperties()) {
                                    Object value = row.get(keyProperty, parameterMetaObject.getSetterType(keyProperty));
                                    parameterMetaObject.setValue(keyProperty, value);
                                }
                                return 1;
                            }));
                        }
                    });
        });
        if (metricsEnabled) {
            return rowsUpdated.name(statementId).metrics();
        } else {
            return rowsUpdated;
        }
    }

    @Override
    public Mono<Integer> delete(String statementId, Object parameter) {
        return update(statementId, parameter);
    }

    @Override
    public Mono<Integer> update(String statementId, Object parameter) {
        MappedStatement mappedStatement = configuration.getMappedStatement(statementId);
        BoundSql boundSql = mappedStatement.getBoundSql(parameter);
        Mono<Integer> updatedRows = getConnection().flatMap(connection -> {
            Statement statement = connection.createStatement(boundSql.getSql());
            if (parameter != null) {
                fillParams(statement, boundSql, parameter);
            }
            return executeMonoStatement(connection, statement)
                    .flatMap(result -> Mono.from(result.getRowsUpdated()));
        });
        if (metricsEnabled) {
            return updatedRows.name(statementId).metrics();
        } else {
            return updatedRows;
        }
    }

    @Override
    public <T> T getMapper(Class<T> clazz) {
        MapperProxyFactory<T> mapperProxyFactory = new MapperProxyFactory<>(clazz);
        return mapperProxyFactory.newInstance(this);
    }


    @Override
    public Configuration getConfiguration() {
        return this.configuration;
    }

    public void fillParams(Statement statement, BoundSql boundSql, Object parameter) {
        List<ParameterMapping> parameterMappings = boundSql.getParameterMappings();
        if (parameterMappings != null) {
            for (int i = 0; i < parameterMappings.size(); i++) {
                ParameterMapping parameterMapping = parameterMappings.get(i);
                if (parameterMapping.getMode() != ParameterMode.OUT) {
                    Object value;
                    String propertyName = parameterMapping.getProperty();
                    if (boundSql.hasAdditionalParameter(propertyName)) {
                        value = boundSql.getAdditionalParameter(propertyName);
                    } else if (parameter == null) {
                        value = null;
                    } else if (typeHandlerRegistry.hasTypeHandler(parameter.getClass())) {
                        value = parameter;
                    } else {
                        MetaObject metaObject = configuration.newMetaObject(parameter);
                        value = metaObject.getValue(propertyName);
                    }
                    TypeHandler<?> typeHandler = parameterMapping.getTypeHandler();
                    try {
                        if (typeHandler instanceof R2DBCTypeHandler) {
                            ((R2DBCTypeHandler<Object>) typeHandler).setParameter(statement, i, value, parameterMapping.getJdbcType());
                        } else {
                            if (value == null) {
                                statement.bindNull(i, parameterMapping.getJavaType());
                            } else {
                                Class<?> parameterClass = value.getClass();
                                if (typeHandlerRegistry.hasTypeHandler(parameterClass)) {
                                    typeHandlerRegistry.getTypeHandler(parameterClass).setParameter(statement, i, value, parameterMapping.getJdbcType());
                                } else {
                                    statement.bind(i, value);
                                }
                            }
                        }
                    } catch (TypeException e) {
                        throw new TypeException("Could not set parameters for mapping: " + parameterMapping + ". Cause: " + e, e);
                    }
                }
            }
        }
    }

    public Object convertRowToResult(Row row, RowMetadata rowMetadata, ResultMap resultMap) {
        //number
        Class<?> type = resultMap.getType();
        boolean pojoMapping = !resultMap.getResultMappings().isEmpty();
        if (NUMBER_TYPES.contains(type)) {
            Number columnValue = (Number) row.get(0);
            if (columnValue == null) {
                return null;
            }
            if (type.equals(columnValue.getClass())) {
                return columnValue;
            } else if (type.equals(Byte.class) || type.equals(byte.class)) {
                return columnValue.byteValue();
            } else if (type.equals(Short.class) || type.equals(short.class)) {
                return columnValue.shortValue();
            } else if (type.equals(Integer.class) || type.equals(int.class)) {
                return columnValue.intValue();
            } else if (type.equals(Long.class) || type.equals(long.class)) {
                return columnValue.longValue();
            } else if (type.equals(Float.class) || type.equals(float.class)) {
                return columnValue.floatValue();
            } else if (type.equals(Double.class) || type.equals(double.class)) {
                return columnValue.doubleValue();
            } else {
                return columnValue;
            }
        } else if (typeHandlerRegistry.hasTypeHandler(type)) {
            R2DBCTypeHandler<?> mappingTypeHandler = typeHandlerRegistry.getTypeHandler(type);
            return mappingTypeHandler.getResult(row, 0, rowMetadata);
        } else if (pojoMapping) {
            Object object = objectFactory.create(type);
            MetaObject resultMetaObject = configuration.newMetaObject(object);
            List<ResultMapping> resultMappings = resultMap.getResultMappings();
            if (!resultMappings.isEmpty()) {
                for (ResultMapping resultMapping : resultMappings) {
                    Class<?> javaType = resultMapping.getJavaType();
                    Object columnValue;
                    TypeHandler<?> typeHandler = resultMapping.getTypeHandler();
                    if (typeHandler instanceof R2DBCTypeHandler) {
                        columnValue = ((R2DBCTypeHandler<?>) typeHandler).getResult(row, resultMapping.getColumn(), rowMetadata);
                    } else if (typeHandlerRegistry.hasTypeHandler(javaType)) {
                        columnValue = typeHandlerRegistry.getTypeHandler(javaType).getResult(row, resultMapping.getColumn(), rowMetadata);
                    } else {
                        columnValue = row.get(resultMapping.getColumn(), javaType);
                    }
                    resultMetaObject.setValue(resultMapping.getProperty(), columnValue);
                }
            } else {
                rowMetadata.getColumnNames().forEach(column -> {
                    Object columnValue = row.get(column);
                    resultMetaObject.setValue(column, columnValue);
                });
            }
            return object;
        } else if (type.isAssignableFrom(Map.class)) {
            Map<String, Object> result = new HashMap<>();
            for (String columnName : rowMetadata.getColumnNames()) {
                result.put(columnName, row.get(columnName));
            }
            return result;
        } else if (type.isAssignableFrom(Collection.class)) {
            List<Object> result = new ArrayList<>();
            for (String columnName : rowMetadata.getColumnNames()) {
                result.add(row.get(columnName));
            }
            return result;
        } else {
            return row.get(0, type);
        }
    }

    private Flux<? extends Result> executeFluxStatement(Connection connection, Statement statement) {
        return Flux.from(statement.execute())
                .doFinally(a -> ((Mono) connection.close()).subscribe());
    }

    private Mono<? extends Result> executeMonoStatement(Connection connection, Statement statement) {
        return Mono.from(statement.execute())
                .doFinally(a -> ((Mono) connection.close()).subscribe());
    }

    private Mono<Connection> getConnection() {
        Mono<Connection> connection = (Mono<Connection>) connectionFactory.create();
        return connection;
    }

}
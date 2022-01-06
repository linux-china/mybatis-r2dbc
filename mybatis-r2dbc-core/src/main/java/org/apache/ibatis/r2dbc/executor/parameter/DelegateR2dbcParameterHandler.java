package org.apache.ibatis.r2dbc.executor.parameter;

import io.r2dbc.spi.Statement;
import org.apache.ibatis.executor.parameter.ParameterHandler;
import org.apache.ibatis.logging.Log;
import org.apache.ibatis.logging.LogFactory;
import org.apache.ibatis.mapping.BoundSql;
import org.apache.ibatis.mapping.ParameterMapping;
import org.apache.ibatis.mapping.ParameterMode;
import org.apache.ibatis.r2dbc.delegate.R2dbcMybatisConfiguration;
import org.apache.ibatis.r2dbc.executor.support.R2dbcStatementLog;
import org.apache.ibatis.r2dbc.executor.type.R2dbcTypeHandlerAdapter;
import org.apache.ibatis.r2dbc.support.ProxyInstanceFactory;
import org.apache.ibatis.reflection.MetaObject;
import org.apache.ibatis.type.JdbcType;
import org.apache.ibatis.type.TypeException;
import org.apache.ibatis.type.TypeHandler;
import org.apache.ibatis.type.TypeHandlerRegistry;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * @author chenggang
 * @date 12/9/21.
 */
public class DelegateR2dbcParameterHandler implements InvocationHandler {

    private static final Log log = LogFactory.getLog(DelegateR2dbcParameterHandler.class);

    private final R2dbcMybatisConfiguration configuration;
    private final ParameterHandler parameterHandler;
    private final Map<Class<?>, Field> parameterHandlerFieldMap;
    private final Statement delegateStatement;
    private final PreparedStatement delegatedPreparedStatement;
    private final AtomicReference<ParameterHandlerContext> parameterHandlerContextReference = new AtomicReference<>();
    private final R2dbcStatementLog r2dbcStatementLog;

    public DelegateR2dbcParameterHandler(R2dbcMybatisConfiguration r2DbcMybatisConfiguration,
                                         ParameterHandler parameterHandler,
                                         Statement statement,
                                         R2dbcStatementLog r2dbcStatementLog) {
        this.configuration = r2DbcMybatisConfiguration;
        this.parameterHandler = parameterHandler;
        this.delegateStatement = statement;
        this.r2dbcStatementLog = r2dbcStatementLog;
        this.delegatedPreparedStatement = initDelegatedPreparedStatement();
        parameterHandlerFieldMap = Stream.of(parameterHandler.getClass().getDeclaredFields())
                .collect(Collectors.toMap(
                        Field::getType,
                        field -> {
                            field.setAccessible(true);
                            return field;
                        }
                ));
    }

    /**
     * init delegated prepared statement
     *
     * @return
     */
    private PreparedStatement initDelegatedPreparedStatement() {
        return ProxyInstanceFactory.newInstanceOfInterfaces(
                PreparedStatement.class,
                () -> new DelegateR2dbcStatement(
                        this.delegateStatement,
                        this.configuration.getR2dbcTypeHandlerAdapterRegistry().getR2dbcTypeHandlerAdapters(),
                        this.configuration.getNotSupportedDataTypes()
                )
        );
    }

    @Override
    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
        String methodName = method.getName();
        if (!Objects.equals("setParameters", methodName)) {
            return method.invoke(parameterHandler, args);
        }
        this.setParameters(this.delegatedPreparedStatement);
        return null;
    }

    /**
     * get field
     *
     * @param parameterHandler
     * @param fieldType
     * @param <T>
     * @return
     */
    private <T> T getField(ParameterHandler parameterHandler, Class<T> fieldType) {
        Field field = this.parameterHandlerFieldMap.get(fieldType);
        try {
            return (T) field.get(parameterHandler);
        } catch (IllegalAccessException e) {
            //ignore
        }
        return null;
    }

    /**
     * delegate set parameters
     *
     * @param ps
     */
    public void setParameters(PreparedStatement ps) {
        BoundSql boundSql = this.getField(this.parameterHandler, BoundSql.class);
        TypeHandlerRegistry typeHandlerRegistry = this.getField(this.parameterHandler, TypeHandlerRegistry.class);
        Object parameterObject = parameterHandler.getParameterObject();
        List<ParameterMapping> parameterMappings = boundSql.getParameterMappings();
        ParameterHandlerContext parameterHandlerContext = new ParameterHandlerContext();
        DelegateR2dbcParameterHandler.this.parameterHandlerContextReference.getAndSet(parameterHandlerContext);
        List<Object> columnValues = new ArrayList<>();
        if (parameterMappings != null) {
            for (int i = 0; i < parameterMappings.size(); i++) {
                ParameterMapping parameterMapping = parameterMappings.get(i);
                if (parameterMapping.getMode() != ParameterMode.OUT) {
                    Object value;
                    String propertyName = parameterMapping.getProperty();
                    // issue #448 ask first for additional params
                    if (boundSql.hasAdditionalParameter(propertyName)) {
                        value = boundSql.getAdditionalParameter(propertyName);
                    } else if (parameterObject == null) {
                        value = null;
                    } else if (typeHandlerRegistry.hasTypeHandler(parameterObject.getClass())) {
                        value = parameterObject;
                    } else {
                        MetaObject metaObject = configuration.newMetaObject(parameterObject);
                        value = metaObject.getValue(propertyName);
                    }
                    TypeHandler typeHandler = parameterMapping.getTypeHandler();
                    JdbcType jdbcType = parameterMapping.getJdbcType();
                    if (value == null && jdbcType == null) {
                        jdbcType = configuration.getJdbcTypeForNull();
                    }
                    try {
                        if (value == null && jdbcType != null) {
                            this.delegateStatement.bindNull(i, parameterMapping.getJavaType());
                            columnValues.add(null);
                        } else {
                            parameterHandlerContext.setIndex(i);
                            parameterHandlerContext.setJavaType(parameterMapping.getJavaType());
                            parameterHandlerContext.setJdbcType(jdbcType);
                            typeHandler.setParameter(ps, i, value, jdbcType);
                            columnValues.add(value);
                        }
                    } catch (TypeException | SQLException e) {
                        throw new TypeException("Could not set parameters for mapping: " + parameterMapping + ". Cause: " + e, e);
                    }
                }
            }
        }
        r2dbcStatementLog.logParameters(columnValues);
    }


    /**
     * delegate Prepare statement
     */
    private class DelegateR2dbcStatement implements InvocationHandler {

        private final Statement statement;
        private final Map<Class, R2dbcTypeHandlerAdapter> r2dbcTypeHandlerAdapters;
        private final Set<Class> notSupportedDataTypes;

        DelegateR2dbcStatement(Statement statement, Map<Class, R2dbcTypeHandlerAdapter> r2dbcTypeHandlerAdapters, Set<Class> notSupportedDataTypes) {
            this.statement = statement;
            this.r2dbcTypeHandlerAdapters = r2dbcTypeHandlerAdapters;
            this.notSupportedDataTypes = notSupportedDataTypes;
        }

        @Override
        public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
            String methodName = method.getName();
            if (!methodName.startsWith("set")) {
                //not handle no set method
                return null;
            }
            int index = (int) args[0];
            Object parameter = args[1];
            Class<?> parameterClass = parameter.getClass();
            //not supported types
            if (notSupportedDataTypes.contains(parameterClass)) {
                throw new IllegalArgumentException("Unsupported Parameter type : " + parameterClass);
            }
            // using adapter
            if (r2dbcTypeHandlerAdapters.containsKey(parameterClass)) {
                log.debug("Found r2dbc type handler adapter for type : " + parameterClass);
                R2dbcTypeHandlerAdapter r2dbcTypeHandlerAdapter = r2dbcTypeHandlerAdapters.get(parameterClass);
                ParameterHandlerContext parameterHandlerContext = DelegateR2dbcParameterHandler.this.parameterHandlerContextReference.get();
                r2dbcTypeHandlerAdapter.setParameter(statement, parameterHandlerContext, parameter);
                return null;
            }
            //default set
            statement.bind(index, parameter);
            return null;
        }

    }

}

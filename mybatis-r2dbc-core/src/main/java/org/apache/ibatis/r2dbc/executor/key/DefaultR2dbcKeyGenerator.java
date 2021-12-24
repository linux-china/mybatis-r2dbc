package org.apache.ibatis.r2dbc.executor.key;

import org.apache.ibatis.binding.MapperMethod.ParamMap;
import org.apache.ibatis.executor.ExecutorException;
import org.apache.ibatis.mapping.MappedStatement;
import org.apache.ibatis.r2dbc.delegate.R2dbcMybatisConfiguration;
import org.apache.ibatis.r2dbc.executor.result.RowResultWrapper;
import org.apache.ibatis.r2dbc.executor.result.TypeHandleContext;
import org.apache.ibatis.r2dbc.executor.result.handler.DelegateR2dbcResultRowDataHandler;
import org.apache.ibatis.r2dbc.support.ProxyInstanceFactory;
import org.apache.ibatis.reflection.ArrayUtil;
import org.apache.ibatis.reflection.MetaObject;
import org.apache.ibatis.reflection.ParamNameResolver;
import org.apache.ibatis.type.TypeHandler;
import org.apache.ibatis.type.TypeHandlerRegistry;
import org.apache.ibatis.util.MapUtil;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.atomic.LongAdder;

/**
 * @author: chenggang
 * @date 12/12/21.
 */
public class DefaultR2dbcKeyGenerator implements R2dbcKeyGenerator {

    private static final String SECOND_GENERIC_PARAM_NAME = ParamNameResolver.GENERIC_NAME_PREFIX + "2";

    private static final String MSG_TOO_MANY_KEYS = "Too many keys are generated. There are only %d target objects. "
            + "You either specified a wrong 'keyProperty' or encountered a driver bug like #1523.";

    private final LongAdder resultRowCounter = new LongAdder();
    private final MappedStatement mappedStatement;
    private final R2dbcMybatisConfiguration r2DbcMybatisConfiguration;

    public DefaultR2dbcKeyGenerator(MappedStatement mappedStatement, R2dbcMybatisConfiguration r2DbcMybatisConfiguration) {
        this.mappedStatement = mappedStatement;
        this.r2DbcMybatisConfiguration = r2DbcMybatisConfiguration;
    }

    private static String nameOfSingleParam(Map<String, ?> paramMap) {
        // There is virtually one parameter, so any key works.
        return paramMap.keySet().iterator().next();
    }

    private static List<?> collectionize(Object param) {
        if (param instanceof Collection) {
            return new ArrayList<Object>((Collection) param);
        } else if (param instanceof Object[]) {
            return Arrays.asList((Object[]) param);
        } else {
            return Arrays.asList(param);
        }
    }

    @Override
    public Integer getResultRowCount() {
        return this.resultRowCounter.intValue();
    }

    @Override
    public Integer handleKeyResult(RowResultWrapper rowResultWrapper, Object parameter) {
        this.assignKeys(r2DbcMybatisConfiguration, rowResultWrapper, mappedStatement.getKeyProperties(), parameter);
        this.resultRowCounter.increment();
        return 1;
    }

    @SuppressWarnings("unchecked")
    private void assignKeys(R2dbcMybatisConfiguration configuration,
                            RowResultWrapper rowResultWrapper,
                            String[] keyProperties,
                            Object parameter) {
        if (parameter instanceof ParamMap) {
            // Multi-param or single param with @Param
            assignKeysToParamMap(configuration, rowResultWrapper, keyProperties, (Map<String, ?>) parameter);
        } else if (parameter instanceof ArrayList && !((ArrayList<?>) parameter).isEmpty()
                && ((ArrayList<?>) parameter).get(0) instanceof ParamMap) {
            // Multi-param or single param with @Param in batch operation
            assignKeysToParamMapList(configuration, rowResultWrapper, keyProperties, (ArrayList<ParamMap<?>>) parameter);
        } else {
            // Single param without @Param
            assignKeysToParam(configuration, rowResultWrapper, keyProperties, parameter);
        }
    }

    private void assignKeysToParam(R2dbcMybatisConfiguration configuration, RowResultWrapper rowResultWrapper,
                                   String[] keyProperties, Object parameter) {
        List<?> params = collectionize(parameter);
        if (params.isEmpty()) {
            return;
        }
        int i = resultRowCounter.intValue();
        if (params.size() <= i) {
            throw new ExecutorException(String.format(MSG_TOO_MANY_KEYS, params.size()));
        }
        KeyAssigner keyAssigner = new KeyAssigner(configuration, i + 1, null, keyProperties[i]);
        keyAssigner.assign(rowResultWrapper, params.get(i));
    }

    private void assignKeysToParamMapList(R2dbcMybatisConfiguration configuration, RowResultWrapper rowResultWrapper,
                                          String[] keyProperties, ArrayList<ParamMap<?>> paramMapList) {
        int i = resultRowCounter.intValue();
        if (paramMapList.size() <= i) {
            throw new ExecutorException(String.format(MSG_TOO_MANY_KEYS, paramMapList.size()));
        }
        List<KeyAssigner> assignerList = new ArrayList<>();
        ParamMap<?> paramMap = paramMapList.get(i);
        for (int j = 0; j < keyProperties.length; j++) {
            assignerList.add(getAssignerForParamMap(configuration, j + 1, paramMap, keyProperties[j], keyProperties, false)
                    .getValue());
        }
        assignerList.forEach(x -> x.assign(rowResultWrapper, paramMap));
    }

    private void assignKeysToParamMap(R2dbcMybatisConfiguration configuration,
                                      RowResultWrapper rowResultWrapper,
                                      String[] keyProperties,
                                      Map<String, ?> paramMap) {
        if (paramMap.isEmpty()) {
            return;
        }
        Map<String, Map.Entry<List<?>, List<KeyAssigner>>> assignerMap = new HashMap<>();
        for (int i = 0; i < keyProperties.length; i++) {
            Map.Entry<String, KeyAssigner> entry = getAssignerForParamMap(configuration, i + 1, paramMap, keyProperties[i],
                    keyProperties, true);
            Map.Entry<List<?>, List<KeyAssigner>> iteratorPair = MapUtil.computeIfAbsent(assignerMap, entry.getKey(),
                    k -> MapUtil.entry(collectionize(paramMap.get(k)), new ArrayList<>()));
            iteratorPair.getValue().add(entry.getValue());
        }
        int i = resultRowCounter.intValue();
        for (Map.Entry<List<?>, List<KeyAssigner>> pair : assignerMap.values()) {
            if (pair.getKey().size() <= i) {
                throw new ExecutorException(String.format(MSG_TOO_MANY_KEYS, paramMap.size()));
            }
            Object param = pair.getKey().get(i);
            pair.getValue().forEach(x -> x.assign(rowResultWrapper, param));
        }
    }

    private Map.Entry<String, KeyAssigner> getAssignerForParamMap(R2dbcMybatisConfiguration config,
                                                                  int columnPosition,
                                                                  Map<String, ?> paramMap,
                                                                  String keyProperty,
                                                                  String[] keyProperties,
                                                                  boolean omitParamName) {
        Set<String> keySet = paramMap.keySet();
        // A caveat : if the only parameter has {@code @Param("param2")} on it,
        // it must be referenced with param name e.g. 'param2.x'.
        boolean singleParam = !keySet.contains(SECOND_GENERIC_PARAM_NAME);
        int firstDot = keyProperty.indexOf('.');
        if (firstDot == -1) {
            if (singleParam) {
                return getAssignerForSingleParam(config, columnPosition, paramMap, keyProperty, omitParamName);
            }
            throw new ExecutorException("Could not determine which parameter to assign generated keys to. "
                    + "Note that when there are multiple parameters, 'keyProperty' must include the parameter name (e.g. 'param.id'). "
                    + "Specified key properties are " + ArrayUtil.toString(keyProperties) + " and available parameters are "
                    + keySet);
        }
        String paramName = keyProperty.substring(0, firstDot);
        if (keySet.contains(paramName)) {
            String argParamName = omitParamName ? null : paramName;
            String argKeyProperty = keyProperty.substring(firstDot + 1);
            return MapUtil.entry(paramName, new KeyAssigner(config, columnPosition, argParamName, argKeyProperty));
        } else if (singleParam) {
            return getAssignerForSingleParam(config, columnPosition, paramMap, keyProperty, omitParamName);
        } else {
            throw new ExecutorException("Could not find parameter '" + paramName + "'. "
                    + "Note that when there are multiple parameters, 'keyProperty' must include the parameter name (e.g. 'param.id'). "
                    + "Specified key properties are " + ArrayUtil.toString(keyProperties) + " and available parameters are "
                    + keySet);
        }
    }

    private Map.Entry<String, KeyAssigner> getAssignerForSingleParam(R2dbcMybatisConfiguration r2DbcMybatisConfiguration,
                                                                     int columnPosition, Map<String, ?> paramMap, String keyProperty, boolean omitParamName) {
        // Assume 'keyProperty' to be a property of the single param.
        String singleParamName = nameOfSingleParam(paramMap);
        String argParamName = omitParamName ? null : singleParamName;
        return MapUtil.entry(singleParamName, new KeyAssigner(r2DbcMybatisConfiguration, columnPosition, argParamName, keyProperty));
    }

    private class KeyAssigner {

        private final R2dbcMybatisConfiguration r2DbcMybatisConfiguration;
        private final TypeHandlerRegistry typeHandlerRegistry;
        private final int columnPosition;
        private final String paramName;
        private final String propertyName;
        private final TypeHandler delegatedTypeHandler;
        private TypeHandler<?> typeHandler;

        protected KeyAssigner(R2dbcMybatisConfiguration r2DbcMybatisConfiguration,
                              int columnPosition,
                              String paramName,
                              String propertyName) {
            super();
            this.r2DbcMybatisConfiguration = r2DbcMybatisConfiguration;
            this.typeHandlerRegistry = r2DbcMybatisConfiguration.getTypeHandlerRegistry();
            this.columnPosition = columnPosition;
            this.paramName = paramName;
            this.propertyName = propertyName;
            this.delegatedTypeHandler = initDelegateTypeHandler();
        }

        /**
         * get delegate type handler
         *
         * @return
         */
        private TypeHandler initDelegateTypeHandler() {
            return ProxyInstanceFactory.newInstanceOfInterfaces(
                    TypeHandler.class,
                    () -> new DelegateR2dbcResultRowDataHandler(
                            this.r2DbcMybatisConfiguration.getNotSupportedDataTypes(),
                            this.r2DbcMybatisConfiguration.getR2dbcTypeHandlerAdapterRegistry().getR2dbcTypeHandlerAdapters()
                    ),
                    TypeHandleContext.class
            );
        }

        protected void assign(RowResultWrapper rowResultWrapper, Object param) {
            if (paramName != null) {
                // If paramName is set, param is ParamMap
                param = ((ParamMap<?>) param).get(paramName);
            }
            MetaObject metaParam = r2DbcMybatisConfiguration.newMetaObject(param);
            try {
                if (typeHandler == null) {
                    if (metaParam.hasSetter(propertyName)) {
                        Class<?> propertyType = metaParam.getSetterType(propertyName);
                        typeHandler = typeHandlerRegistry.getTypeHandler(propertyType);
                    } else {
                        throw new ExecutorException("No setter found for the keyProperty '" + propertyName + "' in '"
                                + metaParam.getOriginalObject().getClass().getName() + "'.");
                    }
                }
                if (typeHandler == null) {
                    // Error?
                } else {
                    ((TypeHandleContext) this.delegatedTypeHandler).contextWith(typeHandler, rowResultWrapper);
                    ResultSet resultSet = null;
                    Object value = delegatedTypeHandler.getResult(resultSet, columnPosition);
                    metaParam.setValue(propertyName, value);
                }
            } catch (SQLException e) {
                throw new ExecutorException("Error getting generated key or setting result to parameter object. Cause: " + e,
                        e);
            }
        }
    }

}

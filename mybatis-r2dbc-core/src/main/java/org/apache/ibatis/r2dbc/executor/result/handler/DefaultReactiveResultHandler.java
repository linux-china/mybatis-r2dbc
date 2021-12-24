package org.apache.ibatis.r2dbc.executor.result.handler;

import io.r2dbc.spi.Row;
import org.apache.ibatis.annotations.AutomapConstructor;
import org.apache.ibatis.cache.CacheKey;
import org.apache.ibatis.executor.ExecutorException;
import org.apache.ibatis.executor.result.DefaultResultContext;
import org.apache.ibatis.executor.result.DefaultResultHandler;
import org.apache.ibatis.executor.result.ResultMapException;
import org.apache.ibatis.mapping.Discriminator;
import org.apache.ibatis.mapping.MappedStatement;
import org.apache.ibatis.mapping.ResultMap;
import org.apache.ibatis.mapping.ResultMapping;
import org.apache.ibatis.r2dbc.delegate.R2dbcMybatisConfiguration;
import org.apache.ibatis.r2dbc.exception.R2dbcResultException;
import org.apache.ibatis.r2dbc.executor.result.RowResultWrapper;
import org.apache.ibatis.r2dbc.executor.result.TypeHandleContext;
import org.apache.ibatis.r2dbc.support.ProxyInstanceFactory;
import org.apache.ibatis.reflection.MetaClass;
import org.apache.ibatis.reflection.MetaObject;
import org.apache.ibatis.reflection.ReflectorFactory;
import org.apache.ibatis.reflection.factory.ObjectFactory;
import org.apache.ibatis.session.AutoMappingBehavior;
import org.apache.ibatis.session.ResultHandler;
import org.apache.ibatis.type.TypeHandler;
import org.apache.ibatis.type.TypeHandlerRegistry;

import java.lang.reflect.Constructor;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.atomic.LongAdder;

/**
 * @author: chenggang
 * @date 12/10/21.
 */
public class DefaultReactiveResultHandler implements ReactiveResultHandler {

    private final LongAdder totalCount = new LongAdder();

    private final R2dbcMybatisConfiguration r2DbcMybatisConfiguration;
    private final MappedStatement mappedStatement;
    private final ObjectFactory objectFactory;
    private final ReflectorFactory reflectorFactory;
    private final TypeHandlerRegistry typeHandlerRegistry;
    // temporary marking flag that indicate using constructor mapping (use field to reduce memory usage)
    private boolean useConstructorMappings;
    private final Map<CacheKey, List<PendingRelation>> pendingRelations = new HashMap<>();
    // Cached Automappings
    private final Map<String, List<UnMappedColumnAutoMapping>> autoMappingsCache = new HashMap<>();
    // nested resultmaps
    private final Map<CacheKey, Object> nestedResultObjects = new HashMap<>();
    private final Map<String, Object> ancestorObjects = new HashMap<>();
    private Object previousRowValue;

    private final TypeHandler delegatedTypeHandler;
    private final List<Object> resultHolder = new ArrayList<>();

    private static class PendingRelation {
        public MetaObject metaObject;
        public ResultMapping propertyMapping;
    }

    private static class UnMappedColumnAutoMapping {
        private final String column;
        private final String property;
        private final TypeHandler<?> typeHandler;
        private final boolean primitive;

        public UnMappedColumnAutoMapping(String column, String property, TypeHandler<?> typeHandler, boolean primitive) {
            this.column = column;
            this.property = property;
            this.typeHandler = typeHandler;
            this.primitive = primitive;
        }
    }

    public DefaultReactiveResultHandler(R2dbcMybatisConfiguration r2DbcMybatisConfiguration, MappedStatement mappedStatement) {
        this.mappedStatement = mappedStatement;
        this.r2DbcMybatisConfiguration = r2DbcMybatisConfiguration;
        this.objectFactory = r2DbcMybatisConfiguration.getObjectFactory();
        this.reflectorFactory = r2DbcMybatisConfiguration.getReflectorFactory();
        this.typeHandlerRegistry = r2DbcMybatisConfiguration.getTypeHandlerRegistry();
        this.delegatedTypeHandler = this.initDelegateTypeHandler();
    }

    @Override
    public Integer getResultRowTotalCount() {
        return totalCount.intValue();
    }

    @Override
    public <T> List<T> handleResult(RowResultWrapper rowResultWrapper) {
        List<ResultMap> resultMaps = mappedStatement.getResultMaps();
        int resultMapCount = resultMaps.size();
        if (resultMapCount < 1) {
            throw new ExecutorException("A query was run and no Result Maps were found for the Mapped Statement '" + mappedStatement.getId()
                    + "'.  It's likely that neither a Result Type nor a Result Map was specified.");
        }
        ResultMap resultMap = resultMaps.get(0);
        if(!resultMap.hasNestedResultMaps()){
            try{
                ResultMap discriminatedResultMap = resolveDiscriminatedResultMap(rowResultWrapper, resultMap, null);
                Object rowValue = getRowValueForSimpleResultMap(rowResultWrapper, discriminatedResultMap, null);
                totalCount.increment();
                return Collections.singletonList((T) rowValue);
            }catch (SQLException e) {
                throw new R2dbcResultException(e);
            }
        }
        try {
            List<Object> objects = handleRowValuesForNestedResultMap(rowResultWrapper, resultMap);
            totalCount.increment();
            return (List<T>) objects;
        } catch (SQLException e) {
            throw new R2dbcResultException(e);
        }
    }

    /**
     * get row value for simple result map
     * @param rowResultWrapper
     * @param resultMap
     * @param columnPrefix
     * @return
     * @throws SQLException
     */
    private Object getRowValueForSimpleResultMap(RowResultWrapper rowResultWrapper, ResultMap resultMap, String columnPrefix) throws SQLException {
        Object rowValue = createResultObject(rowResultWrapper, resultMap, columnPrefix);
        if (rowValue != null && !hasTypeHandlerForResultObject(resultMap.getType())) {
            final MetaObject metaObject = r2DbcMybatisConfiguration.newMetaObject(rowValue);
            boolean foundValues = this.useConstructorMappings;
            if (shouldApplyAutomaticMappings(resultMap, false)) {
                foundValues = applyAutomaticMappings(rowResultWrapper, resultMap, metaObject, columnPrefix) || foundValues;
            }
            foundValues = applyPropertyMappings(rowResultWrapper, resultMap, metaObject, columnPrefix) || foundValues;
            rowValue = foundValues || r2DbcMybatisConfiguration.isReturnInstanceForEmptyRow() ? rowValue : null;
        }
        return rowValue;
    }

    /**
     * handle row values for nested resultMap
     * @param rowResultWrapper
     * @param resultMap
     * @throws SQLException
     */
    private List<Object> handleRowValuesForNestedResultMap(RowResultWrapper rowResultWrapper, ResultMap resultMap) throws SQLException {
        final DefaultResultHandler resultHandler = new DefaultResultHandler(objectFactory);
        final DefaultResultContext<Object> resultContext = new DefaultResultContext<>();
        Object rowValue = previousRowValue;
        final ResultMap discriminatedResultMap = resolveDiscriminatedResultMap(rowResultWrapper, resultMap, null);
        final CacheKey rowKey = createRowKey(discriminatedResultMap, rowResultWrapper, null);
        Object partialObject = nestedResultObjects.get(rowKey);
        // issue #577 && #542
        if (mappedStatement.isResultOrdered()) {
            if (partialObject == null && rowValue != null) {
                nestedResultObjects.clear();
                storeObject(resultHandler, resultContext, rowValue, null, rowResultWrapper);
            }
            rowValue = getRowValueForNestedResultMap(rowResultWrapper, discriminatedResultMap, rowKey, null, partialObject);
        } else {
            rowValue = getRowValueForNestedResultMap(rowResultWrapper, discriminatedResultMap, rowKey, null, partialObject);
            if (partialObject == null) {
                storeObject(resultHandler, resultContext, rowValue, null, rowResultWrapper);
            }
        }
        if (rowValue != null && mappedStatement.isResultOrdered() ) {
            storeObject(resultHandler, resultContext, rowValue, null, rowResultWrapper);
            previousRowValue = null;
        } else if (rowValue != null) {
            previousRowValue = rowValue;
        }
        this.resultHolder.addAll(resultHandler.getResultList());
        if(totalCount.intValue() != 0 && null == partialObject){
            List<Object> holdResultList = new ArrayList<>(this.resultHolder);
            this.resultHolder.clear();
            return holdResultList;
        }
        return Collections.singletonList(DEFERRED);
    }

    private boolean applyNestedResultMappings(RowResultWrapper rowResultWrapper, ResultMap resultMap, MetaObject metaObject, String parentPrefix, CacheKey parentRowKey, boolean newObject) {
        boolean foundValues = false;
        for (ResultMapping resultMapping : resultMap.getPropertyResultMappings()) {
            final String nestedResultMapId = resultMapping.getNestedResultMapId();
            if (nestedResultMapId != null && resultMapping.getResultSet() == null) {
                try {
                    final String columnPrefix = getColumnPrefix(parentPrefix, resultMapping);
                    final ResultMap nestedResultMap = getNestedResultMap(rowResultWrapper, nestedResultMapId, columnPrefix);
                    if (resultMapping.getColumnPrefix() == null) {
                        // try to fill circular reference only when columnPrefix
                        // is not specified for the nested result map (issue #215)
                        Object ancestorObject = ancestorObjects.get(nestedResultMapId);
                        if (ancestorObject != null) {
                            if (newObject) {
                                linkObjects(metaObject, resultMapping, ancestorObject); // issue #385
                            }
                            continue;
                        }
                    }
                    final CacheKey rowKey = createRowKey(nestedResultMap, rowResultWrapper, columnPrefix);
                    final CacheKey combinedKey = combineKeys(rowKey, parentRowKey);
                    Object rowValue = nestedResultObjects.get(combinedKey);
                    boolean knownValue = rowValue != null;
                    instantiateCollectionPropertyIfAppropriate(resultMapping, metaObject); // mandatory
                    if (anyNotNullColumnHasValue(resultMapping, columnPrefix, rowResultWrapper)) {
                        rowValue = getRowValueForNestedResultMap(rowResultWrapper, nestedResultMap, combinedKey, columnPrefix, rowValue);
                        if (rowValue != null && !knownValue) {
                            linkObjects(metaObject, resultMapping, rowValue);
                            foundValues = true;
                        }
                    }
                } catch (SQLException e) {
                    throw new ExecutorException("Error getting nested result map values for '" + resultMapping.getProperty() + "'.  Cause: " + e, e);
                }
            }
        }
        return foundValues;
    }

    private Object getRowValueForNestedResultMap(RowResultWrapper rowResultWrapper, ResultMap resultMap, CacheKey combinedKey, String columnPrefix, Object partialObject) throws SQLException {
        final String resultMapId = resultMap.getId();
        Object rowValue = partialObject;
        if (rowValue != null) {
            final MetaObject metaObject = r2DbcMybatisConfiguration.newMetaObject(rowValue);
            putAncestor(rowValue, resultMapId);
            applyNestedResultMappings(rowResultWrapper, resultMap, metaObject, columnPrefix, combinedKey, false);
            ancestorObjects.remove(resultMapId);
        } else {
            rowValue = createResultObject(rowResultWrapper, resultMap, columnPrefix);
            if (rowValue != null && !hasTypeHandlerForResultObject(resultMap.getType())) {
                final MetaObject metaObject = r2DbcMybatisConfiguration.newMetaObject(rowValue);
                boolean foundValues = this.useConstructorMappings;
                if (shouldApplyAutomaticMappings(resultMap, true)) {
                    foundValues = applyAutomaticMappings(rowResultWrapper, resultMap, metaObject, columnPrefix) || foundValues;
                }
                foundValues = applyPropertyMappings(rowResultWrapper, resultMap, metaObject, columnPrefix) || foundValues;
                putAncestor(rowValue, resultMapId);
                foundValues = applyNestedResultMappings(rowResultWrapper, resultMap, metaObject, columnPrefix, combinedKey, true) || foundValues;
                ancestorObjects.remove(resultMapId);
                rowValue = foundValues || r2DbcMybatisConfiguration.isReturnInstanceForEmptyRow() ? rowValue : null;
            }
            if (combinedKey != CacheKey.NULL_CACHE_KEY) {
                nestedResultObjects.put(combinedKey, rowValue);
            }
        }
        return rowValue;
    }

    private boolean applyPropertyMappings(RowResultWrapper rowResultWrapper, ResultMap resultMap, MetaObject metaObject, String columnPrefix)
            throws SQLException {
        final List<String> mappedColumnNames = rowResultWrapper.getMappedColumnNames(resultMap, columnPrefix);
        boolean foundValues = false;
        final List<ResultMapping> propertyMappings = resultMap.getPropertyResultMappings();
        for (ResultMapping propertyMapping : propertyMappings) {
            String column = prependPrefix(propertyMapping.getColumn(), columnPrefix);
            if (propertyMapping.getNestedResultMapId() != null) {
                // the user added a column attribute to a nested result map, ignore it
                column = null;
            }
            if (propertyMapping.isCompositeResult()
                    || (column != null && mappedColumnNames.contains(column.toUpperCase(Locale.ENGLISH)))
                    || propertyMapping.getResultSet() != null) {
                Object value = getPropertyMappingValue(rowResultWrapper, metaObject, propertyMapping, columnPrefix);
                // issue #541 make property optional
                final String property = propertyMapping.getProperty();
                if (property == null) {
                    continue;
                } else if (value == DEFERRED) {
                    foundValues = true;
                    continue;
                }
                if (value != null) {
                    foundValues = true;
                }
                if (value != null || (r2DbcMybatisConfiguration.isCallSettersOnNulls() && !metaObject.getSetterType(property).isPrimitive())) {
                    // gcode issue #377, call setter on nulls (value is not 'found')
                    metaObject.setValue(property, value);
                }
            }
        }
        return foundValues;
    }

    private Object getPropertyMappingValue(RowResultWrapper rowResultWrapper, MetaObject metaResultObject, ResultMapping propertyMapping,String columnPrefix)
            throws SQLException {
        if (propertyMapping.getNestedQueryId() != null) {
            throw new UnsupportedOperationException("Not supported Nested query ");
        } else {
            final TypeHandler<?> typeHandler = propertyMapping.getTypeHandler();
            final String column = prependPrefix(propertyMapping.getColumn(), columnPrefix);
            ((TypeHandleContext)this.delegatedTypeHandler).contextWith(typeHandler,rowResultWrapper);
            return this.delegatedTypeHandler.getResult(null, column);
        }
    }

    private List<UnMappedColumnAutoMapping> createAutomaticMappings(RowResultWrapper rowResultWrapper, ResultMap resultMap, MetaObject metaObject, String columnPrefix) throws SQLException {
        final String mapKey = resultMap.getId() + ":" + columnPrefix;
        List<UnMappedColumnAutoMapping> autoMapping = autoMappingsCache.get(mapKey);
        if (autoMapping == null) {
            autoMapping = new ArrayList<>();
            final List<String> unmappedColumnNames = rowResultWrapper.getUnmappedColumnNames(resultMap, columnPrefix);
            for (String columnName : unmappedColumnNames) {
                String propertyName = columnName;
                if (columnPrefix != null && !columnPrefix.isEmpty()) {
                    // When columnPrefix is specified,
                    // ignore columns without the prefix.
                    if (columnName.toUpperCase(Locale.ENGLISH).startsWith(columnPrefix)) {
                        propertyName = columnName.substring(columnPrefix.length());
                    } else {
                        continue;
                    }
                }
                final String property = metaObject.findProperty(propertyName, r2DbcMybatisConfiguration.isMapUnderscoreToCamelCase());
                if (property != null && metaObject.hasSetter(property)) {
                    if (resultMap.getMappedProperties().contains(property)) {
                        continue;
                    }
                    final Class<?> propertyType = metaObject.getSetterType(property);
                    if (typeHandlerRegistry.hasTypeHandler(propertyType)) {
                        final TypeHandler<?> typeHandler = rowResultWrapper.getTypeHandler(propertyType, columnName);
                        autoMapping.add(new DefaultReactiveResultHandler.UnMappedColumnAutoMapping(columnName, property, typeHandler, propertyType.isPrimitive()));
                    } else {
                        r2DbcMybatisConfiguration.getAutoMappingUnknownColumnBehavior()
                                .doAction(mappedStatement, columnName, property, propertyType);
                    }
                } else {
                    r2DbcMybatisConfiguration.getAutoMappingUnknownColumnBehavior()
                            .doAction(mappedStatement, columnName, (property != null) ? property : propertyName, null);
                }
            }
            autoMappingsCache.put(mapKey, autoMapping);
        }
        return autoMapping;
    }

    private boolean applyAutomaticMappings(RowResultWrapper rowResultWrapper, ResultMap resultMap, MetaObject metaObject, String columnPrefix) throws SQLException {
        List<UnMappedColumnAutoMapping> autoMapping = createAutomaticMappings(rowResultWrapper, resultMap, metaObject, columnPrefix);
        boolean foundValues = false;
        if (!autoMapping.isEmpty()) {
            for (DefaultReactiveResultHandler.UnMappedColumnAutoMapping mapping : autoMapping) {
                TypeHandler<?> typeHandler = mapping.typeHandler;
                ((TypeHandleContext)this.delegatedTypeHandler).contextWith(typeHandler,rowResultWrapper);
                final Object value = this.delegatedTypeHandler.getResult(null, mapping.column);
                if (value != null) {
                    foundValues = true;
                }
                if (value != null || (r2DbcMybatisConfiguration.isCallSettersOnNulls() && !mapping.primitive)) {
                    // gcode issue #377, call setter on nulls (value is not 'found')
                    metaObject.setValue(mapping.property, value);
                }
            }
        }
        return foundValues;
    }

    private Object createResultObject(RowResultWrapper rowResultWrapper, ResultMap resultMap, String columnPrefix) throws SQLException {
        this.useConstructorMappings = false; // reset previous mapping result
        final List<Class<?>> constructorArgTypes = new ArrayList<>();
        final List<Object> constructorArgs = new ArrayList<>();
        Object resultObject = createResultObject(rowResultWrapper, resultMap, constructorArgTypes, constructorArgs, columnPrefix);
        this.useConstructorMappings = resultObject != null && !constructorArgTypes.isEmpty(); // set current mapping result
        return resultObject;
    }

    private Object createResultObject(RowResultWrapper rowResultWrapper, ResultMap resultMap, List<Class<?>> constructorArgTypes, List<Object> constructorArgs, String columnPrefix)
            throws SQLException {
        final Class<?> resultType = resultMap.getType();
        final MetaClass metaType = MetaClass.forClass(resultType, reflectorFactory);
        final List<ResultMapping> constructorMappings = resultMap.getConstructorResultMappings();
        if (hasTypeHandlerForResultObject(resultType)) {
            return createPrimitiveResultObject(rowResultWrapper, resultMap, columnPrefix);
        } else if (!constructorMappings.isEmpty()) {
            return createParameterizedResultObject(rowResultWrapper, resultType, constructorMappings, constructorArgTypes, constructorArgs, columnPrefix);
        } else if (resultType.isInterface() || metaType.hasDefaultConstructor()) {
            return objectFactory.create(resultType);
        } else if (shouldApplyAutomaticMappings(resultMap, false)) {
            return createByConstructorSignature(rowResultWrapper, resultType, constructorArgTypes, constructorArgs);
        }
        throw new ExecutorException("Do not know how to create an instance of " + resultType);
    }

    private Object createParameterizedResultObject(RowResultWrapper rowResultWrapper, Class<?> resultType, List<ResultMapping> constructorMappings,
                                                   List<Class<?>> constructorArgTypes, List<Object> constructorArgs, String columnPrefix) {
        boolean foundValues = false;
        for (ResultMapping constructorMapping : constructorMappings) {
            final Class<?> parameterType = constructorMapping.getJavaType();
            final String column = constructorMapping.getColumn();
            final Object value;
            try {
                if (constructorMapping.getNestedQueryId() != null) {
                    throw new UnsupportedOperationException("Unsupported constructor with nested query :" + constructorMapping.getNestedQueryId());
                } else if (constructorMapping.getNestedResultMapId() != null) {
                    final ResultMap resultMap = r2DbcMybatisConfiguration.getResultMap(constructorMapping.getNestedResultMapId());
                    value = getRowValueForSimpleResultMap(rowResultWrapper, resultMap, getColumnPrefix(columnPrefix, constructorMapping));
                } else {
                    final TypeHandler<?> typeHandler = constructorMapping.getTypeHandler();
                    ((TypeHandleContext)this.delegatedTypeHandler).contextWith(typeHandler,rowResultWrapper);
                    value = this.delegatedTypeHandler.getResult(null, prependPrefix(column, columnPrefix));
                }
            } catch (ResultMapException | SQLException e) {
                throw new ExecutorException("Could not process result for mapping: " + constructorMapping, e);
            }
            constructorArgTypes.add(parameterType);
            constructorArgs.add(value);
            foundValues = value != null || foundValues;
        }
        return foundValues ? objectFactory.create(resultType, constructorArgTypes, constructorArgs) : null;
    }

    private Object createByConstructorSignature(RowResultWrapper rowResultWrapper, Class<?> resultType, List<Class<?>> constructorArgTypes, List<Object> constructorArgs) throws SQLException {
        final Constructor<?>[] constructors = resultType.getDeclaredConstructors();
        final Constructor<?> defaultConstructor = findDefaultConstructor(constructors);
        if (defaultConstructor != null) {
            return createUsingConstructor(rowResultWrapper, resultType, constructorArgTypes, constructorArgs, defaultConstructor);
        } else {
            for (Constructor<?> constructor : constructors) {
                if (allowedConstructorUsingTypeHandlers(constructor)) {
                    return createUsingConstructor(rowResultWrapper, resultType, constructorArgTypes, constructorArgs, constructor);
                }
            }
        }
        throw new ExecutorException("No constructor found in " + resultType.getName() + " matching " + rowResultWrapper.getClassNames());
    }

    private Object createUsingConstructor(RowResultWrapper rowResultWrapper, Class<?> resultType, List<Class<?>> constructorArgTypes, List<Object> constructorArgs, Constructor<?> constructor) throws SQLException {
        boolean foundValues = false;
        for (int i = 0; i < constructor.getParameterTypes().length; i++) {
            Class<?> parameterType = constructor.getParameterTypes()[i];
            String columnName = rowResultWrapper.getColumnNames().get(i);
            TypeHandler<?> typeHandler = rowResultWrapper.getTypeHandler(parameterType, columnName);
            ((TypeHandleContext)this.delegatedTypeHandler).contextWith(typeHandler,rowResultWrapper);
            Object value = delegatedTypeHandler.getResult(null, columnName);
            constructorArgTypes.add(parameterType);
            constructorArgs.add(value);
            foundValues = value != null || foundValues;
        }
        return foundValues ? objectFactory.create(resultType, constructorArgTypes, constructorArgs) : null;
    }

    private Constructor<?> findDefaultConstructor(final Constructor<?>[] constructors) {
        if (constructors.length == 1) {
            return constructors[0];
        }

        for (final Constructor<?> constructor : constructors) {
            if (constructor.isAnnotationPresent(AutomapConstructor.class)) {
                return constructor;
            }
        }
        return null;
    }

    private boolean allowedConstructorUsingTypeHandlers(final Constructor<?> constructor) {
        final Class<?>[] parameterTypes = constructor.getParameterTypes();
        for (int i = 0; i < parameterTypes.length; i++) {
            if (!typeHandlerRegistry.hasTypeHandler(parameterTypes[i])) {
                return false;
            }
        }
        return true;
    }

    /**
     * create Primitive ResultObject
     * @param rowResultWrapper
     * @param resultMap
     * @param columnPrefix
     * @return
     * @throws SQLException
     */
    private Object createPrimitiveResultObject(RowResultWrapper rowResultWrapper, ResultMap resultMap, String columnPrefix) throws SQLException {
        final Class<?> resultType = resultMap.getType();
        final String columnName;
        if (!resultMap.getResultMappings().isEmpty()) {
            final List<ResultMapping> resultMappingList = resultMap.getResultMappings();
            final ResultMapping mapping = resultMappingList.get(0);
            columnName = prependPrefix(mapping.getColumn(), columnPrefix);
        } else {
            columnName = rowResultWrapper.getColumnNames().get(0);
        }
        final TypeHandler<?> typeHandler = rowResultWrapper.getTypeHandler(resultType, columnName);
        ((TypeHandleContext)this.delegatedTypeHandler).contextWith(typeHandler,rowResultWrapper);
        return delegatedTypeHandler.getResult(null, columnName);
    }

    /**
     * resolve Discriminated ResultMap
     * @param rowResultWrapper
     * @param resultMap
     * @param columnPrefix
     * @return
     * @throws SQLException
     */
    public ResultMap resolveDiscriminatedResultMap(RowResultWrapper rowResultWrapper, ResultMap resultMap, String columnPrefix) throws SQLException {
        Set<String> pastDiscriminators = new HashSet<>();
        Discriminator discriminator = resultMap.getDiscriminator();
        while (discriminator != null) {
            final Object value = getDiscriminatorValue(rowResultWrapper, discriminator, columnPrefix);
            final String discriminatedMapId = discriminator.getMapIdFor(String.valueOf(value));
            if (r2DbcMybatisConfiguration.hasResultMap(discriminatedMapId)) {
                resultMap = r2DbcMybatisConfiguration.getResultMap(discriminatedMapId);
                Discriminator lastDiscriminator = discriminator;
                discriminator = resultMap.getDiscriminator();
                if (discriminator == lastDiscriminator || !pastDiscriminators.add(discriminatedMapId)) {
                    break;
                }
            } else {
                break;
            }
        }
        return resultMap;
    }

    /**
     * get discriminator value
     * @param rowResultWrapper
     * @param discriminator
     * @param columnPrefix
     * @return
     * @throws SQLException
     */
    private Object getDiscriminatorValue(RowResultWrapper rowResultWrapper, Discriminator discriminator, String columnPrefix) throws SQLException {
        final ResultMapping resultMapping = discriminator.getResultMapping();
        final TypeHandler<?> typeHandler = resultMapping.getTypeHandler();
        ((TypeHandleContext)this.delegatedTypeHandler).contextWith(typeHandler,rowResultWrapper);
        return delegatedTypeHandler.getResult(null, prependPrefix(resultMapping.getColumn(), columnPrefix));
    }

    /**
     * store object
     * @param resultHandler
     * @param resultContext
     * @param rowValue
     * @param parentMapping
     * @param rowResultWrapper
     */
    @SuppressWarnings("unchecked" /* because ResultHandler<?> is always ResultHandler<Object>*/)
    private void storeObject(ResultHandler<?> resultHandler, DefaultResultContext<Object> resultContext, Object rowValue, ResultMapping parentMapping, RowResultWrapper rowResultWrapper) {
        if (parentMapping != null) {
            linkToParents(rowResultWrapper, parentMapping, rowValue);
        } else {
            resultContext.nextResultObject(rowValue);
            ((ResultHandler<Object>) resultHandler).handleResult(resultContext);
        }
    }

    private boolean hasTypeHandlerForResultObject(Class<?> resultType) {
        return typeHandlerRegistry.hasTypeHandler(resultType);
    }

    private void linkToParents(RowResultWrapper rowResultWrapper, ResultMapping parentMapping, Object rowValue) {
        CacheKey parentKey = createKeyForMultipleResults(rowResultWrapper, parentMapping, parentMapping.getColumn(), parentMapping.getForeignColumn());
        List<PendingRelation> parents = pendingRelations.get(parentKey);
        if (parents != null) {
            for (DefaultReactiveResultHandler.PendingRelation parent : parents) {
                if (parent != null && rowValue != null) {
                    linkObjects(parent.metaObject, parent.propertyMapping, rowValue);
                }
            }
        }
    }

    private CacheKey createKeyForMultipleResults(RowResultWrapper rowResultWrapper, ResultMapping resultMapping, String names, String columns) {
        CacheKey cacheKey = new CacheKey();
        cacheKey.update(resultMapping);
        if (columns != null && names != null) {
            String[] columnsArray = columns.split(",");
            String[] namesArray = names.split(",");
            Row row = rowResultWrapper.getRow();
            for (int i = 0; i < columnsArray.length; i++) {
                Object value = row.get(columnsArray[i]);
                if (value != null) {
                    cacheKey.update(namesArray[i]);
                    cacheKey.update(value);
                }
            }
        }
        return cacheKey;
    }

    private void putAncestor(Object resultObject, String resultMapId) {
        ancestorObjects.put(resultMapId, resultObject);
    }

    private boolean shouldApplyAutomaticMappings(ResultMap resultMap, boolean isNested) {
        if (resultMap.getAutoMapping() != null) {
            return resultMap.getAutoMapping();
        } else {
            if (isNested) {
                return AutoMappingBehavior.FULL == r2DbcMybatisConfiguration.getAutoMappingBehavior();
            } else {
                return AutoMappingBehavior.NONE != r2DbcMybatisConfiguration.getAutoMappingBehavior();
            }
        }
    }

    private String prependPrefix(String columnName, String prefix) {
        if (columnName == null || columnName.length() == 0 || prefix == null || prefix.length() == 0) {
            return columnName;
        }
        return prefix + columnName;
    }

    private String getColumnPrefix(String parentPrefix, ResultMapping resultMapping) {
        final StringBuilder columnPrefixBuilder = new StringBuilder();
        if (parentPrefix != null) {
            columnPrefixBuilder.append(parentPrefix);
        }
        if (resultMapping.getColumnPrefix() != null) {
            columnPrefixBuilder.append(resultMapping.getColumnPrefix());
        }
        return columnPrefixBuilder.length() == 0 ? null : columnPrefixBuilder.toString().toUpperCase(Locale.ENGLISH);
    }

    private boolean anyNotNullColumnHasValue(ResultMapping resultMapping, String columnPrefix, RowResultWrapper rowResultWrapper) throws SQLException {
        Set<String> notNullColumns = resultMapping.getNotNullColumns();
        if (notNullColumns != null && !notNullColumns.isEmpty()) {
            Row row = rowResultWrapper.getRow();
            for (String column : notNullColumns) {
                if (row.get(prependPrefix(column, columnPrefix)) != null) {
                    return true;
                }
            }
            return false;
        } else if (columnPrefix != null) {
            for (String columnName : rowResultWrapper.getColumnNames()) {
                if (columnName.toUpperCase(Locale.ENGLISH).startsWith(columnPrefix.toUpperCase(Locale.ENGLISH))) {
                    return true;
                }
            }
            return false;
        }
        return true;
    }

    private ResultMap getNestedResultMap(RowResultWrapper rowResultWrapper, String nestedResultMapId, String columnPrefix) throws SQLException {
        ResultMap nestedResultMap = r2DbcMybatisConfiguration.getResultMap(nestedResultMapId);
        return resolveDiscriminatedResultMap(rowResultWrapper, nestedResultMap, columnPrefix);
    }

    private CacheKey createRowKey(ResultMap resultMap, RowResultWrapper rowResultWrapper, String columnPrefix) throws SQLException {
        final CacheKey cacheKey = new CacheKey();
        cacheKey.update(resultMap.getId());
        List<ResultMapping> resultMappings = getResultMappingsForRowKey(resultMap);
        if (resultMappings.isEmpty()) {
            if (Map.class.isAssignableFrom(resultMap.getType())) {
                createRowKeyForMap(rowResultWrapper, cacheKey);
            } else {
                createRowKeyForUnmappedProperties(resultMap, rowResultWrapper, cacheKey, columnPrefix);
            }
        } else {
            createRowKeyForMappedProperties(resultMap, rowResultWrapper, cacheKey, resultMappings, columnPrefix);
        }
        if (cacheKey.getUpdateCount() < 2) {
            return CacheKey.NULL_CACHE_KEY;
        }
        return cacheKey;
    }

    private CacheKey combineKeys(CacheKey rowKey, CacheKey parentRowKey) {
        if (rowKey.getUpdateCount() > 1 && parentRowKey.getUpdateCount() > 1) {
            CacheKey combinedKey;
            try {
                combinedKey = rowKey.clone();
            } catch (CloneNotSupportedException e) {
                throw new ExecutorException("Error cloning cache key.  Cause: " + e, e);
            }
            combinedKey.update(parentRowKey);
            return combinedKey;
        }
        return CacheKey.NULL_CACHE_KEY;
    }

    private List<ResultMapping> getResultMappingsForRowKey(ResultMap resultMap) {
        List<ResultMapping> resultMappings = resultMap.getIdResultMappings();
        if (resultMappings.isEmpty()) {
            resultMappings = resultMap.getPropertyResultMappings();
        }
        return resultMappings;
    }

    private void createRowKeyForMappedProperties(ResultMap resultMap, RowResultWrapper rowResultWrapper, CacheKey cacheKey, List<ResultMapping> resultMappings, String columnPrefix) throws SQLException {
        for (ResultMapping resultMapping : resultMappings) {
            if (resultMapping.isSimple()) {
                final String column = prependPrefix(resultMapping.getColumn(), columnPrefix);
                final TypeHandler<?> typeHandler = resultMapping.getTypeHandler();
                List<String> mappedColumnNames = rowResultWrapper.getMappedColumnNames(resultMap, columnPrefix);
                // Issue #114
                if (column != null && mappedColumnNames.contains(column.toUpperCase(Locale.ENGLISH))) {
                    ((TypeHandleContext)this.delegatedTypeHandler).contextWith(typeHandler,rowResultWrapper);
                    final Object value = this.delegatedTypeHandler.getResult(null, column);
                    if (value != null || r2DbcMybatisConfiguration.isReturnInstanceForEmptyRow()) {
                        cacheKey.update(column);
                        cacheKey.update(value);
                    }
                }
            }
        }
    }

    private void createRowKeyForUnmappedProperties(ResultMap resultMap, RowResultWrapper rowResultWrapper, CacheKey cacheKey, String columnPrefix) throws SQLException {
        final MetaClass metaType = MetaClass.forClass(resultMap.getType(), reflectorFactory);
        List<String> unmappedColumnNames = rowResultWrapper.getUnmappedColumnNames(resultMap, columnPrefix);
        for (String column : unmappedColumnNames) {
            String property = column;
            if (columnPrefix != null && !columnPrefix.isEmpty()) {
                // When columnPrefix is specified, ignore columns without the prefix.
                if (column.toUpperCase(Locale.ENGLISH).startsWith(columnPrefix)) {
                    property = column.substring(columnPrefix.length());
                } else {
                    continue;
                }
            }
            if (metaType.findProperty(property, r2DbcMybatisConfiguration.isMapUnderscoreToCamelCase()) != null) {
                String value = rowResultWrapper.getRow().get(column,String.class);
                if (value != null) {
                    cacheKey.update(column);
                    cacheKey.update(value);
                }
            }
        }
    }

    private void createRowKeyForMap(RowResultWrapper rowResultWrapper, CacheKey cacheKey) {
        List<String> columnNames = rowResultWrapper.getColumnNames();
        for (String columnName : columnNames) {
            final String value = rowResultWrapper.getRow().get(columnName,String.class);
            if (value != null) {
                cacheKey.update(columnName);
                cacheKey.update(value);
            }
        }
    }

    private void linkObjects(MetaObject metaObject, ResultMapping resultMapping, Object rowValue) {
        final Object collectionProperty = instantiateCollectionPropertyIfAppropriate(resultMapping, metaObject);
        if (collectionProperty != null) {
            final MetaObject targetMetaObject = r2DbcMybatisConfiguration.newMetaObject(collectionProperty);
            targetMetaObject.add(rowValue);
        } else {
            metaObject.setValue(resultMapping.getProperty(), rowValue);
        }
    }

    private Object instantiateCollectionPropertyIfAppropriate(ResultMapping resultMapping, MetaObject metaObject) {
        final String propertyName = resultMapping.getProperty();
        Object propertyValue = metaObject.getValue(propertyName);
        if (propertyValue == null) {
            Class<?> type = resultMapping.getJavaType();
            if (type == null) {
                type = metaObject.getSetterType(propertyName);
            }
            try {
                if (objectFactory.isCollection(type)) {
                    propertyValue = objectFactory.create(type);
                    metaObject.setValue(propertyName, propertyValue);
                    return propertyValue;
                }
            } catch (Exception e) {
                throw new ExecutorException("Error instantiating collection property for result '" + resultMapping.getProperty() + "'.  Cause: " + e, e);
            }
        } else if (objectFactory.isCollection(propertyValue.getClass())) {
            return propertyValue;
        }
        return null;
    }

    /**
     * get delegate type handler
     * @return
     */
    private TypeHandler initDelegateTypeHandler(){
        return ProxyInstanceFactory.newInstanceOfInterfaces(
                TypeHandler.class,
                () -> new DelegateR2dbcResultRowDataHandler(
                        this.r2DbcMybatisConfiguration.getNotSupportedDataTypes(),
                        this.r2DbcMybatisConfiguration.getR2dbcTypeHandlerAdapterRegistry().getR2dbcTypeHandlerAdapters()
                ),
                TypeHandleContext.class
        );
    }
}

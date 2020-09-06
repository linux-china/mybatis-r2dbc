package org.apache.ibatis.r2dbc.type;

import org.apache.ibatis.type.MappedTypes;

import java.util.HashMap;
import java.util.Map;

/**
 * Type handler registry
 *
 * @author linux_china
 */
public class TypeHandlerRegistry {
    private final Map<Class<?>, R2DBCTypeHandler<?>> allTypeHandlersMap = new HashMap<>();

    public TypeHandlerRegistry() {
        register(java.util.Date.class, new DateTypeHandler());
        register(java.sql.Date.class, new SqlDateTypeHandler());
        register(java.sql.Time.class, new SqlTimeTypeHandler());
        register(java.sql.Timestamp.class, new SqlTimestampTypeHandler());
    }

    public boolean hasTypeHandler(Class<?> javaType) {
        return allTypeHandlersMap.containsKey(javaType);
    }

    @SuppressWarnings("rawtypes")
    public R2DBCTypeHandler getTypeHandler(Class<?> javaType) {
        return allTypeHandlersMap.get(javaType);
    }

    public <T> void register(Class<T> javaType, R2DBCTypeHandler<? extends T> typeHandler) {
        allTypeHandlersMap.put(javaType, typeHandler);
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    public <T> void register(R2DBCTypeHandler<T> typeHandler) {
        MappedTypes mappedTypes = typeHandler.getClass().getAnnotation(MappedTypes.class);
        if (mappedTypes != null) {
            for (Class handledType : mappedTypes.value()) {
                register(handledType, typeHandler);
            }
        }
        Class handledType = typeHandler.getType();
        if (handledType != null) {
            register(handledType, typeHandler);
        }
    }

}

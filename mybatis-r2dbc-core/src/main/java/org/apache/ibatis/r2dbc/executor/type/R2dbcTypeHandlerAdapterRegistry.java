package org.apache.ibatis.r2dbc.executor.type;

import org.apache.ibatis.io.ResolverUtil;
import org.apache.ibatis.r2dbc.delegate.R2dbcMybatisConfiguration;
import org.apache.ibatis.r2dbc.executor.type.defaults.*;
import org.apache.ibatis.reflection.factory.ObjectFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * @author: chenggang
 * @date 12/9/21.
 */
public class R2dbcTypeHandlerAdapterRegistry {

    private final Map<Class, R2dbcTypeHandlerAdapter> r2dbcTypeHandlerAdapterContainer = new HashMap<>();
    private final R2dbcMybatisConfiguration r2DbcMybatisConfiguration;

    public R2dbcTypeHandlerAdapterRegistry(R2dbcMybatisConfiguration r2DbcMybatisConfiguration) {
        this.r2DbcMybatisConfiguration = r2DbcMybatisConfiguration;
        register(new ByteArrayR2dbcTypeHandlerAdapter());
        register(new ByteObjectArrayR2dbcTypeHandlerAdapter());
        register(new OffsetDateTimeR2dbcTypeHandlerAdapter());
        register(new OffsetTimeR2dbcTypeHandlerAdapter());
        register(new SqlDateR2dbcTypeHandlerAdapter());
        register(new SqlTimeR2dbcTypeHandlerAdapter());
        register(new TimestampR2dbcTypeHandlerAdapter());
        register(new ZonedDateTimeR2dbcTypeHandlerAdapter());
    }

    public Map<Class, R2dbcTypeHandlerAdapter> getR2dbcTypeHandlerAdapters() {
        return this.r2dbcTypeHandlerAdapterContainer;
    }

    public void register(R2dbcTypeHandlerAdapter r2dbcTypeHandlerAdapter) {
        r2dbcTypeHandlerAdapterContainer.put(r2dbcTypeHandlerAdapter.adaptClazz(), r2dbcTypeHandlerAdapter);
    }

    public void register(String packageName) {
        ResolverUtil<R2dbcTypeHandlerAdapter> resolverUtil = new ResolverUtil<>();
        resolverUtil.find(new ResolverUtil.IsA(R2dbcTypeHandlerAdapter.class), packageName);
        resolverUtil.getClasses().forEach(clazz -> {
            ObjectFactory objectFactory = r2DbcMybatisConfiguration.getObjectFactory();
            R2dbcTypeHandlerAdapter r2dbcTypeHandlerAdapter = objectFactory.create(clazz);
            this.register(r2dbcTypeHandlerAdapter);
        });

    }
}

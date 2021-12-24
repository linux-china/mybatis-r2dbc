package org.apache.ibatis.r2dbc.binding;

import org.apache.ibatis.r2dbc.ReactiveSqlSession;
import org.apache.ibatis.r2dbc.support.ProxyInstanceFactory;

import java.lang.reflect.Method;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author Lasse Voss
 */
public class MapperProxyFactory<T> {

    private final Class<T> mapperInterface;
    private final Map<Method, MapperProxy.MapperMethodInvoker> methodCache = new ConcurrentHashMap<>();

    public MapperProxyFactory(Class<T> mapperInterface) {
        this.mapperInterface = mapperInterface;
    }

    public Class<T> getMapperInterface() {
        return mapperInterface;
    }

    public Map<Method, MapperProxy.MapperMethodInvoker> getMethodCache() {
        return methodCache;
    }

    @SuppressWarnings("unchecked")
    protected T newInstance(MapperProxy<T> mapperProxy) {
        return ProxyInstanceFactory.newInstanceOfInterfaces(
                mapperInterface,
                () -> mapperProxy
        );
    }

    public T newInstance(ReactiveSqlSession reactiveSqlSession) {
        final MapperProxy<T> mapperProxy = new MapperProxy<>(reactiveSqlSession, mapperInterface, methodCache);
        return newInstance(mapperProxy);
    }

}

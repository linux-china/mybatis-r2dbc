package org.apache.ibatis.r2dbc.support;


import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

/**
 * @author: chenggang
 * @date 12/12/21.
 */
@SuppressWarnings("unchecked")
public class ProxyInstanceFactory {

    /**
     * new instance of
     *
     * @param interfaceType
     * @param invocationHandlerSupplier
     * @param otherInterfaces
     * @param <T>
     * @return
     */
    public static <T> T newInstanceOfInterfaces(Class<T> interfaceType, Supplier<InvocationHandler> invocationHandlerSupplier, Class... otherInterfaces) {
        List<Class> targetInterfaces = new ArrayList<>();
        targetInterfaces.add(interfaceType);
        Class[] interfaces = new Class[]{interfaceType};
        if (null != otherInterfaces && otherInterfaces.length != 0) {
            interfaces = new Class[otherInterfaces.length + 1];
            interfaces[0] = interfaceType;
            System.arraycopy(otherInterfaces, 0, interfaces, 1, otherInterfaces.length);
        }
        return (T) Proxy.newProxyInstance(interfaceType.getClassLoader(),
                interfaces,
                invocationHandlerSupplier.get()
        );
    }

}

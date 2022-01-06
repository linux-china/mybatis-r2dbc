package org.apache.ibatis.r2dbc.executor.result.handler;

import org.apache.ibatis.logging.Log;
import org.apache.ibatis.logging.LogFactory;
import org.apache.ibatis.r2dbc.executor.result.RowResultWrapper;
import org.apache.ibatis.r2dbc.executor.type.R2dbcTypeHandlerAdapter;
import org.apache.ibatis.type.TypeHandler;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.sql.CallableStatement;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;

/**
 * @author chenggang
 * @date 12/10/21.
 */
public class DelegateR2dbcResultRowDataHandler implements InvocationHandler {

    private static final Log log = LogFactory.getLog(DelegateR2dbcResultRowDataHandler.class);

    private final Set<Class> notSupportedDataTypes;
    private final Map<Class, R2dbcTypeHandlerAdapter> r2dbcTypeHandlerAdapters;
    private TypeHandler delegatedTypeHandler;
    private RowResultWrapper rowResultWrapper;
    private Class typeHandlerArgumentType;

    public DelegateR2dbcResultRowDataHandler(Set<Class> notSupportedDataTypes,
                                             Map<Class, R2dbcTypeHandlerAdapter> r2dbcTypeHandlerAdapters) {
        this.notSupportedDataTypes = notSupportedDataTypes;
        this.r2dbcTypeHandlerAdapters = r2dbcTypeHandlerAdapters;
    }

    @Override
    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
        if ("contextWith".equals(method.getName())) {
            this.delegatedTypeHandler = (TypeHandler) args[0];
            this.rowResultWrapper = (RowResultWrapper) args[1];
            this.typeHandlerArgumentType = this.getTypeHandlerArgumentType(delegatedTypeHandler).orElse(Object.class);
            return null;
        }
        //not getResult() method ,return original invocation
        if (!"getResult".equals(method.getName())) {
            return method.invoke(delegatedTypeHandler, args);
        }
        Object firstArg = args[0];
        Object secondArg = args[1];
        if (null == secondArg) {
            return method.invoke(delegatedTypeHandler, args);
        }
        if (firstArg instanceof CallableStatement) {
            return method.invoke(delegatedTypeHandler, args);
        }
        //not supported
        if (notSupportedDataTypes.contains(this.typeHandlerArgumentType)) {
            throw new IllegalArgumentException("Unsupported Result Data type : " + typeHandlerArgumentType);
        }
        //using adapter
        if (r2dbcTypeHandlerAdapters.containsKey(this.typeHandlerArgumentType)) {
            log.debug("Found r2dbc type handler adapter fro result type : " + this.typeHandlerArgumentType);
            R2dbcTypeHandlerAdapter r2dbcTypeHandlerAdapter = r2dbcTypeHandlerAdapters.get(this.typeHandlerArgumentType);
            // T getResult(ResultSet rs, String columnName)
            if (secondArg instanceof String) {
                return r2dbcTypeHandlerAdapter.getResult(rowResultWrapper.getRow(), rowResultWrapper.getRowMetadata(), (String) secondArg);
            }
            // T getResult(ResultSet rs, int columnIndex)
            if (secondArg instanceof Integer) {
                return r2dbcTypeHandlerAdapter.getResult(rowResultWrapper.getRow(), rowResultWrapper.getRowMetadata(), (Integer) secondArg - 1);
            }
        }
        // T getResult(ResultSet rs, String columnName)
        if (secondArg instanceof String) {
            return rowResultWrapper.getRow().get((String) secondArg, typeHandlerArgumentType);
        }
        // T getResult(ResultSet rs, int columnIndex)
        if (secondArg instanceof Integer) {
            return rowResultWrapper.getRow().get((Integer) secondArg - 1, typeHandlerArgumentType);
        }
        return null;
    }

    /**
     * get type handler actual type argument
     *
     * @return
     */
    private Optional<Class> getTypeHandlerArgumentType(TypeHandler typeHandler) {
        return Stream.of(typeHandler.getClass().getGenericSuperclass())
                .filter(type -> type instanceof ParameterizedType)
                .map(ParameterizedType.class::cast)
                .filter(parameterizedType -> TypeHandler.class.isAssignableFrom((Class) (parameterizedType.getRawType())))
                .flatMap(parameterizedType -> Stream.of(parameterizedType.getActualTypeArguments()))
                .map(Class.class::cast)
                .findFirst();
    }

}

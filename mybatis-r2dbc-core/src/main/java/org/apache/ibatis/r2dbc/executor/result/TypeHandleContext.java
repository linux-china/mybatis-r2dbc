package org.apache.ibatis.r2dbc.executor.result;

import org.apache.ibatis.type.TypeHandler;

/**
 * @author: chenggang
 * @date 12/11/21.
 */
public interface TypeHandleContext {

    /**
     * set delegated type handler
     *
     * @param delegatedTypeHandler
     * @param rowResultWrapper
     */
    void contextWith(TypeHandler delegatedTypeHandler, RowResultWrapper rowResultWrapper);

}

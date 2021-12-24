package org.apache.ibatis.r2dbc.executor.result.handler;

import org.apache.ibatis.r2dbc.executor.result.RowResultWrapper;

import java.util.List;

/**
 * @author: chenggang
 * @date 12/10/21.
 */
public interface ReactiveResultHandler {

    /**
     * deferred object
     */
    Object DEFERRED = new Object();

    /**
     * get result row total count
     *
     * @return
     */
    Integer getResultRowTotalCount();

    /**
     * handle result
     *
     * @param rowResultWrapper
     * @param <T>
     * @return
     */
    <T> List<T> handleResult(RowResultWrapper rowResultWrapper);
}

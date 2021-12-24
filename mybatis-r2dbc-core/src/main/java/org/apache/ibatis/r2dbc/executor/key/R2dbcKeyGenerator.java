package org.apache.ibatis.r2dbc.executor.key;

import org.apache.ibatis.r2dbc.executor.result.RowResultWrapper;

/**
 * @author: chenggang
 * @date 12/12/21.
 */
public interface R2dbcKeyGenerator {

    /**
     * get result row count
     *
     * @return
     */
    Integer getResultRowCount();

    /**
     * handle key result
     *
     * @param rowResultWrapper
     * @param parameter
     * @return
     */
    Integer handleKeyResult(RowResultWrapper rowResultWrapper, Object parameter);
}

package org.apache.ibatis.r2dbc.executor.type;

import io.r2dbc.spi.Row;
import io.r2dbc.spi.RowMetadata;
import io.r2dbc.spi.Statement;
import org.apache.ibatis.r2dbc.executor.parameter.ParameterHandlerContext;

/**
 * @author chenggang
 * @date 12/9/21.
 */
public interface R2dbcTypeHandlerAdapter<T> {

    /**
     * adapted class
     *
     * @return
     */
    Class<T> adaptClazz();

    /**
     * setParameter
     *
     * @param statement
     * @param parameterHandlerContext
     * @param parameter
     */
    void setParameter(Statement statement, ParameterHandlerContext parameterHandlerContext, T parameter);

    /**
     * get result by columnName
     *
     * @param row
     * @param rowMetadata
     * @param columnName
     * @return
     */
    T getResult(Row row, RowMetadata rowMetadata, String columnName);

    /**
     * get result by columnIndex
     *
     * @param row
     * @param rowMetadata
     * @param columnIndex
     * @return
     */
    T getResult(Row row, RowMetadata rowMetadata, int columnIndex);
}

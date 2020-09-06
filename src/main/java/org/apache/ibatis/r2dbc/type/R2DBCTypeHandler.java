package org.apache.ibatis.r2dbc.type;

import io.r2dbc.spi.R2dbcException;
import io.r2dbc.spi.Row;
import io.r2dbc.spi.RowMetadata;
import io.r2dbc.spi.Statement;
import org.apache.ibatis.type.JdbcType;

/**
 * R2DBC Type handler
 *
 * @author linux_china
 */
public interface R2DBCTypeHandler<T> {

    void setParameter(Statement statement, int i, T parameter, JdbcType jdbcType) throws R2dbcException;

    T getResult(Row row, String columnName, RowMetadata rowMetadata) throws R2dbcException;

    T getResult(Row row, int columnIndex, RowMetadata rowMetadata) throws R2dbcException;

    Class<?> getType();
}

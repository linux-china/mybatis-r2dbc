package org.apache.ibatis.r2dbc.executor.type.defaults;

import io.r2dbc.spi.Row;
import io.r2dbc.spi.RowMetadata;
import io.r2dbc.spi.Statement;
import org.apache.ibatis.r2dbc.executor.parameter.ParameterHandlerContext;
import org.apache.ibatis.r2dbc.executor.type.R2dbcTypeHandlerAdapter;

import java.sql.Date;
import java.time.LocalDate;

/**
 * @author chenggang
 * @date 12/9/21.
 */
public class SqlDateR2dbcTypeHandlerAdapter implements R2dbcTypeHandlerAdapter<Date> {

    @Override
    public Class<Date> adaptClazz() {
        return Date.class;
    }

    @Override
    public void setParameter(Statement statement, ParameterHandlerContext parameterHandlerContext, Date parameter) {
        statement.bind(parameterHandlerContext.getIndex(), parameter.toLocalDate());
    }

    @Override
    public Date getResult(Row row, RowMetadata rowMetadata, String columnName) {
        LocalDate localDate = row.get(columnName, LocalDate.class);
        if (null == localDate) {
            return null;
        }
        return Date.valueOf(localDate);
    }

    @Override
    public Date getResult(Row row, RowMetadata rowMetadata, int columnIndex) {
        LocalDate localDate = row.get(columnIndex, LocalDate.class);
        if (null == localDate) {
            return null;
        }
        return Date.valueOf(localDate);
    }

}

package org.apache.ibatis.r2dbc.executor.type.defaults;

import io.r2dbc.spi.Row;
import io.r2dbc.spi.RowMetadata;
import io.r2dbc.spi.Statement;
import org.apache.ibatis.r2dbc.executor.parameter.ParameterHandlerContext;
import org.apache.ibatis.r2dbc.executor.type.R2dbcTypeHandlerAdapter;

import java.sql.Time;
import java.time.LocalTime;

/**
 * @author chenggang
 * @date 12/9/21.
 */
public class SqlTimeR2dbcTypeHandlerAdapter implements R2dbcTypeHandlerAdapter<Time> {

    @Override
    public Class<Time> adaptClazz() {
        return Time.class;
    }

    @Override
    public void setParameter(Statement statement, ParameterHandlerContext parameterHandlerContext, Time parameter) {
        statement.bind(parameterHandlerContext.getIndex(), parameter.toLocalTime());
    }

    @Override
    public Time getResult(Row row, RowMetadata rowMetadata, String columnName) {
        LocalTime localTime = row.get(columnName, LocalTime.class);
        if (null == localTime) {
            return null;
        }
        return Time.valueOf(localTime);
    }

    @Override
    public Time getResult(Row row, RowMetadata rowMetadata, int columnIndex) {
        LocalTime localTime = row.get(columnIndex, LocalTime.class);
        if (null == localTime) {
            return null;
        }
        return Time.valueOf(localTime);
    }

}

package org.apache.ibatis.r2dbc.type;

import io.r2dbc.spi.R2dbcException;
import io.r2dbc.spi.Row;
import io.r2dbc.spi.RowMetadata;
import io.r2dbc.spi.Statement;
import org.apache.ibatis.type.JdbcType;

import java.sql.Time;
import java.time.LocalTime;
import java.time.ZoneId;

/**
 * java.sql.Time type handler
 *
 * @author linux_china
 */
public class SqlTimeTypeHandler extends org.apache.ibatis.type.SqlTimeTypeHandler implements R2DBCTypeHandler<Time> {
    @Override
    public void setParameter(Statement statement, int i, Time parameter, JdbcType jdbcType) throws R2dbcException {
        if (parameter == null) {
            statement.bindNull(i, LocalTime.class);
        } else {
            LocalTime localTime = parameter.toInstant().atZone(ZoneId.systemDefault()).toLocalTime();
            statement.bind(i, localTime);
        }
    }

    @Override
    public Time getResult(Row row, String columnName, RowMetadata rowMetadata) throws R2dbcException {
        LocalTime localTime = row.get(columnName, LocalTime.class);
        if (localTime != null) {
            return Time.valueOf(localTime);
        }
        return null;
    }

    @Override
    public Time getResult(Row row, int columnIndex, RowMetadata rowMetadata) throws R2dbcException {
        LocalTime localTime = row.get(columnIndex, LocalTime.class);
        if (localTime != null) {
            return Time.valueOf(localTime);
        }
        return null;
    }

    @Override
    public Class<?> getType() {
        return Time.class;
    }
}

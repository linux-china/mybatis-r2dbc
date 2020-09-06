package org.apache.ibatis.r2dbc.type;

import io.r2dbc.spi.R2dbcException;
import io.r2dbc.spi.Row;
import io.r2dbc.spi.RowMetadata;
import io.r2dbc.spi.Statement;
import org.apache.ibatis.type.JdbcType;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Date;

/**
 * Java Date type handler
 *
 * @author linux_china
 */
public class DateTypeHandler extends org.apache.ibatis.type.DateTypeHandler implements R2DBCTypeHandler<Date> {
    @Override
    public void setParameter(Statement statement, int i, Date parameter, JdbcType jdbcType) throws R2dbcException {
        if (parameter == null) {
            statement.bindNull(i, LocalDateTime.class);
        } else {
            LocalDateTime localDateTime = parameter.toInstant().atZone(ZoneId.systemDefault()).toLocalDateTime();
            statement.bind(i, localDateTime);
        }
    }

    @Override
    public Date getResult(Row row, String columnName, RowMetadata rowMetadata) throws R2dbcException {
        LocalDateTime localDateTime = row.get(columnName, LocalDateTime.class);
        if (localDateTime != null) {
            return Date.from(localDateTime.atZone(ZoneId.systemDefault()).toInstant());
        }
        return null;
    }

    @Override
    public Date getResult(Row row, int columnIndex, RowMetadata rowMetadata) throws R2dbcException {
        LocalDateTime localDateTime = row.get(columnIndex, LocalDateTime.class);
        if (localDateTime != null) {
            return Date.from(localDateTime.atZone(ZoneId.systemDefault()).toInstant());
        }
        return null;
    }

    @Override
    public Class<?> getType() {
        return Date.class;
    }
}

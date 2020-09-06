package org.apache.ibatis.r2dbc.type;

import io.r2dbc.spi.R2dbcException;
import io.r2dbc.spi.Row;
import io.r2dbc.spi.RowMetadata;
import io.r2dbc.spi.Statement;
import org.apache.ibatis.type.JdbcType;

import java.sql.Date;
import java.time.LocalDate;
import java.time.ZoneId;

/**
 * java.sql.Date type handler
 *
 * @author linux_china
 */
public class SqlDateTypeHandler extends org.apache.ibatis.type.SqlDateTypeHandler implements R2DBCTypeHandler<Date> {
    @Override
    public void setParameter(Statement statement, int i, Date parameter, JdbcType jdbcType) throws R2dbcException {
        if (parameter == null) {
            statement.bindNull(i, LocalDate.class);
        } else {
            LocalDate localDate = parameter.toInstant().atZone(ZoneId.systemDefault()).toLocalDate();
            statement.bind(i, localDate);
        }
    }

    @Override
    public Date getResult(Row rs, String columnName, RowMetadata rowMetadata) throws R2dbcException {
        LocalDate localDate = rs.get(columnName, LocalDate.class);
        if (localDate != null) {
            return Date.valueOf(localDate);
        }
        return null;
    }

    @Override
    public Date getResult(Row rs, int columnIndex, RowMetadata rowMetadata) throws R2dbcException {
        LocalDate localDate = rs.get(columnIndex, LocalDate.class);
        if (localDate != null) {
            return Date.valueOf(localDate);
        }
        return null;
    }

    @Override
    public Class<?> getType() {
        return Date.class;
    }
}

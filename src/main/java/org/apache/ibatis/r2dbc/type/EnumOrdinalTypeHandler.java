package org.apache.ibatis.r2dbc.type;

import io.r2dbc.spi.R2dbcException;
import io.r2dbc.spi.Row;
import io.r2dbc.spi.RowMetadata;
import io.r2dbc.spi.Statement;
import org.apache.ibatis.type.JdbcType;

/**
 * Enum ordinal type handler
 *
 * @author linux_china
 */
public class EnumOrdinalTypeHandler<E extends Enum<E>> extends org.apache.ibatis.type.EnumOrdinalTypeHandler<E> implements R2DBCTypeHandler<E> {

    private Class<E> type;
    private E[] enums;

    public EnumOrdinalTypeHandler(Class<E> type) {
        super(type);
        this.type = type;
        this.enums = type.getEnumConstants();
    }

    @Override
    public void setParameter(Statement statement, int i, E parameter, JdbcType jdbcType) throws R2dbcException {
        if (parameter == null) {
            statement.bindNull(i, Integer.class);
        } else {
            statement.bind(i, parameter.ordinal());
        }
    }

    @Override
    public E getResult(Row row, String columnName, RowMetadata rowMetadata) throws R2dbcException {
        Integer ordinal = row.get(columnName, Integer.class);
        if (ordinal == null || ordinal == 0) {
            return null;
        }
        return toOrdinalEnum(ordinal);
    }

    @Override
    public E getResult(Row row, int columnIndex, RowMetadata rowMetadata) throws R2dbcException {
        Integer ordinal = row.get(columnIndex, Integer.class);
        if (ordinal == null || ordinal == 0) {
            return null;
        }
        return toOrdinalEnum(ordinal);
    }

    protected E toOrdinalEnum(int ordinal) {
        try {
            return enums[ordinal];
        } catch (Exception ex) {
            throw new IllegalArgumentException("Cannot convert " + ordinal + " to " + type.getSimpleName() + " by ordinal value.", ex);
        }
    }

    @Override
    public Class<?> getType() {
        return this.type;
    }
}

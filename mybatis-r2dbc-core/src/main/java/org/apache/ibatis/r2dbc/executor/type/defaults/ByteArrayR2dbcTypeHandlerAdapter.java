package org.apache.ibatis.r2dbc.executor.type.defaults;

import io.r2dbc.spi.Row;
import io.r2dbc.spi.RowMetadata;
import io.r2dbc.spi.Statement;
import org.apache.ibatis.r2dbc.executor.parameter.ParameterHandlerContext;
import org.apache.ibatis.r2dbc.executor.type.R2dbcTypeHandlerAdapter;

import java.nio.ByteBuffer;

/**
 * @author chenggang
 * @date 12/9/21.
 */
public class ByteArrayR2dbcTypeHandlerAdapter implements R2dbcTypeHandlerAdapter<byte[]> {

    @Override
    public Class<byte[]> adaptClazz() {
        return byte[].class;
    }

    @Override
    public void setParameter(Statement statement, ParameterHandlerContext parameterHandlerContext, byte[] parameter) {
        ByteBuffer byteBuffer = ByteBuffer.wrap(parameter);
        statement.bind(parameterHandlerContext.getIndex(), byteBuffer);
    }

    @Override
    public byte[] getResult(Row row, RowMetadata rowMetadata, String columnName) {
        ByteBuffer byteBuffer = row.get(columnName, ByteBuffer.class);
        if (null == byteBuffer) {
            return null;
        }
        return byteBuffer.array();
    }

    @Override
    public byte[] getResult(Row row, RowMetadata rowMetadata, int columnIndex) {
        ByteBuffer byteBuffer = row.get(columnIndex, ByteBuffer.class);
        if (null == byteBuffer) {
            return null;
        }
        return byteBuffer.array();
    }

}

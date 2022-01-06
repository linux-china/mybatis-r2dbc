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
public class ByteObjectArrayR2dbcTypeHandlerAdapter implements R2dbcTypeHandlerAdapter<Byte[]> {

    @Override
    public Class<Byte[]> adaptClazz() {
        return Byte[].class;
    }

    @Override
    public void setParameter(Statement statement, ParameterHandlerContext parameterHandlerContext, Byte[] parameter) {
        ByteBuffer byteBuffer = ByteBuffer.wrap(this.toPrimitives(parameter));
        statement.bind(parameterHandlerContext.getIndex(), byteBuffer);
    }

    @Override
    public Byte[] getResult(Row row, RowMetadata rowMetadata, String columnName) {
        ByteBuffer byteBuffer = row.get(columnName, ByteBuffer.class);
        if (null == byteBuffer) {
            return null;
        }
        return this.toByteObjects(byteBuffer.array());
    }

    @Override
    public Byte[] getResult(Row row, RowMetadata rowMetadata, int columnIndex) {
        ByteBuffer byteBuffer = row.get(columnIndex, ByteBuffer.class);
        if (null == byteBuffer) {
            return null;
        }
        return this.toByteObjects(byteBuffer.array());
    }

    /**
     * Byte[] -> byte[]
     *
     * @param oBytes
     * @return
     */
    private byte[] toPrimitives(Byte[] oBytes) {
        byte[] bytes = new byte[oBytes.length];
        for (int i = 0; i < oBytes.length; i++) {
            bytes[i] = oBytes[i];
        }
        return bytes;
    }

    /**
     * byte[] -> Byte[]
     *
     * @param oBytes
     * @return
     */
    private Byte[] toByteObjects(byte[] oBytes) {
        Byte[] bytes = new Byte[oBytes.length];
        for (int i = 0; i < oBytes.length; i++) {
            bytes[i] = oBytes[i];
        }
        return bytes;
    }
}

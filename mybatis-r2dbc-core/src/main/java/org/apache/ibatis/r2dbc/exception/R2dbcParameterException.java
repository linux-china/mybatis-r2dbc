package org.apache.ibatis.r2dbc.exception;

import io.r2dbc.spi.R2dbcException;

/**
 * @author chenggang
 * @date 12/10/21.
 */
public class R2dbcParameterException extends R2dbcException {

    public R2dbcParameterException() {
        super();
    }

    public R2dbcParameterException(String reason) {
        super(reason);
    }

    public R2dbcParameterException(String reason, Throwable cause) {
        super(reason, cause);
    }

    public R2dbcParameterException(Throwable cause) {
        super(cause);
    }
}

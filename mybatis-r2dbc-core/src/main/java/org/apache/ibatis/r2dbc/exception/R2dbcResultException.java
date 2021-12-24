package org.apache.ibatis.r2dbc.exception;

import io.r2dbc.spi.R2dbcException;

/**
 * @author: chenggang
 * @date 12/10/21.
 */
public class R2dbcResultException extends R2dbcException {

    public R2dbcResultException() {
        super();
    }

    public R2dbcResultException(String reason) {
        super(reason);
    }

    public R2dbcResultException(String reason, Throwable cause) {
        super(reason, cause);
    }

    public R2dbcResultException(Throwable cause) {
        super(cause);
    }
}

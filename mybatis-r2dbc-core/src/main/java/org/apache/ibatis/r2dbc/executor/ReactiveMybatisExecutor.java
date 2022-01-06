package org.apache.ibatis.r2dbc.executor;

import org.apache.ibatis.mapping.MappedStatement;
import org.apache.ibatis.session.RowBounds;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * @author chenggang
 * @date 12/7/21.
 */
public interface ReactiveMybatisExecutor {

    /**
     * update
     *
     * @param mappedStatement
     * @param parameter
     * @return
     */
    Mono<Integer> update(MappedStatement mappedStatement, Object parameter);

    /**
     * query
     *
     * @param mappedStatement
     * @param parameter
     * @param rowBounds
     * @param <E>
     * @return
     */
    <E> Flux<E> query(MappedStatement mappedStatement, Object parameter, RowBounds rowBounds);

    /**
     * commit
     *
     * @param required
     * @return
     */
    Mono<Void> commit(boolean required);

    /**
     * rollback
     *
     * @param required
     * @return
     */
    Mono<Void> rollback(boolean required);

    /**
     * close
     *
     * @param forceRollback
     * @return
     */
    Mono<Void> close(boolean forceRollback);

}

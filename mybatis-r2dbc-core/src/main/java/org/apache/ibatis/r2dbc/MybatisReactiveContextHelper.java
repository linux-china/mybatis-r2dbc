package org.apache.ibatis.r2dbc;

import org.apache.ibatis.r2dbc.executor.support.ReactiveExecutorContext;
import org.apache.ibatis.r2dbc.executor.support.R2dbcStatementLog;
import reactor.core.publisher.Mono;
import reactor.util.context.Context;

/**
 * @author chenggang
 * @date 12/16/21.
 */
public interface MybatisReactiveContextHelper {

    /**
     * current context
     *
     * @return
     */
    static Mono<ReactiveExecutorContext> currentContext() {
        return Mono.deferContextual(contextView -> Mono
                .justOrEmpty(contextView.getOrEmpty(ReactiveExecutorContext.class))
                .switchIfEmpty(Mono.error(new IllegalStateException("ReactiveExecutorContext is empty")))
                .cast(ReactiveExecutorContext.class)
        );
    }

    /**
     * init reactive executor context with StatementLogHelper
     *
     * @param context
     * @param r2dbcStatementLog
     * @return
     */
    Context initReactiveExecutorContext(Context context, R2dbcStatementLog r2dbcStatementLog);

    /**
     * init reactive executor context
     *
     * @param context
     * @return
     */
    Context initReactiveExecutorContext(Context context);
}

package org.apache.ibatis.r2dbc;

import org.apache.ibatis.r2dbc.executor.ReactiveExecutorContext;
import org.apache.ibatis.r2dbc.executor.StatementLogHelper;
import reactor.core.publisher.Mono;
import reactor.util.context.Context;

/**
 * @author: chenggang
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
     * @param statementLogHelper
     * @return
     */
    Context initReactiveExecutorContext(Context context, StatementLogHelper statementLogHelper);

    /**
     * init reactive executor context
     *
     * @param context
     * @return
     */
    Context initReactiveExecutorContext(Context context);
}

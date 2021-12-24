package org.apache.ibatis.r2dbc.defaults;

import io.r2dbc.spi.IsolationLevel;
import org.apache.ibatis.exceptions.TooManyResultsException;
import org.apache.ibatis.logging.Log;
import org.apache.ibatis.logging.LogFactory;
import org.apache.ibatis.mapping.MappedStatement;
import org.apache.ibatis.r2dbc.MybatisReactiveContextHelper;
import org.apache.ibatis.r2dbc.ReactiveSqlSession;
import org.apache.ibatis.r2dbc.delegate.R2dbcMybatisConfiguration;
import org.apache.ibatis.r2dbc.executor.ReactiveExecutorContext;
import org.apache.ibatis.r2dbc.executor.ReactiveMybatisExecutor;
import org.apache.ibatis.r2dbc.executor.StatementLogHelper;
import org.apache.ibatis.reflection.ParamNameResolver;
import org.apache.ibatis.session.RowBounds;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.util.context.Context;

import java.util.Optional;

/**
 * @author: chenggang
 * @date 12/8/21.
 */
public class DefaultReactiveSqlSession implements ReactiveSqlSession, MybatisReactiveContextHelper {

    private static final Log log = LogFactory.getLog(DefaultReactiveSqlSession.class);

    private final R2dbcMybatisConfiguration configuration;
    private final ReactiveMybatisExecutor reactiveMybatisExecutor;
    private boolean autoCommit = false;
    private IsolationLevel isolationLevel;
    private boolean withTransaction = false;

    public DefaultReactiveSqlSession(R2dbcMybatisConfiguration configuration, ReactiveMybatisExecutor reactiveMybatisExecutor) {
        this.configuration = configuration;
        this.reactiveMybatisExecutor = reactiveMybatisExecutor;
    }

    @Override
    public ReactiveSqlSession setAutoCommit(boolean autoCommit) {
        if (!withTransaction && autoCommit) {
            this.autoCommit = true;
        }
        return this;
    }

    @Override
    public ReactiveSqlSession setIsolationLevel(IsolationLevel isolationLevel) {
        this.isolationLevel = isolationLevel;
        return this;
    }

    @Override
    public ReactiveSqlSession usingTransaction(boolean usingTransactionSupport) {
        this.withTransaction = usingTransactionSupport;
        if (this.withTransaction) {
            this.autoCommit = false;
        }
        return this;
    }

    @Override
    public <T> Mono<T> selectOne(String statement, Object parameter) {
        return this.<T>selectList(statement, parameter)
                .buffer(2)
                .flatMap(results -> {
                    if (results.isEmpty()) {
                        return Mono.empty();
                    }
                    if (results.size() > 1) {
                        return Mono.error(new TooManyResultsException("Expected one result (or null) to be returned by selectOne()"));
                    }
                    return Mono.justOrEmpty(results.get(0));
                }).singleOrEmpty();
    }

    @Override
    public <E> Flux<E> selectList(String statement, Object parameter, RowBounds rowBounds) {
        MappedStatement mappedStatement = configuration.getMappedStatement(statement);
        return reactiveMybatisExecutor.<E>query(mappedStatement, wrapCollection(parameter), rowBounds)
                .contextWrite(context -> initReactiveExecutorContext(context, new StatementLogHelper(mappedStatement.getStatementLog())));
    }

    @Override
    public Mono<Integer> insert(String statement, Object parameter) {
        return this.update(statement, parameter);
    }

    @Override
    public Mono<Integer> update(String statement, Object parameter) {
        MappedStatement mappedStatement = configuration.getMappedStatement(statement);
        return reactiveMybatisExecutor.update(mappedStatement, wrapCollection(parameter))
                .contextWrite(context -> initReactiveExecutorContext(context, new StatementLogHelper(mappedStatement.getStatementLog())));
    }

    @Override
    public Mono<Integer> delete(String statement, Object parameter) {
        return this.update(statement, parameter);
    }

    @Override
    public Mono<Void> commit(boolean force) {
        return reactiveMybatisExecutor.commit((force))
                .contextWrite(this::initReactiveExecutorContext);
    }

    @Override
    public Mono<Void> rollback(boolean force) {
        return reactiveMybatisExecutor.rollback(force)
                .contextWrite(this::initReactiveExecutorContext);
    }

    @Override
    public R2dbcMybatisConfiguration getConfiguration() {
        return this.configuration;
    }

    @Override
    public <T> T getMapper(Class<T> type) {
        return this.configuration.getMapper(type, this);
    }

    @Override
    public Mono<Void> close() {
        return reactiveMybatisExecutor.close(false)
                .contextWrite(this::initReactiveExecutorContext);
    }

    private Object wrapCollection(final Object object) {
        return ParamNameResolver.wrapToMapIfCollection(object, null);
    }

    @Override
    public Context initReactiveExecutorContext(Context context, StatementLogHelper statementLogHelper) {
        Optional<ReactiveExecutorContext> optionalContext = context.getOrEmpty(ReactiveExecutorContext.class)
                .map(ReactiveExecutorContext.class::cast);
        if (optionalContext.isPresent()) {
            ReactiveExecutorContext reactiveExecutorContext = optionalContext.get();
            if (this.withTransaction) {
                reactiveExecutorContext.setWithTransaction();
            }
            reactiveExecutorContext.setStatementLogHelper(statementLogHelper);
            return context;
        }
        ReactiveExecutorContext newContext = new ReactiveExecutorContext(autoCommit, isolationLevel);
        newContext.setStatementLogHelper(statementLogHelper);
        if (this.withTransaction) {
            newContext.setWithTransaction();
        }
        return context.put(ReactiveExecutorContext.class, newContext);
    }

    @Override
    public Context initReactiveExecutorContext(Context context) {
        Optional<ReactiveExecutorContext> optionalContext = context.getOrEmpty(ReactiveExecutorContext.class)
                .map(ReactiveExecutorContext.class::cast);
        if (optionalContext.isPresent()) {
            if (log.isTraceEnabled()) {
                log.trace("Initialize reactive executor context,context already exist :" + optionalContext);
            }
            return context;
        }
        if (log.isTraceEnabled()) {
            log.trace("Initialize reactive executor context,context not exist,create new one");
        }
        ReactiveExecutorContext newContext = new ReactiveExecutorContext(autoCommit, isolationLevel);
        if (this.withTransaction) {
            newContext.setWithTransaction();
        }
        return context.put(ReactiveExecutorContext.class, newContext);
    }
}

package org.apache.ibatis.r2dbc.executor;

import io.r2dbc.spi.Connection;
import io.r2dbc.spi.ConnectionFactory;
import org.apache.ibatis.logging.Log;
import org.apache.ibatis.logging.LogFactory;
import org.apache.ibatis.mapping.MappedStatement;
import org.apache.ibatis.r2dbc.MybatisReactiveContextHelper;
import org.apache.ibatis.r2dbc.connection.ConnectionCloseHolder;
import org.apache.ibatis.r2dbc.delegate.R2dbcMybatisConfiguration;
import org.apache.ibatis.session.RowBounds;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.function.Function;

/**
 * @author: chenggang
 * @date 12/8/21.
 */
public abstract class AbstractReactiveMybatisExecutor implements ReactiveMybatisExecutor {

    private static final Log log = LogFactory.getLog(AbstractReactiveMybatisExecutor.class);

    protected final R2dbcMybatisConfiguration configuration;
    protected final ConnectionFactory connectionFactory;

    protected AbstractReactiveMybatisExecutor(R2dbcMybatisConfiguration configuration, ConnectionFactory connectionFactory) {
        this.configuration = configuration;
        this.connectionFactory = connectionFactory;
    }

    @Override
    public Mono<Integer> update(MappedStatement mappedStatement, Object parameter) {
        return MybatisReactiveContextHelper.currentContext()
                .flatMap(reactiveExecutorContext -> {
                    reactiveExecutorContext.setDirty();
                    return this.inConnection(
                            this.connectionFactory,
                            connection -> this.doUpdateWithConnection(connection, mappedStatement, parameter)
                    );
                });
    }

    @Override
    public <E> Flux<E> query(MappedStatement mappedStatement, Object parameter, RowBounds rowBounds) {
        return this.inConnectionMany(
                this.connectionFactory,
                connection -> this.doQueryWithConnection(connection, mappedStatement, parameter, rowBounds)
        );
    }

    @Override
    public Mono<Void> commit(boolean required) {
        return MybatisReactiveContextHelper.currentContext()
                .flatMap(reactiveExecutorContext -> {
                    reactiveExecutorContext.setForceCommit(reactiveExecutorContext.isDirty() || required);
                    return Mono.justOrEmpty(reactiveExecutorContext.getConnection())
                            .flatMap(connection -> Mono.from(connection.close()))
                            .then(Mono.defer(() -> {
                                reactiveExecutorContext.resetDirty();
                                return Mono.empty();
                            }));
                });
    }

    @Override
    public Mono<Void> rollback(boolean required) {
        return MybatisReactiveContextHelper.currentContext()
                .flatMap(reactiveExecutorContext -> {
                    reactiveExecutorContext.setForceRollback(reactiveExecutorContext.isDirty() || required);
                    return Mono.justOrEmpty(reactiveExecutorContext.getConnection())
                            .flatMap(connection -> Mono.from(connection.close()))
                            .then(Mono.defer(() -> {
                                reactiveExecutorContext.resetDirty();
                                return Mono.empty();
                            }));
                });
    }

    @Override
    public Mono<Void> close(boolean forceRollback) {
        return MybatisReactiveContextHelper.currentContext()
                .flatMap(reactiveExecutorContext -> {
                    reactiveExecutorContext.setForceRollback(forceRollback);
                    reactiveExecutorContext.setRequireClosed(true);
                    return Mono.justOrEmpty(reactiveExecutorContext.getConnection())
                            .flatMap(connection -> Mono.from(connection.close()))
                            .then(Mono.defer(() -> {
                                reactiveExecutorContext.resetDirty();
                                return Mono.empty();
                            }));
                });
    }

    /**
     * do update with connection
     *
     * @param connection
     * @param mappedStatement
     * @param parameter
     * @return
     */
    protected abstract Mono<Integer> doUpdateWithConnection(Connection connection, MappedStatement mappedStatement, Object parameter);

    /**
     * do query with connection
     *
     * @param connection
     * @param mappedStatement
     * @param parameter
     * @param rowBounds
     * @return
     */
    protected abstract <E> Flux<E> doQueryWithConnection(Connection connection, MappedStatement mappedStatement, Object parameter, RowBounds rowBounds);

    /**
     * in connection
     *
     * @param connectionFactory
     * @param action
     * @param <T>
     * @return
     */
    protected <T> Mono<T> inConnection(ConnectionFactory connectionFactory, Function<Connection, Mono<T>> action) {
        Mono<ConnectionCloseHolder> connectionMono = MybatisReactiveContextHelper.currentContext()
                .flatMap(reactiveExecutorContext -> Mono
                        .from(connectionFactory.create())
                        .doOnNext(connection -> {
                            log.debug("Execute Statement With Mono,Get Connection [" + connection + "] From Connection Factory ");
                        })
                )
                .map(connection -> new ConnectionCloseHolder(connection, this::closeConnection));
        // ensure close method only execute once with Mono.usingWhen() operator
        return Mono.usingWhen(connectionMono,
                connection -> action.apply(connection.getTarget()),
                ConnectionCloseHolder::close,
                (connection, err) -> connection.close(),
                ConnectionCloseHolder::close);
    }

    /**
     * in connection many
     *
     * @param connectionFactory
     * @param action
     * @param <T>
     * @return
     */
    protected <T> Flux<T> inConnectionMany(ConnectionFactory connectionFactory, Function<Connection, Flux<T>> action) {
        Mono<ConnectionCloseHolder> connectionMono = MybatisReactiveContextHelper.currentContext()
                .flatMap(reactiveExecutorContext -> Mono
                        .from(connectionFactory.create())
                        .doOnNext(connection -> {
                            log.debug("Execute Statement With Flux,Get Connection [" + connection + "] From Connection Factory ");
                        })
                )
                .map(connection -> new ConnectionCloseHolder(connection, this::closeConnection));
        // ensure close method only execute once with Mono.usingWhen() operator
        return Flux.usingWhen(connectionMono,
                connection -> action.apply(connection.getTarget()),
                ConnectionCloseHolder::close,
                (connection, err) -> connection.close(),
                ConnectionCloseHolder::close);
    }

    /**
     * Release the {@link Connection}.
     *
     * @param connection
     * @return
     */
    protected Mono<Void> closeConnection(Connection connection) {
        return Mono.from(connection.close())
                .onErrorResume(e -> Mono.from(connection.close()));
    }

}

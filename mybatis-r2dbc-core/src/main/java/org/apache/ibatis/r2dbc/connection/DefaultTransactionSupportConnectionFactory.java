package org.apache.ibatis.r2dbc.connection;

import io.r2dbc.pool.ConnectionPool;
import io.r2dbc.spi.Connection;
import io.r2dbc.spi.ConnectionFactory;
import io.r2dbc.spi.ConnectionFactoryMetadata;
import io.r2dbc.spi.Wrapped;
import org.apache.ibatis.logging.Log;
import org.apache.ibatis.logging.LogFactory;
import org.apache.ibatis.r2dbc.MybatisReactiveContextHelper;
import org.apache.ibatis.r2dbc.executor.support.ReactiveExecutorContext;
import org.apache.ibatis.r2dbc.support.ProxyInstanceFactory;
import reactor.core.publisher.Mono;

import java.io.Closeable;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

/**
 * @author evans
 */
public class DefaultTransactionSupportConnectionFactory implements ConnectionFactory, Wrapped<ConnectionFactory>, Closeable {

    private static final Log log = LogFactory.getLog(DefaultTransactionSupportConnectionFactory.class);

    private final ConnectionFactory targetConnectionFactory;

    public DefaultTransactionSupportConnectionFactory(ConnectionFactory targetConnectionFactory) {
        this.targetConnectionFactory = targetConnectionFactory;
    }

    @Override
    public Mono<? extends Connection> create() {
        return this.getOptionalTransactionAwareConnectionProxy(this.targetConnectionFactory);
    }

    @Override
    public ConnectionFactoryMetadata getMetadata() {
        return this.targetConnectionFactory.getMetadata();
    }

    @Override
    public ConnectionFactory unwrap() {
        return this.targetConnectionFactory;
    }

    /**
     * close connection factory
     */
    @Override
    public void close() {
        if (this.targetConnectionFactory instanceof ConnectionPool) {
            ConnectionPool connectionPool = ((ConnectionPool) this.targetConnectionFactory);
            if (!connectionPool.isDisposed()) {
                connectionPool.dispose();
            }
        }
    }

    /**
     * get optional transaction aware connection based on ReactiveExecutorContext's isUsingTransaction()
     *
     * @param targetConnectionFactory
     * @return
     */
    private Mono<Connection> getOptionalTransactionAwareConnectionProxy(ConnectionFactory targetConnectionFactory) {
        return MybatisReactiveContextHelper.currentContext()
                .flatMap(reactiveExecutorContext -> Mono.justOrEmpty(reactiveExecutorContext.getConnection())
                        .switchIfEmpty(Mono.from(targetConnectionFactory.create())
                                .map(newConnection -> {
                                    log.debug("[Get connection]Old connection not exist ,Create connection : " + newConnection);
                                    return this.getConnectionProxy(newConnection, reactiveExecutorContext.isWithTransaction());
                                })
                        )
                        .doOnNext(transactionConnection -> {
                            log.debug("[Get connection]Bind to context : " + transactionConnection);
                            reactiveExecutorContext.bindConnection(transactionConnection);
                        })
                        //if using transaction then force set auto commit to false
                        .flatMap(newConnection -> Mono.justOrEmpty(reactiveExecutorContext.getIsolationLevel())
                                .flatMap(isolationLevel -> {
                                    log.debug("[Get connection]Transaction isolation level exist : " + isolationLevel);
                                    return Mono.from(newConnection.setTransactionIsolationLevel(isolationLevel))
                                            .then(Mono.defer(() -> {
                                                log.debug("[Get connection]Force set autocommit to false");
                                                return Mono.from(newConnection.setAutoCommit(reactiveExecutorContext.isAutoCommit()));
                                            }));
                                })
                                .switchIfEmpty(Mono.from(newConnection.setAutoCommit(reactiveExecutorContext.isAutoCommit())))
                                .then(Mono.defer(() -> {
                                    if (reactiveExecutorContext.setActiveTransaction()) {
                                        return Mono.from(newConnection.beginTransaction())
                                                .then(Mono.defer(() -> Mono.just(newConnection)));
                                    }
                                    return Mono.just(newConnection);
                                }))
                        ));
    }

    private Connection getConnectionProxy(Connection connection, boolean suspendClose) {
        return ProxyInstanceFactory.newInstanceOfInterfaces(
                Connection.class,
                () -> new TransactionAwareConnection(connection, suspendClose),
                Wrapped.class
        );
    }


    /**
     * Invocation handler that delegates close calls on R2DBC Connections to
     */
    private static class TransactionAwareConnection implements InvocationHandler {

        private final Connection connection;
        private final boolean suspendClose;
        private boolean closed = false;

        TransactionAwareConnection(Connection connection, boolean suspendClose) {
            this.connection = connection;
            this.suspendClose = suspendClose;
        }

        @Override
        public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
            switch (method.getName()) {
                case "toString":
                    return proxyToString(proxy);
                case "equals":
                    return (proxy == args[0]);
                case "hashCode":
                    return System.identityHashCode(proxy);
                case "unwrap":
                    return this.connection;
                case "close":
                    if (this.closed) {
                        return Mono.empty();
                    }
                    return MybatisReactiveContextHelper.currentContext()
                            .flatMap(reactiveExecutorContext -> {
                                if (reactiveExecutorContext.isForceRollback()) {
                                    return this.handleRollback(reactiveExecutorContext);
                                }
                                if (reactiveExecutorContext.isForceCommit()) {
                                    return this.handleCommit(reactiveExecutorContext);
                                }
                                if (reactiveExecutorContext.isRequireClosed()) {
                                    log.debug("[Close connection]close connection");
                                    return this.executeCloseConnection(reactiveExecutorContext);
                                }
                                if (!suspendClose) {
                                    return this.executeCloseConnection(reactiveExecutorContext);
                                }
                                log.trace("[Close connection]neither rollback or commit,nothing to do");
                                return Mono.empty();
                            });
                case "isClosed":
                    return this.closed;
                default:
                    //ignore
            }

            if (this.closed) {
                throw new IllegalStateException("Connection handle already closed");
            }

            // Invoke method on target Connection.
            try {
                return method.invoke(this.connection, args);
            } catch (InvocationTargetException ex) {
                throw ex.getTargetException();
            }
        }

        /**
         * handle rollback
         *
         * @param reactiveExecutorContext
         * @return
         */
        private Mono<Void> handleRollback(ReactiveExecutorContext reactiveExecutorContext) {
            return Mono.just(reactiveExecutorContext.isRequireClosed())
                    .filter(requireClose -> requireClose)
                    .flatMap(requireClose -> {
                        log.debug("[Close connection]rollback and close connection");
                        return Mono.from(this.connection.rollbackTransaction())
                                .then(Mono.defer(
                                        () -> {
                                            reactiveExecutorContext.setForceRollback(false);
                                            return this.executeCloseConnection(reactiveExecutorContext);
                                        }
                                ));
                    })
                    .switchIfEmpty(Mono.defer(
                            () -> {
                                log.debug("[Close connection]just rollback,not close connection");
                                reactiveExecutorContext.setForceRollback(false);
                                return Mono.from(this.connection.rollbackTransaction())
                                        .onErrorResume(Exception.class, this::onErrorOperation);
                            }
                    ));
        }

        /**
         * handle commit
         *
         * @param reactiveExecutorContext
         * @return
         */
        private Mono<Void> handleCommit(ReactiveExecutorContext reactiveExecutorContext) {
            return Mono.just(reactiveExecutorContext.isRequireClosed())
                    .filter(requireClose -> requireClose)
                    .flatMap(requireClose -> {
                        log.debug("[Close connection]commit and close connection");
                        return Mono.from(this.connection.commitTransaction())
                                .then(Mono.defer(
                                        () -> {
                                            reactiveExecutorContext.setForceCommit(false);
                                            return this.executeCloseConnection(reactiveExecutorContext);
                                        }
                                ));
                    })
                    .switchIfEmpty(Mono.defer(
                            () -> {
                                log.debug("[Close connection]just commit,not close connection");
                                reactiveExecutorContext.setForceCommit(false);
                                return Mono.from(this.connection.commitTransaction())
                                        .onErrorResume(Exception.class, this::onErrorOperation);
                            }
                    ));
        }

        /**
         * execute close connection
         *
         * @param reactiveExecutorContext
         * @return
         */
        private Mono<Void> executeCloseConnection(ReactiveExecutorContext reactiveExecutorContext) {
            log.debug("[Close Connection]Connection : " + this.connection);
            return Mono.from(this.connection.close())
                    .doOnSubscribe(s -> this.closed = true)
                    .then(Mono.defer(
                            () -> Mono.justOrEmpty(reactiveExecutorContext.clearConnection())
                                    .flatMap(oldConnection -> {
                                        log.debug("[Close Connection]Clear connection in context : " + oldConnection);
                                        return Mono.empty();
                                    })
                    ))
                    .then()
                    .onErrorResume(Exception.class, this::onErrorOperation);

        }

        /**
         * on error operation
         *
         * @param e
         * @return
         */
        private Mono<Void> onErrorOperation(Exception e) {
            return Mono.from(this.connection.close())
                    .doOnSubscribe(v -> this.closed = true)
                    .then(Mono.error(e));
        }

        private String proxyToString(Object proxy) {
            return "Transaction-support proxy for target Connection [" + this.connection + "]";
        }

    }
}

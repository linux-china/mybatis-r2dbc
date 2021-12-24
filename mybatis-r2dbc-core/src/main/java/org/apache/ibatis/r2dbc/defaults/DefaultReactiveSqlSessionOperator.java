package org.apache.ibatis.r2dbc.defaults;

import org.apache.ibatis.r2dbc.MybatisReactiveContextHelper;
import org.apache.ibatis.r2dbc.ReactiveSqlSession;
import org.apache.ibatis.r2dbc.ReactiveSqlSessionFactory;
import org.apache.ibatis.r2dbc.ReactiveSqlSessionOperator;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * @author: chenggang
 * @date 12/16/21.
 */
public class DefaultReactiveSqlSessionOperator implements ReactiveSqlSessionOperator {

    private final ReactiveSqlSession reactiveSqlSession;
    private final MybatisReactiveContextHelper mybatisReactiveContextHelper;

    public DefaultReactiveSqlSessionOperator(ReactiveSqlSessionFactory reactiveSqlSessionFactory) {
        this.reactiveSqlSession = reactiveSqlSessionFactory.openSession().usingTransaction(true);
        this.mybatisReactiveContextHelper = (MybatisReactiveContextHelper) this.reactiveSqlSession;
    }

    @Override
    public <T> Mono<T> execute(Mono<T> monoExecution) {
        return MybatisReactiveContextHelper.currentContext()
                .flatMap(reactiveExecutorContext -> Mono.usingWhen(
                        Mono.just(reactiveSqlSession),
                        session -> monoExecution,
                        ReactiveSqlSession::close,
                        (session, err) -> Mono.empty(),
                        ReactiveSqlSession::close
                ).onErrorResume(ex -> this.reactiveSqlSession.close()
                        .then(Mono.defer(() -> Mono.error(ex)))
                )).contextWrite(mybatisReactiveContextHelper::initReactiveExecutorContext);
    }

    @Override
    public <T> Mono<T> executeAndCommit(Mono<T> monoExecution) {
        return MybatisReactiveContextHelper.currentContext()
                .flatMap(reactiveExecutorContext -> Mono.usingWhen(
                        Mono.just(reactiveSqlSession),
                        session -> monoExecution,
                        session -> session.commit(true)
                                .then(Mono.defer(session::close)),
                        (session, err) -> Mono.empty(),
                        session -> session.rollback(true)
                                .then(Mono.defer(session::close))
                ).onErrorResume(ex -> this.reactiveSqlSession.rollback(true)
                        .then(Mono.defer(this.reactiveSqlSession::close))
                        .then(Mono.defer(() -> Mono.error(ex)))
                )).contextWrite(mybatisReactiveContextHelper::initReactiveExecutorContext);
    }

    @Override
    public <T> Mono<T> executeAndRollback(Mono<T> monoExecution) {
        return MybatisReactiveContextHelper.currentContext()
                .flatMap(reactiveExecutorContext -> Mono.usingWhen(
                        Mono.just(reactiveSqlSession),
                        session -> monoExecution,
                        session -> session.rollback(true)
                                .then(Mono.defer(session::close)),
                        (session, err) -> Mono.empty(),
                        session -> session.rollback(true)
                                .then(Mono.defer(session::close))
                ).onErrorResume(ex -> this.reactiveSqlSession.rollback(true)
                        .then(Mono.defer(this.reactiveSqlSession::close))
                        .then(Mono.defer(() -> Mono.error(ex)))
                )).contextWrite(mybatisReactiveContextHelper::initReactiveExecutorContext);
    }

    @Override
    public <T> Flux<T> executeMany(Flux<T> fluxExecution) {
        return MybatisReactiveContextHelper.currentContext()
                .flatMapMany(reactiveExecutorContext -> Flux.usingWhen(
                        Mono.just(reactiveSqlSession),
                        session -> fluxExecution,
                        ReactiveSqlSession::close,
                        (session, err) -> Mono.empty(),
                        ReactiveSqlSession::close
                ).onErrorResume(ex -> this.reactiveSqlSession.close()
                        .then(Mono.defer(() -> Mono.error(ex)))
                )).contextWrite(mybatisReactiveContextHelper::initReactiveExecutorContext);
    }

    @Override
    public <T> Flux<T> executeManyAndCommit(Flux<T> fluxExecution) {
        return MybatisReactiveContextHelper.currentContext()
                .flatMapMany(reactiveExecutorContext -> Flux.usingWhen(
                        Mono.just(reactiveSqlSession),
                        session -> fluxExecution,
                        session -> session.commit(true)
                                .then(Mono.defer(session::close)),
                        (session, err) -> Mono.empty(),
                        session -> session.rollback(true)
                                .then(Mono.defer(session::close))
                ).onErrorResume(ex -> this.reactiveSqlSession.rollback(true)
                        .then(Mono.defer(this.reactiveSqlSession::close))
                        .then(Mono.defer(() -> Mono.error(ex)))
                )).contextWrite(mybatisReactiveContextHelper::initReactiveExecutorContext);
    }

    @Override
    public <T> Flux<T> executeManyAndRollback(Flux<T> fluxExecution) {
        return MybatisReactiveContextHelper.currentContext()
                .flatMapMany(reactiveExecutorContext -> Flux.usingWhen(
                        Mono.just(reactiveSqlSession),
                        session -> fluxExecution,
                        session -> session.commit(true)
                                .then(Mono.defer(session::close)),
                        (session, err) -> Mono.empty(),
                        session -> session.rollback(true)
                                .then(Mono.defer(session::close))
                ).onErrorResume(ex -> this.reactiveSqlSession.rollback(true)
                        .then(Mono.defer(this.reactiveSqlSession::close))
                        .then(Mono.defer(() -> Mono.error(ex)))
                )).contextWrite(mybatisReactiveContextHelper::initReactiveExecutorContext);
    }
}

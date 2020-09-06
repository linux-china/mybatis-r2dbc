package org.apache.ibatis.r2dbc;

import org.apache.ibatis.session.Configuration;
import org.apache.ibatis.session.RowBounds;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * Reactive SQL Session
 *
 * @author linux_china
 */
public interface ReactiveSqlSession {

    default Mono<Integer> selectOne(String statementId) {
        return selectOne(statementId, null);
    }

    <T> Mono<T> selectOne(String statementId, Object parameter);

    default <T> Flux<T> select(String statementId) {
        return select(statementId, null);
    }

    <T> Flux<T> select(String statementId, Object parameter);

    <T> Flux<T> select(String statementId, Object parameter, RowBounds rowBounds);

    default Mono<Integer> insert(String statementId) {
        return insert(statementId, null);
    }

    Mono<Integer> insert(String statementId, Object parameter);

    default Mono<Integer> update(String statementId) {
        return update(statementId, null);
    }

    Mono<Integer> update(String statementId, Object parameter);

    default Mono<Integer> delete(String statementId) {
        return delete(statementId, null);
    }

    Mono<Integer> delete(String statementId, Object parameter);

    Configuration getConfiguration();

    <T> T getMapper(Class<T> clazz);
}

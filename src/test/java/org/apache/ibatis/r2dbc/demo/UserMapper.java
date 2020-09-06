package org.apache.ibatis.r2dbc.demo;

import org.apache.ibatis.annotations.ResultMap;
import org.apache.ibatis.annotations.Select;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.Map;

/**
 * User Mapper
 *
 * @author linux_china
 */
public interface UserMapper {

    Mono<Integer> insert(User user);

    Mono<Boolean> update(User demo);

    Mono<User> findById(Integer id);

    Mono<User> dynamicFindExample(User example);

    Mono<Map<String, Object>> find2ById(Integer id);

    @Select("SELECT id, nick, created_at FROM people WHERE nick = #{value}")
    @ResultMap("UserResultMap")
    Mono<User> findByNick(String nick);

    Flux<User> findAll();

    Mono<Long> getAllCount();
}

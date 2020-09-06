package org.apache.ibatis;

import org.apache.ibatis.cache.Cache;
import org.apache.ibatis.executor.keygen.KeyGenerator;
import org.apache.ibatis.mapping.*;
import org.apache.ibatis.r2dbc.demo.User;
import org.apache.ibatis.r2dbc.demo.UserMapper;
import org.apache.ibatis.r2dbc.type.R2DBCTypeHandler;
import org.apache.ibatis.reflection.MetaObject;
import org.apache.ibatis.session.Configuration;
import org.apache.ibatis.type.TypeHandler;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.math.RoundingMode;
import java.time.Duration;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * MyBatis Configuration test
 *
 * @author linux_china
 */
public class MybatisConfigurationTest extends MyBatisBaseTestSupport {

    @Test
    public void testSelect() {
        Configuration configuration = getConfiguration();
        MappedStatement mappedStatement = configuration.getMappedStatement("org.apache.ibatis.r2dbc.demo.UserMapper.findById");
        Integer paramObject = 1;
        //BoundSQL
        BoundSql boundSql = mappedStatement.getBoundSql(paramObject);
        String sql = boundSql.getSql();
        //MetaObject
        MetaObject metaObject = configuration.newMetaObject(paramObject);
        List<ParameterMapping> parameterMappings = boundSql.getParameterMappings();
        for (int i = 0; i < parameterMappings.size(); i++) {
            ParameterMapping parameterMapping = parameterMappings.get(i);
            System.out.println(i + ":" + metaObject.getValue(parameterMapping.getProperty()));
        }
        //result map
        ResultMap resultMap = mappedStatement.getResultMaps().get(0);
        for (ResultMapping resultMapping : resultMap.getResultMappings()) {
            System.out.println(resultMapping.getColumn() + ":" + resultMapping.getProperty());
            if (resultMapping.getTypeHandler() != null) {
                System.out.println("Result handler: " + resultMapping.getTypeHandler().getClass());
            }
        }
        assertThat(mappedStatement).isNotNull();
    }

    @Test
    public void testDynamicQuery() {
        Configuration configuration = getConfiguration();
        MappedStatement mappedStatement = configuration.getMappedStatement("org.apache.ibatis.r2dbc.demo.UserMapper.dynamicFindExample");
        User paramObject = new User(1, "linux_china");
        BoundSql boundSql = mappedStatement.getBoundSql(paramObject);
        String sql = boundSql.getSql();
        System.out.println(sql);
    }

    @Test
    public void testInsert() {
        Configuration configuration = getConfiguration();
        MappedStatement mappedStatement = configuration.getMappedStatement("org.apache.ibatis.r2dbc.demo.UserMapper.insert");
        User paramObject = new User(99, "xxx");
        BoundSql boundSql = mappedStatement.getBoundSql(paramObject);
        String sql = boundSql.getSql();
        System.out.println(sql);
        List<ParameterMapping> parameterMappings = boundSql.getParameterMappings();
        for (ParameterMapping parameterMapping : parameterMappings) {
            System.out.println("param: " + parameterMapping.getProperty());
        }
    }

    @Test
    public void testUseGeneratedKeys() {
        Configuration configuration = getConfiguration();
        MappedStatement mappedStatement = configuration.getMappedStatement("org.apache.ibatis.r2dbc.demo.UserMapper.insert");
        User paramObject = new User();
        paramObject.setNick("xxx");
        //BoundSQL
        BoundSql boundSql = mappedStatement.getBoundSql(paramObject);
        String sql = boundSql.getSql();
        System.out.println(sql);
        KeyGenerator keyGenerator = mappedStatement.getKeyGenerator();
        String[] keyProperties = mappedStatement.getKeyProperties();
        assertThat(keyGenerator).isNotNull();
        assertThat(keyProperties).hasSize(1);
    }

    @Test
    public void testEnumMapping() {
        Configuration configuration = getConfiguration();
        TypeHandler<RoundingMode> typeHandler = configuration.getTypeHandlerRegistry().getTypeHandler(RoundingMode.class);
        if (typeHandler instanceof R2DBCTypeHandler) {
            R2DBCTypeHandler r2DBCTypeHandler = (R2DBCTypeHandler) typeHandler;
            System.out.println(r2DBCTypeHandler);
        }
        System.out.println(typeHandler);
    }

    @Test
    public void testCacheOperation() throws Exception {
        Configuration configuration = getConfiguration();
        Cache cache = configuration.getCache(UserMapper.class.getCanonicalName());
        Mono<String> user = Mono.defer(() -> {
            return Mono.just("good");
        }).cache(Duration.ofSeconds(2));
        cache.putObject("nick", user);
        System.out.println(((Mono<String>) cache.getObject("nick")).block());
        System.out.println(((Mono<String>) cache.getObject("nick")).block());
        Thread.sleep(3000);
        System.out.println(((Mono<String>) cache.getObject("nick")).block());
        System.out.println("stopped");
    }
}

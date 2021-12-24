package org.apache.ibatis.r2dbc.spring.support;

import org.apache.ibatis.executor.ErrorContext;
import org.apache.ibatis.r2dbc.ReactiveSqlSessionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.FactoryBean;
import org.springframework.beans.factory.InitializingBean;

import static org.springframework.util.Assert.notNull;

/**
 * R2dbcMapperFactoryBean
 *
 * @param <T>
 * @author evans
 */
public class R2dbcMapperFactoryBean<T> implements FactoryBean<T>, InitializingBean {

    private static final Logger log = LoggerFactory.getLogger(R2dbcMapperFactoryBean.class);

    private Class<T> mapperInterface;

    private ReactiveSqlSessionFactory reactiveSqlSessionFactory;

    public R2dbcMapperFactoryBean() {

    }

    public R2dbcMapperFactoryBean(Class<T> clazz) {
        this.mapperInterface = clazz;
    }

    @Override
    public T getObject() throws Exception {
        return reactiveSqlSessionFactory.openSession()
                .getMapper(this.mapperInterface);
    }

    @Override
    public Class<?> getObjectType() {
        return mapperInterface;
    }

    @Override
    public boolean isSingleton() {
        return true;
    }

    public void setMapperInterface(Class<T> mapperInterface) {
        this.mapperInterface = mapperInterface;
    }

    public void setSqlSessionFactory(ReactiveSqlSessionFactory reactiveSqlSessionFactory) {
        this.reactiveSqlSessionFactory = reactiveSqlSessionFactory;
    }

    @Override
    public void afterPropertiesSet() throws Exception {
        notNull(this.reactiveSqlSessionFactory, "Property 'sqlSessionFactory' are required...");
        if (!reactiveSqlSessionFactory.getConfiguration().hasMapper(this.mapperInterface)) {
            try {
                reactiveSqlSessionFactory.getConfiguration().addMapper(this.mapperInterface);
            } catch (Exception e) {
                log.error("Error while adding the mapper '" + this.mapperInterface + "' to configuration.", e);
                throw new IllegalArgumentException(e);
            } finally {
                ErrorContext.instance().reset();
            }
        }
    }
}

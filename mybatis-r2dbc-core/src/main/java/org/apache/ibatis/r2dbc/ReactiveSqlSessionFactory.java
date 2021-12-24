package org.apache.ibatis.r2dbc;

import org.apache.ibatis.r2dbc.delegate.R2dbcMybatisConfiguration;

/**
 * @author: chenggang
 * @date 12/7/21.
 */
public interface ReactiveSqlSessionFactory extends AutoCloseable {

    /**
     * open session
     *
     * @return
     */
    ReactiveSqlSession openSession();

    /**
     * get configuration
     *
     * @return
     */
    R2dbcMybatisConfiguration getConfiguration();
}

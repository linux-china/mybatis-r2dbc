package org.apache.ibatis.r2dbc.impl;

import org.apache.ibatis.builder.xml.XMLConfigBuilder;
import org.apache.ibatis.r2dbc.ReactiveSqlSession;
import org.apache.ibatis.r2dbc.demo.User;
import org.apache.ibatis.session.Configuration;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * DefaultReactiveSqlSession test
 *
 * @author linux_china
 */
public class DefaultReactiveSqlSessionTest {
    private ReactiveSqlSession reactiveSqlSession;
    private DefaultReactiveSqlSessionFactory sqlSessionFactory;

    @BeforeAll
    public void setUp() {
        XMLConfigBuilder xmlConfigBuilder = new XMLConfigBuilder(this.getClass().getResourceAsStream("/mybatis-config.xml"));
        Configuration configuration = xmlConfigBuilder.parse();
        sqlSessionFactory = new DefaultReactiveSqlSessionFactory(configuration);
        reactiveSqlSession = sqlSessionFactory.openSession();
    }

    @AfterAll
    public void tearDown() throws Exception{
        this.sqlSessionFactory.close();
    }

    @Test
    public void testSelect() throws Exception {
        reactiveSqlSession.<User>select("org.apache.ibatis.r2dbc.demo.UserMapper.findById",
                1)
                .subscribe(user -> {
                    System.out.println(user.getNick());
                });
        Thread.sleep(2000);
    }
}

package org.apache.ibatis;

import org.apache.ibatis.builder.xml.XMLConfigBuilder;
import org.apache.ibatis.r2dbc.ReactiveSqlSession;
import org.apache.ibatis.r2dbc.impl.DefaultReactiveSqlSessionFactory;
import org.apache.ibatis.session.Configuration;

import java.lang.reflect.Method;

/**
 * MyBatis Base test support
 *
 * @author linux_china
 */
public abstract class MyBatisBaseTestSupport {
    private static Configuration configuration = null;
    private static ReactiveSqlSession reactiveSqlSession = null;
    private static DefaultReactiveSqlSessionFactory reactiveSqlSessionFactory;

    public Configuration getConfiguration() {
        if (configuration == null) {
            XMLConfigBuilder xmlConfigBuilder = new XMLConfigBuilder(this.getClass().getResourceAsStream("/mybatis-config.xml"));
            configuration = xmlConfigBuilder.parse();
        }
        return configuration;
    }

    public ReactiveSqlSession getReactiveSqlSession() {
        if (reactiveSqlSession == null) {
            Configuration configuration = getConfiguration();
            reactiveSqlSessionFactory = new DefaultReactiveSqlSessionFactory(configuration);
            reactiveSqlSession = reactiveSqlSessionFactory.openSession();
        }
        return reactiveSqlSession;
    }

    protected Method findMethod(Class<?> clazz, String methodName) {
        for (Method declaredMethod : clazz.getDeclaredMethods()) {
            if (declaredMethod.getName().equals(methodName)) {
                return declaredMethod;
            }
        }
        return null;
    }
}

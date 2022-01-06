package org.apache.ibatis.r2dbc.suite.setup;

import io.r2dbc.pool.ConnectionPool;
import io.r2dbc.pool.ConnectionPoolConfiguration;
import io.r2dbc.spi.ConnectionFactories;
import io.r2dbc.spi.ConnectionFactory;
import io.r2dbc.spi.ValidationDepth;
import org.apache.ibatis.builder.xml.XMLMapperBuilder;
import org.apache.ibatis.executor.ErrorContext;
import org.apache.ibatis.r2dbc.ReactiveSqlSessionFactory;
import org.apache.ibatis.r2dbc.defaults.DefaultReactiveSqlSessionFactory;
import org.apache.ibatis.r2dbc.delegate.R2dbcMybatisConfiguration;
import org.apache.ibatis.r2dbc.suite.properties.R2dbcConnectionFactoryProperties;
import org.apache.ibatis.r2dbc.suite.properties.R2dbcMybatisProperties;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.core.io.Resource;
import org.springframework.core.io.support.PathMatchingResourcePatternResolver;
import org.springframework.core.io.support.ResourcePatternResolver;
import reactor.blockhound.BlockHound;
import reactor.core.publisher.Hooks;

import java.io.IOException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.springframework.util.StringUtils.tokenizeToStringArray;

/**
 * @author chenggang
 * @date 12/15/21.
 */
@TestInstance(PER_CLASS)
public class MybatisR2dbcBaseTests extends R2dbcTestConfig {

    protected R2dbcMybatisProperties r2dbcMybatisProperties;
    protected R2dbcConnectionFactoryProperties r2dbcConnectionFactoryProperties;
    protected R2dbcMybatisConfiguration r2dbcMybatisConfiguration;
    protected ConnectionFactory connectionFactory;
    protected ReactiveSqlSessionFactory reactiveSqlSessionFactory;

    @BeforeAll
    public void setUp() throws Exception {
        BlockHound.install();
        Hooks.onOperatorDebug();
        Hooks.enableContextLossTracking();
        this.r2dbcMybatisProperties = this.r2dbcMybatisProperties();
        this.r2dbcConnectionFactoryProperties = this.r2dbcConnectionFactoryProperties();
        this.r2dbcMybatisConfiguration = this.configuration(this.r2dbcMybatisProperties);
        this.connectionFactory = this.connectionFactory(this.r2dbcConnectionFactoryProperties);
        this.reactiveSqlSessionFactory = this.reactiveSqlSessionFactory(this.r2dbcMybatisConfiguration, this.connectionFactory);
    }

    @Test
    public void testConfigurationByProperties() throws Exception {
        assertThat(this.r2dbcMybatisConfiguration, notNullValue());
        assertThat(this.connectionFactory, notNullValue());
        assertThat(this.reactiveSqlSessionFactory, notNullValue());
    }

    protected R2dbcMybatisProperties r2dbcMybatisProperties() {
        R2dbcMybatisProperties r2dbcMybatisProperties = new R2dbcMybatisProperties();
        r2dbcMybatisProperties.setMapperLocations(new String[]{"mapper/*.xml"});
        r2dbcMybatisProperties.setMapUnderscoreToCamelCase(true);
        r2dbcMybatisProperties.setTypeAliasesPackage("pro.chenggang.project.reactive.mybatis.support.r2dbc.application.entity.model");
        return r2dbcMybatisProperties;
    }

    protected R2dbcConnectionFactoryProperties r2dbcConnectionFactoryProperties() {
        R2dbcConnectionFactoryProperties r2dbcConnectionFactoryProperties = new R2dbcConnectionFactoryProperties();
        r2dbcConnectionFactoryProperties.setEnableMetrics(true);
        r2dbcConnectionFactoryProperties.setName("test-mybatis-r2dbc");
        r2dbcConnectionFactoryProperties.setJdbcUrl("r2dbc:mysql://" + super.databaseIp + ":" + super.databasePort + "/" + super.databaseName);
        r2dbcConnectionFactoryProperties.setUsername(super.databaseUsername);
        r2dbcConnectionFactoryProperties.setPassword(super.databasePassword);
        R2dbcConnectionFactoryProperties.Pool pool = new R2dbcConnectionFactoryProperties.Pool();
        pool.setMaxIdleTime(super.maxIdleTime);
        pool.setValidationQuery("SELECT 1 FROM DUAL");
        pool.setInitialSize(super.initialSize);
        pool.setMaxSize(super.maxSize);
        r2dbcConnectionFactoryProperties.setPool(pool);
        return r2dbcConnectionFactoryProperties;
    }


    protected R2dbcMybatisConfiguration configuration(R2dbcMybatisProperties properties) {
        R2dbcMybatisConfiguration configuration = new R2dbcMybatisConfiguration();
        configuration.setMapUnderscoreToCamelCase(properties.isMapUnderscoreToCamelCase());
        if (properties.getTypeAliasesPackage() != null) {
            String[] typeAliasPackageArray = tokenizeToStringArray(properties.getTypeAliasesPackage(),
                    ConfigurableApplicationContext.CONFIG_LOCATION_DELIMITERS);
            for (String packageToScan : typeAliasPackageArray) {
                configuration.getTypeAliasRegistry().registerAliases(packageToScan, Object.class);
            }
        }
        String[] mapperLocations = properties.getMapperLocations();
        Resource[] mapperResources = this.resolveMapperLocations(mapperLocations);
        if (mapperLocations != null && mapperLocations.length > 0) {
            for (Resource mapperLocation : mapperResources) {
                if (mapperLocation == null) {
                    continue;
                }
                try {
                    XMLMapperBuilder xmlMapperBuilder = new XMLMapperBuilder(mapperLocation.getInputStream(),
                            configuration, mapperLocation.toString(), configuration.getSqlFragments());
                    xmlMapperBuilder.parse();
                } catch (Exception e) {
                    throw new IllegalArgumentException("Failed to parse mapping resource: '" + mapperLocation + "'", e);
                } finally {
                    ErrorContext.instance().reset();
                }
            }
        } else {
            throw new IllegalArgumentException("mapperLocations cannot be empty...");
        }
        return configuration;
    }

    protected ConnectionPool connectionFactory(R2dbcConnectionFactoryProperties r2dbcConnectionFactoryProperties) {
        ConnectionFactory connectionFactory = ConnectionFactories.get(r2dbcConnectionFactoryProperties.determineConnectionFactoryUrl());
        if (connectionFactory instanceof ConnectionPool) {
            return (ConnectionPool) connectionFactory;
        }
        R2dbcConnectionFactoryProperties.Pool pool = r2dbcConnectionFactoryProperties.getPool();
        ConnectionPoolConfiguration.Builder builder = ConnectionPoolConfiguration.builder(connectionFactory)
                .name(r2dbcConnectionFactoryProperties.determineConnectionFactoryName())
                .maxSize(pool.getMaxSize())
                .initialSize(pool.getInitialSize())
                .maxIdleTime(pool.getMaxIdleTime())
                .acquireRetry(pool.getAcquireRetry())
                .backgroundEvictionInterval(pool.getBackgroundEvictionInterval())
                .maxAcquireTime(pool.getMaxAcquireTime())
                .maxCreateConnectionTime(pool.getMaxCreateConnectionTime())
                .maxLifeTime(pool.getMaxLifeTime())
                .validationDepth(pool.getValidationDepth());
        if (pool.getValidationQuery() != null) {
            builder.validationQuery(pool.getValidationQuery());
        } else {
            builder.validationDepth(ValidationDepth.LOCAL);
        }
        ConnectionPool connectionPool = new ConnectionPool(builder.build());
        return connectionPool;
    }

    protected ReactiveSqlSessionFactory reactiveSqlSessionFactory(R2dbcMybatisConfiguration configuration, ConnectionFactory connectionFactory) {
        return DefaultReactiveSqlSessionFactory.newBuilder()
                .withConnectionFactory(connectionFactory)
                .withR2dbcMybatisConfiguration(configuration)
                .build();
    }

    protected Method findMethod(Class<?> clazz, String methodName) {
        for (Method declaredMethod : clazz.getDeclaredMethods()) {
            if (declaredMethod.getName().equals(methodName)) {
                return declaredMethod;
            }
        }
        return null;
    }

    public Resource[] resolveMapperLocations(String[] mapperLocations) {
        ResourcePatternResolver resourceResolver = new PathMatchingResourcePatternResolver();
        List<Resource> resources = new ArrayList<>();
        if (mapperLocations != null) {
            for (String mapperLocation : mapperLocations) {
                try {
                    Resource[] mappers = resourceResolver.getResources(mapperLocation);
                    resources.addAll(Arrays.asList(mappers));
                } catch (IOException ignore) {
                    //ignore
                }
            }
        }
        return resources.toArray(new Resource[0]);
    }
}

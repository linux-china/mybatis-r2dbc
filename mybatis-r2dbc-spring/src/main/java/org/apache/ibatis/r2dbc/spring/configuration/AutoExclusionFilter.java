package org.apache.ibatis.r2dbc.spring.configuration;

import io.r2dbc.spi.ConnectionFactory;
import org.springframework.boot.autoconfigure.AutoConfigurationImportFilter;
import org.springframework.boot.autoconfigure.AutoConfigurationMetadata;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.context.annotation.Configuration;

import java.util.HashSet;
import java.util.Set;

/**
 * exclude DataSourceAutoConfiguration
 *
 * @author evans
 */
@Configuration
@ConditionalOnClass(ConnectionFactory.class)
public class AutoExclusionFilter implements AutoConfigurationImportFilter {

    private static final Set<String> SHOULD_SKIP = new HashSet<>();

    public AutoExclusionFilter() {
        SHOULD_SKIP.add("org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration");
    }

    @Override
    public boolean[] match(String[] classNames, AutoConfigurationMetadata metadata) {
        boolean[] matches = new boolean[classNames.length];
        for (int i = 0; i < classNames.length; i++) {
            matches[i] = !SHOULD_SKIP.contains(classNames[i]);
        }
        return matches;
    }
}
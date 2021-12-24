package org.apache.ibatis.r2dbc.suite.properties;

import io.r2dbc.spi.ValidationDepth;
import reactor.util.annotation.Nullable;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.UUID;

/**
 * @author: chenggang
 * @date 6/25/21.
 */
public class R2dbcConnectionFactoryProperties {

    /**
     * Name of the connection factory
     */
    private String name;

    /**
     * Whether to generate a random connection factory name.
     */
    private boolean generateUniqueName = true;

    /**
     * Jdbc format URL .
     */
    private String jdbcUrl;

    /**
     * R2dbc format Url
     */
    private String r2dbcUrl;

    /**
     * Login username of the database.
     */
    private String username;

    /**
     * Login password of the database.
     */
    private String password;

    /**
     * r2dbc factory pull
     */
    private Pool pool = new Pool();

    /**
     * r2dbc connection factory metrics enabled
     */
    private Boolean enableMetrics = Boolean.FALSE;

    private static boolean containsText(CharSequence str) {
        int strLen = str.length();
        for (int i = 0; i < strLen; i++) {
            if (!Character.isWhitespace(str.charAt(i))) {
                return true;
            }
        }
        return false;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public boolean isGenerateUniqueName() {
        return generateUniqueName;
    }

    public void setGenerateUniqueName(boolean generateUniqueName) {
        this.generateUniqueName = generateUniqueName;
    }

    public String getJdbcUrl() {
        return jdbcUrl;
    }

    public void setJdbcUrl(String jdbcUrl) {
        this.jdbcUrl = jdbcUrl;
    }

    public String getR2dbcUrl() {
        return r2dbcUrl;
    }

    public void setR2dbcUrl(String r2dbcUrl) {
        this.r2dbcUrl = r2dbcUrl;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public Pool getPool() {
        return pool;
    }

    public void setPool(Pool pool) {
        this.pool = pool;
    }

    public Boolean getEnableMetrics() {
        return enableMetrics;
    }

    public void setEnableMetrics(Boolean enableMetrics) {
        this.enableMetrics = enableMetrics;
    }

    /**
     * r2dbc connection factory name based on configuration
     *
     * @return the connection factory name to use or {@code null}
     */
    public String determineConnectionFactoryName() {
        if (this.generateUniqueName && !hasText(this.name)) {
            this.name = UUID.randomUUID().toString();
        }
        return this.name;
    }

    /**
     * r2dbc connection url
     *
     * @return
     */
    public String determineConnectionFactoryUrl() {
        if (!hasText(this.jdbcUrl) && !hasText(this.r2dbcUrl)) {
            return null;
        }
        if (hasText(this.r2dbcUrl)) {
            this.r2dbcUrl = this.r2dbcUrl.replace("r2dbc:mysql:", "r2dbc:mariadb:");
            return this.r2dbcUrl;
        }
        String encodedUsername;
        try {
            encodedUsername = URLEncoder.encode(username, StandardCharsets.UTF_8.name());
        } catch (UnsupportedEncodingException e) {
            //fallback to original username
            encodedUsername = username;
        }
        String encodedPassword;
        try {
            encodedPassword = URLEncoder.encode(password, StandardCharsets.UTF_8.name());
        } catch (UnsupportedEncodingException e) {
            //fallback to original password
            encodedPassword = password;
        }
        String credential = encodedUsername + (password == null || password.isEmpty() ? "" : ":" + encodedPassword);
        this.r2dbcUrl = this.jdbcUrl.replace("r2dbc:mysql:", "r2dbc:mariadb:");
        this.r2dbcUrl = r2dbcUrl.replace("//", "//" + credential + "@");
        return this.r2dbcUrl;
    }

    @Override
    public String toString() {
        return "R2dbcConnectionFactoryProperties[" +
                "name='" + name + '\'' +
                ", generateUniqueName=" + generateUniqueName +
                ", jdbcUrl='" + jdbcUrl + '\'' +
                ", r2dbcUrl='" + r2dbcUrl + '\'' +
                ", username='" + username + '\'' +
                ", password='" + password + '\'' +
                ", pool=" + pool +
                ", enableMetrics=" + enableMetrics +
                ']';
    }

    private boolean hasText(@Nullable String str) {
        return (str != null && !str.isEmpty() && containsText(str));
    }

    public static class Pool {

        /**
         * r2dbc connection factory initial size
         * default 1
         */
        private Integer initialSize = 1;

        /**
         * r2dbc connection factory max size
         * default 10
         */
        private Integer maxSize = 10;

        /**
         * r2dbc connection factory max idle time
         */
        private Duration maxIdleTime = Duration.ofMinutes(30);

        /**
         * r2dbc connection factory max create connection time
         * ZERO indicates no-timeout
         */
        private Duration maxCreateConnectionTime = Duration.ZERO;
        /**
         * r2dbc connection factory max acquire time
         * ZERO indicates no-timeout
         */
        private Duration maxAcquireTime = Duration.ZERO;

        /**
         * r2dbc connection factory max life time
         * ZERO indicates no-lifetime
         */
        private Duration maxLifeTime = Duration.ZERO;
        /**
         * r2dbc connection factory validation query
         */
        @Nullable
        private String validationQuery;

        /**
         * r2dbc connection factory validation depth
         * LOCAL Perform a client-side only validation
         * REMOTE Perform a remote connection validations
         * {@link ValidationDepth}
         */
        private ValidationDepth validationDepth = ValidationDepth.REMOTE;

        /**
         * r2dbc connection factory acquire retry
         * ZERO indicates no-retry
         * default 1
         */
        private int acquireRetry = 1;

        /**
         * r2dbc connection factory background eviction interval
         * ZERO indicates no-timeout, negative marks unconfigured.
         */
        private Duration backgroundEvictionInterval = Duration.ofNanos(-1);

        public Integer getInitialSize() {
            return initialSize;
        }

        public void setInitialSize(Integer initialSize) {
            this.initialSize = initialSize;
        }

        public Integer getMaxSize() {
            return maxSize;
        }

        public void setMaxSize(Integer maxSize) {
            this.maxSize = maxSize;
        }

        public Duration getMaxIdleTime() {
            return maxIdleTime;
        }

        public void setMaxIdleTime(Duration maxIdleTime) {
            this.maxIdleTime = maxIdleTime;
        }

        public Duration getMaxCreateConnectionTime() {
            return maxCreateConnectionTime;
        }

        public void setMaxCreateConnectionTime(Duration maxCreateConnectionTime) {
            this.maxCreateConnectionTime = maxCreateConnectionTime;
        }

        public Duration getMaxAcquireTime() {
            return maxAcquireTime;
        }

        public void setMaxAcquireTime(Duration maxAcquireTime) {
            this.maxAcquireTime = maxAcquireTime;
        }

        public Duration getMaxLifeTime() {
            return maxLifeTime;
        }

        public void setMaxLifeTime(Duration maxLifeTime) {
            this.maxLifeTime = maxLifeTime;
        }

        @Nullable
        public String getValidationQuery() {
            return validationQuery;
        }

        public void setValidationQuery(@Nullable String validationQuery) {
            this.validationQuery = validationQuery;
        }

        public ValidationDepth getValidationDepth() {
            return validationDepth;
        }

        public void setValidationDepth(ValidationDepth validationDepth) {
            this.validationDepth = validationDepth;
        }

        public int getAcquireRetry() {
            return acquireRetry;
        }

        public void setAcquireRetry(int acquireRetry) {
            this.acquireRetry = acquireRetry;
        }

        public Duration getBackgroundEvictionInterval() {
            return backgroundEvictionInterval;
        }

        public void setBackgroundEvictionInterval(Duration backgroundEvictionInterval) {
            this.backgroundEvictionInterval = backgroundEvictionInterval;
        }

        @Override
        public String toString() {
            return "Pool[" +
                    "initialSize=" + initialSize +
                    ", maxSize=" + maxSize +
                    ", maxIdleTime=" + maxIdleTime +
                    ", maxCreateConnectionTime=" + maxCreateConnectionTime +
                    ", maxAcquireTime=" + maxAcquireTime +
                    ", maxLifeTime=" + maxLifeTime +
                    ", validationQuery='" + validationQuery + '\'' +
                    ", validationDepth=" + validationDepth +
                    ", acquireRetry=" + acquireRetry +
                    ", backgroundEvictionInterval=" + backgroundEvictionInterval +
                    ']';
        }
    }
}

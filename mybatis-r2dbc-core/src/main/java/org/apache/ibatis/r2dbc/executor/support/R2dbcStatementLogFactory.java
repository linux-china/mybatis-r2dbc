package org.apache.ibatis.r2dbc.executor.support;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import org.apache.ibatis.mapping.MappedStatement;
import org.apache.ibatis.r2dbc.delegate.R2dbcMybatisConfiguration;

/**
 * The type R2dbc statement log factory.
 *
 * @author chenggang
 * @date 2022/1/6.
 * @since 1.0.0
 */
public class R2dbcStatementLogFactory {

    private final Map<String,R2dbcStatementLog> r2dbcStatementLogContainer = new HashMap<>();
    private final R2dbcMybatisConfiguration configuration;

    /**
     * Instantiates a new r2dbc statement log factory.
     *
     * @param r2dbcMybatisConfiguration the r2dbc mybatis configuration
     */
    public R2dbcStatementLogFactory(R2dbcMybatisConfiguration r2dbcMybatisConfiguration) {
        this.configuration = r2dbcMybatisConfiguration;
    }

    /**
     * Init r2dbc statement log.
     *
     * @param mappedStatement the mapped statement
     */
    public void initR2dbcStatementLog(MappedStatement mappedStatement){
        String logId = mappedStatement.getId();
        if (configuration.getLogPrefix() != null) {
            logId = configuration.getLogPrefix() + mappedStatement.getId();
        }
        r2dbcStatementLogContainer.put(logId,new R2dbcStatementLog(mappedStatement.getStatementLog()));
    }

    /**
     * Get r2dbc statement log optional.
     *
     * @param mappedStatement the MappedStatement
     * @return the R2dbcStatementLog
     */
    public R2dbcStatementLog getR2dbcStatementLog(MappedStatement mappedStatement){
        String logId = mappedStatement.getId();
        if (configuration.getLogPrefix() != null) {
            logId = configuration.getLogPrefix() + mappedStatement.getId();
        }
        R2dbcStatementLog r2dbcStatementLog = r2dbcStatementLogContainer.get(logId);
        if(Objects.nonNull(r2dbcStatementLog)){
            return r2dbcStatementLog;
        }
        r2dbcStatementLog = new R2dbcStatementLog(mappedStatement.getStatementLog());
        this.r2dbcStatementLogContainer.put(logId,r2dbcStatementLog);
        return r2dbcStatementLog;
    }

    /**
     * get all r2dbc statement logs
     * @return unmodifiable Map<String,R2dbcStatementLog>
     */
    public Map<String,R2dbcStatementLog> getAllR2dbcStatementLog(){
        return Collections.unmodifiableMap(this.r2dbcStatementLogContainer);
    }

}

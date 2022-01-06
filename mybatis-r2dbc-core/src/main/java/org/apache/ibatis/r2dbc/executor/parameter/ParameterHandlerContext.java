package org.apache.ibatis.r2dbc.executor.parameter;

import org.apache.ibatis.type.JdbcType;

/**
 * @author chenggang
 * @date 12/9/21.
 */
public class ParameterHandlerContext {

    private int index;
    private JdbcType jdbcType;
    private Class javaType;


    public int getIndex() {
        return index;
    }

    public void setIndex(int index) {
        this.index = index;
    }

    public JdbcType getJdbcType() {
        return jdbcType;
    }

    public void setJdbcType(JdbcType jdbcType) {
        this.jdbcType = jdbcType;
    }

    public Class getJavaType() {
        return javaType;
    }

    public void setJavaType(Class javaType) {
        this.javaType = javaType;
    }
}

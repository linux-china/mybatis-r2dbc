package org.apache.ibatis.r2dbc.executor.support;

import org.apache.ibatis.builder.SqlSourceBuilder;
import org.apache.ibatis.logging.Log;
import org.apache.ibatis.reflection.ArrayUtil;

import java.sql.Array;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class R2dbcStatementLog {

    private final Log statementLog;

    public R2dbcStatementLog(Log statementLog) {
        this.statementLog = statementLog;
    }

    public void logSql(String sql) {
        if (statementLog.isDebugEnabled()) {
            debug(" Preparing: " + removeExtraWhitespace(sql), true);
        }
    }

    public void logParameters(List<Object> columnValues) {
        debug("Parameters: " + getParameterValueString(columnValues), true);
    }

    public void logUpdates(Integer updateCount) {
        debug("   Updates: " + updateCount, false);
    }

    public void logTotal(Integer rows) {
        debug("     Total: " + rows, false);
    }

    private String getParameterValueString(List<Object> columnValues) {
        List<Object> typeList = new ArrayList<>(columnValues.size());
        for (Object value : columnValues) {
            if (value == null) {
                typeList.add("null");
            } else {
                typeList.add(objectValueString(value) + "(" + value.getClass().getSimpleName() + ")");
            }
        }
        final String parameters = typeList.toString();
        return parameters.substring(1, parameters.length() - 1);
    }

    private String objectValueString(Object value) {
        if (value instanceof Array) {
            try {
                return ArrayUtil.toString(((Array) value).getArray());
            } catch (SQLException e) {
                return value.toString();
            }
        }
        return value.toString();
    }

    private String removeExtraWhitespace(String original) {
        return SqlSourceBuilder.removeExtraWhitespaces(original);
    }

    private void debug(String text, boolean input) {
        if (statementLog.isDebugEnabled()) {
            statementLog.debug(prefix(input) + text);
        }
    }

    private void trace(String text, boolean input) {
        if (statementLog.isTraceEnabled()) {
            statementLog.trace(prefix(input) + text);
        }
    }

    private String prefix(boolean isInput) {
        char[] buffer = new char[2 + 2];
        Arrays.fill(buffer, '=');
        buffer[2 + 1] = ' ';
        if (isInput) {
            buffer[2] = '>';
        } else {
            buffer[0] = '<';
        }
        return new String(buffer);
    }
}
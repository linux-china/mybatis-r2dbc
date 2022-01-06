package org.apache.ibatis.r2dbc.executor.support;

import io.r2dbc.spi.Connection;
import io.r2dbc.spi.IsolationLevel;

import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/**
 * @author chenggang
 * @date 12/9/21.
 */
public class ReactiveExecutorContext {

    private final AtomicBoolean activeTransaction = new AtomicBoolean(false);
    private final AtomicReference<Connection> connectionReference = new AtomicReference<>();
    private final AtomicBoolean forceCommit = new AtomicBoolean(false);
    private final AtomicBoolean forceRollback = new AtomicBoolean(false);
    private final AtomicBoolean requireClosed = new AtomicBoolean(false);
    private final AtomicBoolean dirty = new AtomicBoolean(false);
    private final AtomicBoolean withTransaction = new AtomicBoolean(false);
    private final boolean autoCommit;
    private final IsolationLevel isolationLevel;
    private R2dbcStatementLog r2dbcStatementLog;

    public ReactiveExecutorContext(boolean autoCommit, IsolationLevel isolationLevel) {
        this.autoCommit = autoCommit;
        this.isolationLevel = isolationLevel;
    }

    public boolean isAutoCommit() {
        return autoCommit;
    }

    public boolean isForceCommit() {
        return forceCommit.get();
    }

    public void setForceCommit(boolean forceCommit) {
        this.forceCommit.getAndSet(forceCommit);
    }

    public boolean isForceRollback() {
        return forceRollback.get();
    }

    public void setForceRollback(boolean forceRollback) {
        this.forceRollback.getAndSet(forceRollback);
    }

    public boolean isDirty() {
        return dirty.get();
    }

    public void setDirty() {
        this.dirty.compareAndSet(false, true);
    }

    public void resetDirty() {
        this.dirty.compareAndSet(true, false);
    }

    public void setWithTransaction() {
        this.withTransaction.compareAndSet(false, true);
    }

    public void resetWithTransaction() {
        this.withTransaction.compareAndSet(true, false);
    }

    public boolean isWithTransaction() {
        return this.withTransaction.get();
    }

    public boolean setActiveTransaction() {
        return this.activeTransaction.compareAndSet(false, true);
    }

    public boolean isRequireClosed() {
        return this.requireClosed.get();
    }

    public void setRequireClosed(boolean requireClosed) {
        this.requireClosed.getAndSet(requireClosed);
    }

    public IsolationLevel getIsolationLevel() {
        return isolationLevel;
    }

    public R2dbcStatementLog getR2dbcStatementLog() {
        return r2dbcStatementLog;
    }

    public void setR2dbcStatementLog(R2dbcStatementLog r2dbcStatementLog) {
        this.r2dbcStatementLog = r2dbcStatementLog;
    }

    public boolean bindConnection(Connection connection) {
        return this.connectionReference.compareAndSet(null, connection);
    }

    public Optional<Connection> clearConnection() {
        return Optional.ofNullable(this.connectionReference.getAndSet(null));
    }

    public Optional<Connection> getConnection() {
        return Optional.ofNullable(this.connectionReference.get());
    }

    @Override
    public String toString() {
        return "ReactiveExecutorContext [" +
                ", connectionReference=" + connectionReference +
                ", forceCommit=" + forceCommit +
                ", forceRollback=" + forceRollback +
                ", requireClosed=" + requireClosed +
                ", r2dbcStatementLog=" + r2dbcStatementLog +
                " ]";
    }
}

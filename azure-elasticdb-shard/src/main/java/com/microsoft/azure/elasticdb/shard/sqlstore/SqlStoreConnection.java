package com.microsoft.azure.elasticdb.shard.sqlstore;

// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

import com.google.common.base.Preconditions;
import com.microsoft.azure.elasticdb.shard.store.IStoreConnection;
import com.microsoft.azure.elasticdb.shard.store.IStoreTransactionScope;
import com.microsoft.azure.elasticdb.shard.store.StoreConnectionKind;
import com.microsoft.azure.elasticdb.shard.store.StoreTransactionScopeKind;
import com.microsoft.sqlserver.jdbc.SQLServerConnection;
import javafx.concurrent.Task;

import java.io.IOException;
import java.sql.Connection;
import java.util.UUID;

/**
 * Instance of a Sql Store Connection.
 */
public class SqlStoreConnection implements IStoreConnection {
    /**
     * Underlying SQL connection object.
     */
    private Connection _conn;
    /**
     * Type of store connection.
     */
    private StoreConnectionKind kind;

    /**
     * Constructs an instance of Sql Store Connection.
     *
     * @param kind             Type of store connection.
     * @param connectionString
     */
    public SqlStoreConnection(StoreConnectionKind kind, String connectionString) {
        this.kind = Preconditions.checkNotNull(kind);
        /*_conn = new SqlConnection();
        _conn.ConnectionString = connectionString;*/
    }

    public StoreConnectionKind getKind() {
        return kind;
    }


    /**
     * Open the store connection.
     */
    public void Open() {
        /*SqlUtils.WithSqlExceptionHandling(() -> {
            _conn.Open();
        });*/
    }

    /**
     * Asynchronously open the store connection.
     *
     * @return A task to await completion of the Open
     */
    public Task OpenAsync() {
        //TODO
        return null;
        /*return SqlUtils.WithSqlExceptionHandlingAsync(() -> {
            return _conn.OpenAsync();
        });*/
    }

    /**
     * Open the store connection, and acquire a lock on the store.
     *
     * @param lockId Lock Id.
     */
    public void OpenWithLock(UUID lockId) {
        /*SqlUtils.WithSqlExceptionHandling(() -> {
            _conn.Open();
            this.GetAppLock(lockId);
        });*/
    }

    /**
     * Closes the store connection.
     */
    public void Close() {
        /*SqlUtils.WithSqlExceptionHandling(() -> {
            if (_conn != null) {
                _conn.Dispose();
                _conn = null;
            }
        });*/
    }

    /**
     * Closes the store connection after releasing lock.
     *
     * @param lockId Lock Id.
     */
    public void CloseWithUnlock(UUID lockId) {
        /*SqlUtils.WithSqlExceptionHandling(() -> {
            if (_conn != null) {
                if (_conn.State == ConnectionState.Open) {
                    this.ReleaseAppLock(lockId);
                }
                _conn.Dispose();
                _conn = null;
            }
        });*/
    }

    /**
     * Acquires a transactional scope on the connection.
     *
     * @param kind Type of transaction scope.
     * @return Transaction scope on the store connection.
     */
    @Override
    public IStoreTransactionScope GetTransactionScope(StoreTransactionScopeKind kind) {
        return new SqlStoreTransactionScope(kind, _conn);
    }

    ///#region IDisposable

    /**
     * Disposes the object.
     */
    public final void Dispose() {
        this.Dispose(true);
        //TODO: GC.SuppressFinalize(this);
    }

    /**
     * Performs actual Dispose of resources.
     *
     * @param disposing Whether the invocation was from IDisposable.Dipose method.
     */
    protected void Dispose(boolean disposing) {
        if (disposing) {
            this.Close();
        }
    }

    ///#endregion IDisposable

    /**
     * Acquires an application level lock on the connection which is session scoped.
     *
     * @param lockId Identity of the lock.
     */
    private void GetAppLock(UUID lockId) {
        /*try (SqlCommand cmdGetAppLock = _conn.CreateCommand()) {
            cmdGetAppLock.CommandText = "sp_getapplock";
            cmdGetAppLock.CommandType = CommandType.StoredProcedure;

            SqlUtils.AddCommandParameter(cmdGetAppLock, "@Resource", SqlDbType.NVarChar, ParameterDirection.Input, 255 * 2, lockId.toString());

            SqlUtils.AddCommandParameter(cmdGetAppLock, "@LockMode", SqlDbType.NVarChar, ParameterDirection.Input, 32 * 2, "Exclusive");

            SqlUtils.AddCommandParameter(cmdGetAppLock, "@LockOwner", SqlDbType.NVarChar, ParameterDirection.Input, 32 * 2, "Session");

            SqlUtils.AddCommandParameter(cmdGetAppLock, "@LockTimeout", SqlDbType.Int, ParameterDirection.Input, 0, GlobalConstants.DefaultLockTimeOut);

            SqlParameter returnValue = SqlUtils.AddCommandParameter(cmdGetAppLock, "@RETURN_VALUE", SqlDbType.Int, ParameterDirection.ReturnValue, 0, 0);

            cmdGetAppLock.ExecuteNonQuery();

            // If time-out or other errors happen.
            if ((int) returnValue.Value < 0) {
                throw new ShardManagementException(ShardManagementErrorCategory.General, ShardManagementErrorCode.LockNotAcquired, Errors._Store_SqlOperation_LockNotAcquired, lockId);
            }
        }*/
    }

    /**
     * Releases an application level lock on the connection which is session scoped.
     *
     * @param lockId Identity of the lock.
     */
    private void ReleaseAppLock(UUID lockId) {
        /*try (SqlCommand cmdReleaseAppLock = _conn.CreateCommand()) {
            cmdReleaseAppLock.CommandText = "sp_releaseapplock";
            cmdReleaseAppLock.CommandType = CommandType.StoredProcedure;

            SqlUtils.AddCommandParameter(cmdReleaseAppLock, "@Resource", SqlDbType.NVarChar, ParameterDirection.Input, 255 * 2, lockId.toString());

            SqlUtils.AddCommandParameter(cmdReleaseAppLock, "@LockOwner", SqlDbType.NVarChar, ParameterDirection.Input, 32 * 2, "Session");

            SqlParameter returnValue = SqlUtils.AddCommandParameter(cmdReleaseAppLock, "@RETURN_VALUE", SqlDbType.Int, ParameterDirection.ReturnValue, 0, 0);

            try {
                cmdReleaseAppLock.ExecuteNonQuery();
            } catch (RuntimeException e) {
                // ignore all exceptions.
                return;
            }

            // If parameter validation or other errors happen.
            if ((int) returnValue.Value < 0) {
                throw new ShardManagementException(ShardManagementErrorCategory.General, ShardManagementErrorCode.LockNotReleased, Errors._Store_SqlOperation_LockNotReleased, lockId);
            }
        }*/
    }

    @Override
    public void close() throws IOException {

    }
}

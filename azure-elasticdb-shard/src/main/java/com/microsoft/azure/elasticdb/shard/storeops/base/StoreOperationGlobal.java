package com.microsoft.azure.elasticdb.shard.storeops.base;

// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

import com.microsoft.azure.elasticdb.core.commons.transientfaulthandling.RetryPolicy;
import com.microsoft.azure.elasticdb.shard.mapmanager.ShardManagementErrorCategory;
import com.microsoft.azure.elasticdb.shard.mapmanager.ShardManagementException;
import com.microsoft.azure.elasticdb.shard.sqlstore.SqlShardMapManagerCredentials;
import com.microsoft.azure.elasticdb.shard.sqlstore.SqlStoreConnection;
import com.microsoft.azure.elasticdb.shard.store.*;
import com.microsoft.azure.elasticdb.shard.utils.ExceptionUtils;

import java.io.IOException;
import java.util.concurrent.Callable;

/**
 * Performs a GSM only store operation.
 */
public abstract class StoreOperationGlobal implements IStoreOperationGlobal {
    /**
     * GSM connection.
     */
    private IStoreConnection _globalConnection;

    /**
     * Credentials for connection establishment.
     */
    private SqlShardMapManagerCredentials _credentials;

    /**
     * Retry policy.
     */
    private RetryPolicy _retryPolicy;
    /**
     * Operation name, useful for diagnostics.
     */
    private String OperationName;

    /**
     * Constructs an instance of SqlOperationGlobal.
     *
     * @param credentials   Credentials for connecting to SMM GSM database.
     * @param retryPolicy   Retry policy for requests.
     * @param operationName Operation name, useful for diagnostics.
     */
    public StoreOperationGlobal(SqlShardMapManagerCredentials credentials, RetryPolicy retryPolicy, String operationName) {
        this.setOperationName(operationName);
        _credentials = credentials;
        _retryPolicy = retryPolicy;
    }

    protected final String getOperationName() {
        return OperationName;
    }

    private void setOperationName(String value) {
        OperationName = value;
    }

    /**
     * Whether this is a read-only operation.
     */
    public abstract boolean getReadOnly();

    /**
     * Performs the store operation.
     *
     * @return Results of the operation.
     */
    public final IStoreResults Do() {
        IStoreResults result;

        try {
            do {
                result = _retryPolicy.ExecuteAction(() -> {
                    IStoreResults r = null;
                    try {
                        // Open connection.
                        this.EstablishConnnection();

                        try (IStoreTransactionScope ts = this.GetTransactionScope()) {
                            r = this.DoGlobalExecute(ts);

                            ts.setSuccess(r.getResult() == StoreResult.Success);
                        } catch (IOException e) {
                            e.printStackTrace();
                            //TODO
                        }

                        if (r.getStoreOperations().isEmpty()) {
                            if (r.getResult() != StoreResult.Success) {
                                this.DoGlobalUpdateCachePre(r);

                                this.HandleDoGlobalExecuteError(r);
                            }

                            this.DoGlobalUpdateCachePost(r);
                        }

                        return r;
                    } finally {
                        // Close connection.
                        this.TeardownConnection();
                    }
                });

                // If pending operation, deserialize the pending operation and perform Undo.
                if (!result.getStoreOperations().isEmpty()) {
                    assert result.getStoreOperations().size() == 1;

                    this.UndoPendingStoreOperations(result.getStoreOperations().get(0));
                }
            } while (!result.getStoreOperations().isEmpty());
        } catch (StoreException se) {
            throw this.OnStoreException(se);
        }
        assert result != null;
        return result;
    }

    /**
     * Asynchronously performs the store operation.
     *
     * @return Task encapsulating the results of the operation.
     */
    public final Callable<IStoreResults> DoAsync() {
        IStoreResults result;

        //TODO
        return null;
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
        if (_globalConnection != null) {
            //TODO: _globalConnection.Dispose();
            _globalConnection = null;
        }
    }

    ///#endregion IDisposable

    /**
     * Execute the operation against GSM in the current transaction scope.
     *
     * @param ts Transaction scope.
     * @return Results of the operation.
     */
    public abstract IStoreResults DoGlobalExecute(IStoreTransactionScope ts);

    /**
     * Asynchronously execute the operation against GSM in the current transaction scope.
     *
     * @param ts Transaction scope.
     * @return Task encapsulating results of the operation.
     */
    public Callable<IStoreResults> DoGlobalExecuteAsync(IStoreTransactionScope ts) {
        // Currently only implemented by FindMappingByKeyGlobalOperation
        throw new UnsupportedOperationException();
    }

    /**
     * Invalidates the cache on unsuccessful commit of the GSM operation.
     *
     * @param result Operation result.
     */
    public void DoGlobalUpdateCachePre(IStoreResults result) {
    }

    /**
     * Handles errors from the GSM operation after the LSM operations.
     *
     * @param result Operation result.
     */
    public abstract void HandleDoGlobalExecuteError(IStoreResults result);

    /**
     * Refreshes the cache on successful commit of the GSM operation.
     *
     * @param result Operation result.
     */
    public void DoGlobalUpdateCachePost(IStoreResults result) {
    }

    /**
     * Returns the ShardManagementException to be thrown corresponding to a StoreException.
     *
     * @param se Store exception that has been raised.
     * @return ShardManagementException to be thrown.
     */
    public ShardManagementException OnStoreException(StoreException se) {
        return ExceptionUtils.GetStoreExceptionGlobal(this.getErrorCategory(), se, this.getOperationName());
    }

    /**
     * Error category for store exception.
     */
    protected abstract ShardManagementErrorCategory getErrorCategory();

    /**
     * Performs undo of the storage operation that is pending.
     *
     * @param logEntry Log entry for the pending operation.
     */
    protected void UndoPendingStoreOperations(IStoreLogEntry logEntry) throws Exception {
        // Will only be implemented by LockOrUnLockMapping operation
        // which will actually perform the undo operation.
        throw new UnsupportedOperationException();
    }

    /**
     * Asynchronously performs undo of the storage operation that is pending.
     *
     * @param logEntry Log entry for the pending operation.
     * @return Task to await Undo of the operation
     * Currently not used anywhere since the Async APIs were added
     * in support of the look-up operations
     */
    protected Callable UndoPendingStoreOperationsAsync(IStoreLogEntry logEntry) {
        // Currently async APIs are only used by FindMappingByKeyGlobalOperation
        // which doesn't require Undo
        throw new UnsupportedOperationException();
    }

    /**
     * Establishes connection to the SMM GSM database.
     */
    private void EstablishConnnection() {
        _globalConnection = new SqlStoreConnection(StoreConnectionKind.Global, _credentials.getConnectionStringShardMapManager());
        _globalConnection.Open();
    }

    /**
     * Asynchronously establishes connection to the SMM GSM database.
     *
     * @return Task to await connection establishment
     */
    private Callable EstablishConnnectionAsync() {
        _globalConnection = new SqlStoreConnection(StoreConnectionKind.Global, _credentials.getConnectionStringShardMapManager());
        return _globalConnection.OpenAsync();
    }

    /**
     * Acquires the transaction scope.
     *
     * @return Transaction scope, operations within the scope excute atomically.
     */
    private IStoreTransactionScope GetTransactionScope() {
        return _globalConnection.GetTransactionScope(this.getReadOnly() ? StoreTransactionScopeKind.ReadOnly : StoreTransactionScopeKind.ReadWrite);
    }

    /**
     * Terminates the connections after finishing the operation.
     */
    private void TeardownConnection() {
        if (_globalConnection != null) {
            _globalConnection.Close();
            _globalConnection = null;
        }
    }
}

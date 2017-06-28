package com.microsoft.azure.elasticdb.shard.storeops.base;

/* Copyright (c) Microsoft. All rights reserved.
Licensed under the MIT license. See LICENSE file in the project root for full license information.*/

import com.microsoft.azure.elasticdb.core.commons.transientfaulthandling.RetryPolicy;
import com.microsoft.azure.elasticdb.shard.mapmanager.ShardManagementErrorCategory;
import com.microsoft.azure.elasticdb.shard.mapmanager.ShardManagementException;
import com.microsoft.azure.elasticdb.shard.sqlstore.SqlShardMapManagerCredentials;
import com.microsoft.azure.elasticdb.shard.sqlstore.SqlStoreConnection;
import com.microsoft.azure.elasticdb.shard.store.IStoreConnection;
import com.microsoft.azure.elasticdb.shard.store.IStoreTransactionScope;
import com.microsoft.azure.elasticdb.shard.store.StoreConnectionKind;
import com.microsoft.azure.elasticdb.shard.store.StoreException;
import com.microsoft.azure.elasticdb.shard.store.StoreLogEntry;
import com.microsoft.azure.elasticdb.shard.store.StoreResult;
import com.microsoft.azure.elasticdb.shard.store.StoreResults;
import com.microsoft.azure.elasticdb.shard.store.StoreTransactionScopeKind;
import com.microsoft.azure.elasticdb.shard.utils.ExceptionUtils;
import java.io.IOException;
import java.util.concurrent.Callable;

/**
 * Performs a GSM only store operation.
 */
public abstract class StoreOperationGlobal implements IStoreOperationGlobal, AutoCloseable {

  /**
   * GSM connection.
   */
  private IStoreConnection globalConnection;

  /**
   * Credentials for connection establishment.
   */
  private SqlShardMapManagerCredentials credentials;

  /**
   * Retry policy.
   */
  private RetryPolicy retryPolicy;
  /**
   * Operation name, useful for diagnostics.
   */
  private String operationName;

  /**
   * Constructs an instance of SqlOperationGlobal.
   *
   * @param credentials Credentials for connecting to SMM GSM database.
   * @param retryPolicy Retry policy for requests.
   * @param operationName Operation name, useful for diagnostics.
   */
  public StoreOperationGlobal(SqlShardMapManagerCredentials credentials, RetryPolicy retryPolicy,
      String operationName) {
    this.setOperationName(operationName);
    this.credentials = credentials;
    this.retryPolicy = retryPolicy;
  }

  protected final String getOperationName() {
    return operationName;
  }

  private void setOperationName(String value) {
    operationName = value;
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
  public final StoreResults doGlobal() {
    StoreResults result;

    try {
      do {
        result = retryPolicy.executeAction(() -> {
          StoreResults r = null;
          try {
            // Open connection.
            this.establishConnnection();

            try (IStoreTransactionScope ts = this.getTransactionScope()) {
              r = this.doGlobalExecute(ts);

              ts.setSuccess(r.getResult() == StoreResult.Success);
            } catch (IOException e) {
              e.printStackTrace();
              throw new StoreException(e.getMessage(),
                  e.getCause() != null ? (Exception) e.getCause() : e);
            }

            if (r.getStoreOperations().isEmpty()) {
              if (r.getResult() != StoreResult.Success) {
                this.doGlobalUpdateCachePre(r);

                this.handleDoGlobalExecuteError(r);
              }

              this.doGlobalUpdateCachePost(r);
            }

            return r;
          } finally {
            // close connection.
            this.teardownConnection();
          }
        });

        // If pending operation, deserialize the pending operation and perform Undo.
        if (!result.getStoreOperations().isEmpty()) {
          assert result.getStoreOperations().size() == 1;

          try {
            this.undoPendingStoreOperations(result.getStoreOperations().get(0));
          } catch (Exception e) {
            e.printStackTrace();
            throw new StoreException(e.getMessage(),
                e.getCause() != null ? (Exception) e.getCause() : e);
          }
        }
      } while (!result.getStoreOperations().isEmpty());
    } catch (StoreException se) {
      throw this.onStoreException(se);
    } catch (InterruptedException e) {
      e.printStackTrace();
      result = null;
    }
    assert result != null;
    return result;
  }

  /**
   * Asynchronously performs the store operation.
   *
   * @return Task encapsulating the results of the operation.
   */
  public final Callable<StoreResults> doAsync() {
    return this::doGlobal;
  }

  ///#region IDisposable

  /**
   * Performs actual Dispose of resources.
   */
  public void close() {
    if (globalConnection != null) {
      globalConnection.close();
      globalConnection = null;
    }
  }

  ///#endregion IDisposable

  /**
   * Execute the operation against GSM in the current transaction scope.
   *
   * @param ts Transaction scope.
   * @return Results of the operation.
   */
  public abstract StoreResults doGlobalExecute(IStoreTransactionScope ts);

  /**
   * Asynchronously execute the operation against GSM in the current transaction scope.
   *
   * @param ts Transaction scope.
   * @return Task encapsulating results of the operation.
   */
  public Callable<StoreResults> doGlobalExecuteAsync(IStoreTransactionScope ts) {
    // Currently only implemented by FindMappingByKeyGlobalOperation
    throw new UnsupportedOperationException();
  }

  /**
   * Invalidates the cache on unsuccessful commit of the GSM operation.
   *
   * @param result Operation result.
   */
  public void doGlobalUpdateCachePre(StoreResults result) {
  }

  /**
   * Handles errors from the GSM operation after the LSM operations.
   *
   * @param result Operation result.
   */
  public abstract void handleDoGlobalExecuteError(StoreResults result);

  /**
   * Refreshes the cache on successful commit of the GSM operation.
   *
   * @param result Operation result.
   */
  public void doGlobalUpdateCachePost(StoreResults result) {
  }

  /**
   * Returns the ShardManagementException to be thrown corresponding to a StoreException.
   *
   * @param se Store exception that has been raised.
   * @return ShardManagementException to be thrown.
   */
  public ShardManagementException onStoreException(StoreException se) {
    return ExceptionUtils.getStoreExceptionGlobal(this.getErrorCategory(), se,
        this.getOperationName());
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
  protected void undoPendingStoreOperations(StoreLogEntry logEntry) throws Exception {
    // Will only be implemented by LockOrUnLockMapping operation
    // which will actually perform the undo operation.
    throw new UnsupportedOperationException();
  }

  /**
   * Asynchronously performs undo of the storage operation that is pending.
   *
   * @param logEntry Log entry for the pending operation.
   * @return Task to await Undo of the operation Currently not used anywhere since the Async APIs
   * were added in support of the look-up operations
   */
  protected Callable undoPendingStoreOperationsAsync(StoreLogEntry logEntry) {
    // Currently async APIs are only used by FindMappingByKeyGlobalOperation
    // which doesn't require Undo
    throw new UnsupportedOperationException();
  }

  /**
   * Establishes connection to the SMM GSM database.
   */
  private void establishConnnection() {
    globalConnection = new SqlStoreConnection(StoreConnectionKind.Global,
        credentials.getConnectionStringShardMapManager());
    globalConnection.open();
  }

  /**
   * Asynchronously establishes connection to the SMM GSM database.
   *
   * @return Task to await connection establishment
   */
  private Callable establishConnnectionAsync() {
    globalConnection = new SqlStoreConnection(StoreConnectionKind.Global,
        credentials.getConnectionStringShardMapManager());
    return globalConnection.openAsync();
  }

  /**
   * Acquires the transaction scope.
   *
   * @return Transaction scope, operations within the scope excute atomically.
   */
  private IStoreTransactionScope getTransactionScope() {
    return globalConnection.getTransactionScope(
        this.getReadOnly() ? StoreTransactionScopeKind.ReadOnly
            : StoreTransactionScopeKind.ReadWrite);
  }

  /**
   * Terminates the connections after finishing the operation.
   */
  private void teardownConnection() {
    if (globalConnection != null) {
      globalConnection.close();
      globalConnection = null;
    }
  }
}

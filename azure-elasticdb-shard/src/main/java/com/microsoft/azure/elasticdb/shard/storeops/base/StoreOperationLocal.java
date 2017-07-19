package com.microsoft.azure.elasticdb.shard.storeops.base;

/* Copyright (c) Microsoft. All rights reserved.
Licensed under the MIT license. See LICENSE file in the project root for full license information.*/

import com.microsoft.azure.elasticdb.core.commons.transientfaulthandling.RetryPolicy;
import com.microsoft.azure.elasticdb.shard.base.ShardLocation;
import com.microsoft.azure.elasticdb.shard.mapmanager.ShardManagementErrorCategory;
import com.microsoft.azure.elasticdb.shard.mapmanager.ShardManagementException;
import com.microsoft.azure.elasticdb.shard.sqlstore.SqlConnectionStringBuilder;
import com.microsoft.azure.elasticdb.shard.sqlstore.SqlShardMapManagerCredentials;
import com.microsoft.azure.elasticdb.shard.sqlstore.SqlStoreConnection;
import com.microsoft.azure.elasticdb.shard.store.IStoreConnection;
import com.microsoft.azure.elasticdb.shard.store.IStoreTransactionScope;
import com.microsoft.azure.elasticdb.shard.store.StoreConnectionKind;
import com.microsoft.azure.elasticdb.shard.store.StoreException;
import com.microsoft.azure.elasticdb.shard.store.StoreResult;
import com.microsoft.azure.elasticdb.shard.store.StoreResults;
import com.microsoft.azure.elasticdb.shard.store.StoreTransactionScopeKind;
import com.microsoft.azure.elasticdb.shard.utils.ExceptionUtils;

/**
 * Performs a SqlOperation against an LSM.
 */
public abstract class StoreOperationLocal implements IStoreOperationLocal, AutoCloseable {

  /**
   * LSM connection.
   */
  private IStoreConnection localConnection;

  /**
   * Credentials for connection establishment.
   */
  private SqlShardMapManagerCredentials credentials;

  /**
   * Retry policy.
   */
  private RetryPolicy retryPolicy;

  private String operationName;
  /**
   * Location of LSM.
   */
  private ShardLocation location;

  /**
   * Constructs an instance of SqlOperationLocal.
   *
   * @param credentials Credentials for connecting to SMM databases.
   * @param retryPolicy Retry policy for requests.
   * @param location Shard location where the operation is to be performed.
   * @param operationName Operation name.
   */
  public StoreOperationLocal(SqlShardMapManagerCredentials credentials, RetryPolicy retryPolicy,
      ShardLocation location, String operationName) {
    this.credentials = credentials;
    this.retryPolicy = retryPolicy;
    this.setOperationName(operationName);
    this.setLocation(location);
  }

  protected final String getOperationName() {
    return operationName;
  }

  private void setOperationName(String value) {
    operationName = value;
  }

  protected final ShardLocation getLocation() {
    return location;
  }

  private void setLocation(ShardLocation value) {
    location = value;
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
  public final StoreResults doLocal() {
    try {
      return retryPolicy.executeAction(() -> {
        StoreResults r;
        try {
          // Open connection.
          this.establishConnnection();

          try (IStoreTransactionScope ts = this.getTransactionScope()) {
            r = this.doLocalExecute(ts);

            ts.setSuccess(r.getResult() == StoreResult.Success);
          }

          if (r.getResult() != StoreResult.Success) {
            this.handleDoLocalExecuteError(r);
          }

          return r;
        } finally {
          // Close connection.
          this.teardownConnection();
        }
      });
    } catch (StoreException se) {
      throw this.onStoreException(se);
    } catch (Exception e) {
      throw new StoreException(e.getMessage(), e);
    }
  }

  /**
   * Performs actual Dispose of resources.
   */
  public void close() {
    if (localConnection != null) {
      localConnection.close();
      localConnection = null;
    }
  }

  ///#endregion IDisposable

  /**
   * Execute the operation against LSM in the current transaction scope.
   *
   * @param ts Transaction scope.
   * @return Results of the operation.
   */
  public abstract StoreResults doLocalExecute(IStoreTransactionScope ts);

  /**
   * Handles errors from the LSM operation.
   *
   * @param result Operation result.
   */
  public abstract void handleDoLocalExecuteError(StoreResults result);

  /**
   * Returns the ShardManagementException to be thrown corresponding to a StoreException.
   *
   * @param se Store exception that has been raised.
   * @return ShardManagementException to be thrown.
   */
  public ShardManagementException onStoreException(StoreException se) {
    return ExceptionUtils
        .getStoreExceptionLocal(ShardManagementErrorCategory.Recovery, se, this.getOperationName(),
            this.getLocation());
  }

  /**
   * Establishes connection to the target shard.
   */
  private void establishConnnection() {
    // Open connection.
    SqlConnectionStringBuilder localConnectionString = new SqlConnectionStringBuilder(
        credentials.getConnectionStringShard());
    localConnectionString.setDataSource(this.getLocation().getDataSource());
    localConnectionString.setDatabaseName(this.getLocation().getDatabase());

    localConnection = new SqlStoreConnection(StoreConnectionKind.LocalSource,
        localConnectionString.getConnectionString());
  }

  /**
   * Acquires the transaction scope.
   *
   * @return Transaction scope, operations within the scope excute atomically.
   */
  private IStoreTransactionScope getTransactionScope() {
    return localConnection.getTransactionScope(
        this.getReadOnly() ? StoreTransactionScopeKind.ReadOnly
            : StoreTransactionScopeKind.ReadWrite);
  }

  /**
   * Terminates the connections after finishing the operation.
   */
  private void teardownConnection() {
    // Close connection.
    if (localConnection != null) {
      localConnection.close();
    }
  }
}

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
public abstract class StoreOperationLocal implements IStoreOperationLocal {

  /**
   * LSM connection.
   */
  private IStoreConnection _localConnection;

  /**
   * Credentials for connection establishment.
   */
  private SqlShardMapManagerCredentials _credentials;

  /**
   * Retry policy.
   */
  private RetryPolicy _retryPolicy;
  private String OperationName;
  /**
   * Location of LSM.
   */
  private ShardLocation Location;

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
    _credentials = credentials;
    _retryPolicy = retryPolicy;
    this.setOperationName(operationName);
    this.setLocation(location);
  }

  protected final String getOperationName() {
    return OperationName;
  }

  private void setOperationName(String value) {
    OperationName = value;
  }

  protected final ShardLocation getLocation() {
    return Location;
  }

  private void setLocation(ShardLocation value) {
    Location = value;
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
  public final StoreResults Do() {
    try {
      return _retryPolicy.ExecuteAction(() -> {
        StoreResults r;
        try {
          // Open connection.
          this.EstablishConnnection();

          try (IStoreTransactionScope ts = this.GetTransactionScope()) {
            r = this.DoLocalExecute(ts);

            ts.setSuccess(r.getResult() == StoreResult.Success);
          }

          if (r.getResult() != StoreResult.Success) {
            this.HandleDoLocalExecuteError(r);
          }

          return r;
        } finally {
          // Close connection.
          this.TeardownConnection();
        }
      });
    } catch (StoreException se) {
      throw this.OnStoreException(se);
    }
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
    if (_localConnection != null) {
      //TODO: _localConnection.Dispose();
      _localConnection = null;
    }
  }

  ///#endregion IDisposable

  /**
   * Execute the operation against LSM in the current transaction scope.
   *
   * @param ts Transaction scope.
   * @return Results of the operation.
   */
  public abstract StoreResults DoLocalExecute(IStoreTransactionScope ts);

  /**
   * Handles errors from the LSM operation.
   *
   * @param result Operation result.
   */
  public abstract void HandleDoLocalExecuteError(StoreResults result);

  /**
   * Returns the ShardManagementException to be thrown corresponding to a StoreException.
   *
   * @param se Store exception that has been raised.
   * @return ShardManagementException to be thrown.
   */
  public ShardManagementException OnStoreException(StoreException se) {
    return ExceptionUtils
        .GetStoreExceptionLocal(ShardManagementErrorCategory.Recovery, se, this.getOperationName(),
            this.getLocation());
  }

  /**
   * Establishes connection to the target shard.
   */
  private void EstablishConnnection() {
    // Open connection.
    SqlConnectionStringBuilder localConnectionString = new SqlConnectionStringBuilder(
        _credentials.getConnectionStringShard());
    localConnectionString.setDataSource(this.getLocation().getDataSource());
    localConnectionString.setDatabaseName(this.getLocation().getDatabase());

    _localConnection = new SqlStoreConnection(StoreConnectionKind.LocalSource,
        localConnectionString.getConnectionString());
    _localConnection.Open();
  }

  /**
   * Acquires the transaction scope.
   *
   * @return Transaction scope, operations within the scope excute atomically.
   */
  private IStoreTransactionScope GetTransactionScope() {
    return _localConnection.GetTransactionScope(
        this.getReadOnly() ? StoreTransactionScopeKind.ReadOnly
            : StoreTransactionScopeKind.ReadWrite);
  }

  /**
   * Terminates the connections after finishing the operation.
   */
  private void TeardownConnection() {
    // Close connection.
    if (_localConnection != null) {
      _localConnection.close();
    }
  }
}

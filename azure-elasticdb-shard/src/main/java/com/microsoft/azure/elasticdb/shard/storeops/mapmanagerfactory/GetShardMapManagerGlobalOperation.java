package com.microsoft.azure.elasticdb.shard.storeops.mapmanagerfactory;

// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

import com.microsoft.azure.elasticdb.core.commons.transientfaulthandling.RetryPolicy;
import com.microsoft.azure.elasticdb.shard.mapmanager.ShardManagementErrorCategory;
import com.microsoft.azure.elasticdb.shard.mapmanager.ShardManagementErrorCode;
import com.microsoft.azure.elasticdb.shard.mapmanager.ShardManagementException;
import com.microsoft.azure.elasticdb.shard.sqlstore.SqlShardMapManagerCredentials;
import com.microsoft.azure.elasticdb.shard.store.IStoreTransactionScope;
import com.microsoft.azure.elasticdb.shard.store.StoreResult;
import com.microsoft.azure.elasticdb.shard.store.StoreResults;
import com.microsoft.azure.elasticdb.shard.storeops.base.StoreOperationGlobal;
import com.microsoft.azure.elasticdb.shard.utils.Errors;
import com.microsoft.azure.elasticdb.shard.utils.SqlUtils;
import java.io.IOException;
import java.util.List;

/**
 * Obtains the shard map manager object if the GSM has the SMM objects in it.
 */
public class GetShardMapManagerGlobalOperation extends StoreOperationGlobal {

  /**
   * Whether to throw exception on failure.
   */
  private boolean _throwOnFailure;

  /**
   * Constructs request for obtaining shard map manager object if the GSM has the SMM objects in it.
   *
   * @param credentials Credentials for connection.
   * @param retryPolicy Retry policy.
   * @param operationName Operation name, useful for diagnostics.
   * @param throwOnFailure Whether to throw exception on failure or return error code.
   */
  public GetShardMapManagerGlobalOperation(SqlShardMapManagerCredentials credentials,
      RetryPolicy retryPolicy, String operationName, boolean throwOnFailure) {
    super(credentials, retryPolicy, operationName);
    _throwOnFailure = throwOnFailure;
  }

  /**
   * Whether this is a read-only operation.
   */
  @Override
  public boolean getReadOnly() {
    return true;
  }

  /**
   * Execute the operation against GSM in the current transaction scope.
   *
   * @param ts Transaction scope.
   * @return Results of the operation.
   */
  @Override
  public StoreResults DoGlobalExecute(IStoreTransactionScope ts) {
    StoreResults returnedResult = null;
    List<StringBuilder> globalScript = SqlUtils.getCheckIfExistsGlobalScript();
    StringBuilder command = globalScript.get(0);
    StoreResults result = ts.ExecuteCommandSingle(command);

    returnedResult = new StoreResults();

    // If we did not find some store deployed.
    if (result.getStoreVersion() == null) {
      returnedResult.setResult(StoreResult.Failure);
    } else {
      // DevNote: We need to have a way to error out if versions do not match.
      // we can potentially call upgrade here to get to latest version.
      // Should this be exposed as a new parameter ?
      returnedResult.setResult(StoreResult.Success);
    }

    return returnedResult;
  }

  /**
   * Handles errors from the GSM operation after the LSM operations.
   *
   * @param result Operation result.
   */
  @Override
  public void HandleDoGlobalExecuteError(StoreResults result) {
    if (_throwOnFailure) {
      throw new ShardManagementException(ShardManagementErrorCategory.ShardMapManagerFactory,
          ShardManagementErrorCode.ShardMapManagerStoreDoesNotExist,
          Errors._Store_ShardMapManager_DoesNotExistGlobal);
    }
  }

  /**
   * Error category for store exception.
   */
  @Override
  protected ShardManagementErrorCategory getErrorCategory() {
    return ShardManagementErrorCategory.ShardMapManagerFactory;
  }

  @Override
  public void close() throws IOException {

  }
}

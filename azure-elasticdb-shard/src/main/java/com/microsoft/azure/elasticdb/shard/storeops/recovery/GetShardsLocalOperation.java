package com.microsoft.azure.elasticdb.shard.storeops.recovery;

/* Copyright (c) Microsoft. All rights reserved.
Licensed under the MIT license. See LICENSE file in the project root for full license information.*/

import com.microsoft.azure.elasticdb.shard.base.ShardLocation;
import com.microsoft.azure.elasticdb.shard.mapmanager.ShardManagementErrorCategory;
import com.microsoft.azure.elasticdb.shard.mapmanager.ShardManagementErrorCode;
import com.microsoft.azure.elasticdb.shard.mapmanager.ShardManagementException;
import com.microsoft.azure.elasticdb.shard.mapmanager.ShardMapManager;
import com.microsoft.azure.elasticdb.shard.store.IStoreTransactionScope;
import com.microsoft.azure.elasticdb.shard.store.StoreResults;
import com.microsoft.azure.elasticdb.shard.storeops.base.StoreOperationErrorHandler;
import com.microsoft.azure.elasticdb.shard.storeops.base.StoreOperationLocal;
import com.microsoft.azure.elasticdb.shard.storeops.base.StoreOperationRequestBuilder;
import com.microsoft.azure.elasticdb.shard.utils.Errors;
import com.microsoft.azure.elasticdb.shard.utils.SqlUtils;

/**
 * Obtains all the shard maps and shards from an LSM.
 */
public class GetShardsLocalOperation extends StoreOperationLocal {

  /**
   * Constructs request for obtaining all the shard maps and shards from an LSM.
   *
   * @param shardMapManager Shard map manager.
   * @param location Location of the LSM.
   * @param operationName Operation name.
   */
  public GetShardsLocalOperation(ShardMapManager shardMapManager, ShardLocation location,
      String operationName) {
    super(shardMapManager.getCredentials(), shardMapManager.getRetryPolicy(), location,
        operationName);
  }

  /**
   * Whether this is a read-only operation.
   */
  @Override
  public boolean getReadOnly() {
    return true;
  }

  /**
   * Execute the operation against LSM in the current transaction scope.
   *
   * @param ts Transaction scope.
   * @return Results of the operation.
   */
  @Override
  public StoreResults doLocalExecute(IStoreTransactionScope ts) {
    StoreResults result = ts.executeCommandSingle(SqlUtils.getCheckIfExistsLocalScript().get(0));

    if (result.getStoreVersion() == null) {
      // Shard not deployed, which is an error condition.
      throw new ShardManagementException(ShardManagementErrorCategory.Recovery,
          ShardManagementErrorCode.ShardNotValid, Errors._Recovery_ShardNotValid,
          this.getLocation(), this.getOperationName());
    }

    return ts.executeOperation(StoreOperationRequestBuilder.SP_GET_ALL_SHARDS_LOCAL,
        StoreOperationRequestBuilder.getAllShardsLocal());
  }

  /**
   * Handles errors from the LSM operation.
   *
   * @param result Operation result.
   */
  @Override
  public void handleDoLocalExecuteError(StoreResults result) {
    // Possible errors are:
    // StoreResult.StoreVersionMismatch
    // StoreResult.MissingParametersForStoredProcedure
    throw StoreOperationErrorHandler.onRecoveryErrorLocal(result, null, this.getLocation(),
        ShardManagementErrorCategory.Recovery, this.getOperationName(),
        StoreOperationRequestBuilder.SP_GET_ALL_SHARDS_LOCAL);
  }
}
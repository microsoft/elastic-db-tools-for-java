package com.microsoft.azure.elasticdb.shard.storeops.schemainformation;

/* Copyright (c) Microsoft. All rights reserved.
Licensed under the MIT license. See LICENSE file in the project root for full license information.*/

import com.microsoft.azure.elasticdb.shard.mapmanager.ShardManagementErrorCategory;
import com.microsoft.azure.elasticdb.shard.mapmanager.ShardMapManager;
import com.microsoft.azure.elasticdb.shard.store.IStoreTransactionScope;
import com.microsoft.azure.elasticdb.shard.store.StoreResults;
import com.microsoft.azure.elasticdb.shard.store.StoreSchemaInfo;
import com.microsoft.azure.elasticdb.shard.storeops.base.StoreOperationErrorHandler;
import com.microsoft.azure.elasticdb.shard.storeops.base.StoreOperationGlobal;
import com.microsoft.azure.elasticdb.shard.storeops.base.StoreOperationRequestBuilder;

/**
 * Add schema info to GSM.
 */
public class AddShardingSchemaInfoGlobalOperation extends StoreOperationGlobal {

  /**
   * Schema info to add.
   */
  private StoreSchemaInfo schemaInfo;

  /**
   * Constructs a request to add schema info to GSM.
   *
   * @param shardMapManager Shard map manager object.
   * @param operationName Operation name, useful for diagnostics.
   * @param schemaInfo Schema info to add.
   */
  public AddShardingSchemaInfoGlobalOperation(ShardMapManager shardMapManager, String operationName,
      StoreSchemaInfo schemaInfo) {
    super(shardMapManager.getCredentials(), shardMapManager.getRetryPolicy(), operationName);
    this.schemaInfo = schemaInfo;
  }

  /**
   * Whether this is a read-only operation.
   */
  @Override
  public boolean getReadOnly() {
    return false;
  }

  /**
   * Execute the operation against GSM in the current transaction scope.
   *
   * @param ts Transaction scope.
   * @return Results of the operation.
   */
  @Override
  public StoreResults doGlobalExecute(IStoreTransactionScope ts) {
    return ts.executeOperation(StoreOperationRequestBuilder.SP_ADD_SHARDING_SCHEMA_INFO_GLOBAL,
        StoreOperationRequestBuilder.addShardingSchemaInfoGlobal(schemaInfo));
  }

  /**
   * Handles errors from the GSM operation after the LSM operations.
   *
   * @param result Operation result.
   */
  @Override
  public void handleDoGlobalExecuteError(StoreResults result) {
    // Expected errors are:
    // StoreResult.SchemaInfoNameConflict:
    // StoreResult.MissingParametersForStoredProcedure:
    // StoreResult.StoreVersionMismatch:
    throw StoreOperationErrorHandler.onShardSchemaInfoErrorGlobal(result, schemaInfo.getName(),
        this.getOperationName(), StoreOperationRequestBuilder.SP_ADD_SHARDING_SCHEMA_INFO_GLOBAL);
  }

  /**
   * Error category for store exception.
   */
  @Override
  protected ShardManagementErrorCategory getErrorCategory() {
    return ShardManagementErrorCategory.SchemaInfoCollection;
  }
}
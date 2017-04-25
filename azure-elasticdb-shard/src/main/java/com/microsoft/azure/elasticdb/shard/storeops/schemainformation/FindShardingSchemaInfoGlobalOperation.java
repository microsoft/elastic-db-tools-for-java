package com.microsoft.azure.elasticdb.shard.storeops.schemainformation;

// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

import com.microsoft.azure.elasticdb.shard.mapmanager.ShardManagementErrorCategory;
import com.microsoft.azure.elasticdb.shard.mapmanager.ShardMapManager;
import com.microsoft.azure.elasticdb.shard.store.IStoreTransactionScope;
import com.microsoft.azure.elasticdb.shard.store.StoreResult;
import com.microsoft.azure.elasticdb.shard.store.StoreResults;
import com.microsoft.azure.elasticdb.shard.storeops.base.StoreOperationErrorHandler;
import com.microsoft.azure.elasticdb.shard.storeops.base.StoreOperationGlobal;
import com.microsoft.azure.elasticdb.shard.storeops.base.StoreOperationRequestBuilder;
import java.io.IOException;

/**
 * Finds schema info for given name in GSM.
 */
public class FindShardingSchemaInfoGlobalOperation extends StoreOperationGlobal {

  /**
   * Shard map name for given schema info.
   */
  private String _schemaInfoName;

  /**
   * Constructs a request to find schema info in GSM.
   *
   * @param shardMapManager Shard map manager object.
   * @param operationName Operation name, useful for diagnostics.
   * @param schemaInfoName Name of schema info to search.
   */
  public FindShardingSchemaInfoGlobalOperation(ShardMapManager shardMapManager,
      String operationName, String schemaInfoName) {
    super(shardMapManager.getCredentials(), shardMapManager.getRetryPolicy(), operationName);
    _schemaInfoName = schemaInfoName;
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
    return ts.ExecuteOperation(StoreOperationRequestBuilder.SpFindShardingSchemaInfoByNameGlobal,
        StoreOperationRequestBuilder.FindShardingSchemaInfoGlobal(_schemaInfoName));
  }

  /**
   * Handles errors from the GSM operation after the LSM operations.
   *
   * @param result Operation result.
   */
  @Override
  public void HandleDoGlobalExecuteError(StoreResults result) {
    // SchemaInfoNameDoesNotExist is handled by the callers i.e. Get vs TryGet.
    if (result.getResult() != StoreResult.SchemaInfoNameDoesNotExist) {
      // Expected errors are:
      // StoreResult.MissingParametersForStoredProcedure:
      // StoreResult.StoreVersionMismatch:
      throw StoreOperationErrorHandler
          .OnShardSchemaInfoErrorGlobal(result, _schemaInfoName, this.getOperationName(),
              StoreOperationRequestBuilder.SpFindShardingSchemaInfoByNameGlobal);
    }
  }

  /**
   * Error category for store exception.
   */
  @Override
  protected ShardManagementErrorCategory getErrorCategory() {
    return ShardManagementErrorCategory.SchemaInfoCollection;
  }

  @Override
  public void close() throws IOException {

  }
}
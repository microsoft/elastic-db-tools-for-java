package com.microsoft.azure.elasticdb.shard.storeops.base;

/* Copyright (c) Microsoft. All rights reserved.
Licensed under the MIT license. See LICENSE file in the project root for full license information.*/

import com.microsoft.azure.elasticdb.shard.mapmanager.ShardManagementException;
import com.microsoft.azure.elasticdb.shard.store.IStoreTransactionScope;
import com.microsoft.azure.elasticdb.shard.store.StoreException;
import com.microsoft.azure.elasticdb.shard.store.StoreResults;

/**
 * Represents an LSM only store operation.
 */
public interface IStoreOperationLocal extends java.io.Closeable {

  /**
   * Whether this is a read-only operation.
   */
  boolean getReadOnly();

  /**
   * Performs the store operation.
   *
   * @return Results of the operation.
   */
  StoreResults doLocal();

  /**
   * Execute the operation against LSM in the current transaction scope.
   *
   * @param ts Transaction scope.
   * @return Results of the operation.
   */
  StoreResults doLocalExecute(IStoreTransactionScope ts);

  /**
   * Handles errors from the LSM operation.
   *
   * @param result Operation result.
   */
  void handleDoLocalExecuteError(StoreResults result);

  /**
   * Returns the ShardManagementException to be thrown corresponding to a StoreException.
   *
   * @param se Store exception that has been raised.
   * @return ShardManagementException to be thrown.
   */
  ShardManagementException onStoreException(StoreException se);
}
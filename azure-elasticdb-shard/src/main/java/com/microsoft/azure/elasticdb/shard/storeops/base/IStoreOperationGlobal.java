package com.microsoft.azure.elasticdb.shard.storeops.base;

// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

import com.microsoft.azure.elasticdb.shard.mapmanager.ShardManagementException;
import com.microsoft.azure.elasticdb.shard.store.StoreResults;
import com.microsoft.azure.elasticdb.shard.store.IStoreTransactionScope;
import com.microsoft.azure.elasticdb.shard.store.StoreException;

import java.util.concurrent.Callable;

/**
 * Represents a GSM only store operation.
 */
public interface IStoreOperationGlobal extends AutoCloseable {
    /**
     * Whether this is a read-only operation.
     */
    boolean getReadOnly();

    /**
     * Performs the store operation.
     *
     * @return Results of the operation.
     */
    StoreResults Do();

    /**
     * Asynchronously performs the store operation.
     *
     * @return Task encapsulating results of the operation.
     */
    Callable<StoreResults> DoAsync();

    /**
     * Execute the operation against GSM in the current transaction scope.
     *
     * @param ts Transaction scope.
     * @return Results of the operation.
     */
    StoreResults DoGlobalExecute(IStoreTransactionScope ts);

    /**
     * Asynchronously execute the operation against GSM in the current transaction scope.
     *
     * @param ts Transaction scope.
     * @return Task encapsulating results of the operation.
     */
    Callable<StoreResults> DoGlobalExecuteAsync(IStoreTransactionScope ts);

    /**
     * Invalidates the cache on unsuccessful commit of the GSM operation.
     *
     * @param result Operation result.
     */
    void DoGlobalUpdateCachePre(StoreResults result);

    /**
     * Handles errors from the GSM operation after the LSM operations.
     *
     * @param result Operation result.
     */
    void HandleDoGlobalExecuteError(StoreResults result);

    /**
     * Refreshes the cache on successful commit of the GSM operation.
     *
     * @param result Operation result.
     */
    void DoGlobalUpdateCachePost(StoreResults result);

    /**
     * Returns the ShardManagementException to be thrown corresponding to a StoreException.
     *
     * @param se Store exception that has been raised.
     * @return ShardManagementException to be thrown.
     */
    ShardManagementException OnStoreException(StoreException se);
}
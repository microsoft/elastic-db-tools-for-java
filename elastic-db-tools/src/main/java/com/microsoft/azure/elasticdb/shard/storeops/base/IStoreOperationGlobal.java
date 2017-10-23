package com.microsoft.azure.elasticdb.shard.storeops.base;

import java.util.concurrent.Callable;

/*
 * Elastic database tools for Azure SQL Database.
 * 
 * Copyright(c) Microsoft Corporation All rights reserved.
 * 
 * This program is made available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

import com.microsoft.azure.elasticdb.shard.mapmanager.ShardManagementException;
import com.microsoft.azure.elasticdb.shard.store.IStoreTransactionScope;
import com.microsoft.azure.elasticdb.shard.store.StoreException;
import com.microsoft.azure.elasticdb.shard.store.StoreResults;

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
    StoreResults doGlobal();

    /**
     * Asynchronously performs the store operation.
     *
     * @return Task encapsulating results of the operation.
     */
    Callable<StoreResults> doAsync();

    /**
     * Execute the operation against GSM in the current transaction scope.
     *
     * @param ts
     *            Transaction scope.
     * @return Results of the operation.
     */
    StoreResults doGlobalExecute(IStoreTransactionScope ts);

    /**
     * Asynchronously execute the operation against GSM in the current transaction scope.
     *
     * @param ts
     *            Transaction scope.
     * @return Task encapsulating results of the operation.
     */
    Callable<StoreResults> doGlobalExecuteAsync(IStoreTransactionScope ts);

    /**
     * Invalidates the cache on unsuccessful commit of the GSM operation.
     *
     * @param result
     *            Operation result.
     */
    void doGlobalUpdateCachePre(StoreResults result);

    /**
     * Handles errors from the GSM operation after the LSM operations.
     *
     * @param result
     *            Operation result.
     */
    void handleDoGlobalExecuteError(StoreResults result);

    /**
     * Refreshes the cache on successful commit of the GSM operation.
     *
     * @param result
     *            Operation result.
     */
    void doGlobalUpdateCachePost(StoreResults result);

    /**
     * Returns the ShardManagementException to be thrown corresponding to a StoreException.
     *
     * @param se
     *            Store exception that has been raised.
     * @return ShardManagementException to be thrown.
     */
    ShardManagementException onStoreException(StoreException se);
}
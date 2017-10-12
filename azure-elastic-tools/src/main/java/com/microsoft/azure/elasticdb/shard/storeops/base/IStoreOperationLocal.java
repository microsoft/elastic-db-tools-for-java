package com.microsoft.azure.elasticdb.shard.storeops.base;

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
     * @param ts
     *            Transaction scope.
     * @return Results of the operation.
     */
    StoreResults doLocalExecute(IStoreTransactionScope ts);

    /**
     * Handles errors from the LSM operation.
     *
     * @param result
     *            Operation result.
     */
    void handleDoLocalExecuteError(StoreResults result);

    /**
     * Returns the ShardManagementException to be thrown corresponding to a StoreException.
     *
     * @param se
     *            Store exception that has been raised.
     * @return ShardManagementException to be thrown.
     */
    ShardManagementException onStoreException(StoreException se);
}
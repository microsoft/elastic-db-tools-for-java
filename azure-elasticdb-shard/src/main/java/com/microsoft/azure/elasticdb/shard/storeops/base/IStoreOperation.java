package com.microsoft.azure.elasticdb.shard.storeops.base;

// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

import com.microsoft.azure.elasticdb.shard.mapmanager.ShardManagementException;
import com.microsoft.azure.elasticdb.shard.store.IStoreResults;
import com.microsoft.azure.elasticdb.shard.store.IStoreTransactionScope;
import com.microsoft.azure.elasticdb.shard.store.StoreException;

/**
 * Distributed Store operation.
 */
public interface IStoreOperation extends java.io.Closeable {
    /**
     * Performs the store operation.
     *
     * @return Results of the operation.
     */
    IStoreResults Do();

    /**
     * Performs the undo store operation.
     */
    void Undo();

    /**
     * Requests the derived class to provide information regarding the connections
     * needed for the operation.
     *
     * @return Information about shards involved in the operation.
     */
    StoreConnectionInfo GetStoreConnectionInfo();

    /**
     * Performs the initial GSM operation prior to LSM operations.
     *
     * @param ts Transaction scope.
     * @return Pending operations on the target objects if any.
     */
    IStoreResults DoGlobalPreLocalExecute(IStoreTransactionScope ts);

    /**
     * Handles errors from the initial GSM operation prior to LSM operations.
     *
     * @param result Operation result.
     */
    void HandleDoGlobalPreLocalExecuteError(IStoreResults result);

    /**
     * Performs the LSM operation on the source shard.
     *
     * @param ts Transaction scope.
     * @return Result of the operation.
     */
    IStoreResults DoLocalSourceExecute(IStoreTransactionScope ts);

    /**
     * Handles errors from the the LSM operation on the source shard.
     *
     * @param result Operation result.
     */
    void HandleDoLocalSourceExecuteError(IStoreResults result);

    /**
     * Performs the LSM operation on the target shard.
     *
     * @param ts Transaction scope.
     * @return Result of the operation.
     */
    IStoreResults DoLocalTargetExecute(IStoreTransactionScope ts);

    /**
     * Performs the final GSM operation after the LSM operations.
     *
     * @param result Operation result.
     */
    void HandleDoLocalTargetExecuteError(IStoreResults result);

    /**
     * Performs the final GSM operation after the LSM operations.
     *
     * @param ts Transaction scope.
     * @return Pending operations on the target objects if any.
     */
    IStoreResults DoGlobalPostLocalExecute(IStoreTransactionScope ts);

    /**
     * Handles errors from the final GSM operation after the LSM operations.
     *
     * @param result Operation result.
     */
    void HandleDoGlobalPostLocalExecuteError(IStoreResults result);

    /**
     * Refreshes the cache on successful commit of the final GSM operation after the LSM operations.
     *
     * @param result Operation result.
     */
    void DoGlobalPostLocalUpdateCache(IStoreResults result);

    /**
     * Performs the undo of LSM operation on the source shard.
     *
     * @param ts Transaction scope.
     * @return Result of the operation.
     */
    IStoreResults UndoLocalSourceExecute(IStoreTransactionScope ts);

    /**
     * Handles errors from the undo of LSM operation on the source shard.
     *
     * @param result Operation result.
     */
    void HandleUndoLocalSourceExecuteError(IStoreResults result);

    /**
     * Performs the undo of GSM operation after LSM operations.
     *
     * @param ts Transaction scope.
     * @return Pending operations on the target objects if any.
     */
    IStoreResults UndoGlobalPostLocalExecute(IStoreTransactionScope ts);

    /**
     * Handles errors from the undo of GSM operation after LSM operations.
     *
     * @param result Operation result.
     */
    void HandleUndoGlobalPostLocalExecuteError(IStoreResults result);

    /**
     * Returns the ShardManagementException to be thrown corresponding to a StoreException.
     *
     * @param se    Store Exception that has been raised.
     * @param state SQL operation state.
     * @return ShardManagementException to be thrown.
     */
    ShardManagementException OnStoreException(StoreException se, StoreOperationState state);
}
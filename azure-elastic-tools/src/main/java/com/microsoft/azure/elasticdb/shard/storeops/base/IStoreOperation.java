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
 * Distributed Store operation.
 */
public interface IStoreOperation extends AutoCloseable {

    /**
     * Performs the store operation.
     *
     * @return Results of the operation.
     */
    StoreResults doOperation();

    /**
     * Performs the undo store operation.
     */
    void undoOperation();

    /**
     * Requests the derived class to provide information regarding the connections needed for the operation.
     *
     * @return Information about shards involved in the operation.
     */
    StoreConnectionInfo getStoreConnectionInfo();

    /**
     * Performs the initial GSM operation prior to LSM operations.
     *
     * @param ts
     *            Transaction scope.
     * @return Pending operations on the target objects if any.
     */
    StoreResults doGlobalPreLocalExecute(IStoreTransactionScope ts);

    /**
     * Handles errors from the initial GSM operation prior to LSM operations.
     *
     * @param result
     *            Operation result.
     */
    void handleDoGlobalPreLocalExecuteError(StoreResults result);

    /**
     * Performs the LSM operation on the source shard.
     *
     * @param ts
     *            Transaction scope.
     * @return Result of the operation.
     */
    StoreResults doLocalSourceExecute(IStoreTransactionScope ts);

    /**
     * Handles errors from the the LSM operation on the source shard.
     *
     * @param result
     *            Operation result.
     */
    void handleDoLocalSourceExecuteError(StoreResults result);

    /**
     * Performs the LSM operation on the target shard.
     *
     * @param ts
     *            Transaction scope.
     * @return Result of the operation.
     */
    StoreResults doLocalTargetExecute(IStoreTransactionScope ts);

    /**
     * Performs the final GSM operation after the LSM operations.
     *
     * @param result
     *            Operation result.
     */
    void handleDoLocalTargetExecuteError(StoreResults result);

    /**
     * Performs the final GSM operation after the LSM operations.
     *
     * @param ts
     *            Transaction scope.
     * @return Pending operations on the target objects if any.
     */
    StoreResults doGlobalPostLocalExecute(IStoreTransactionScope ts);

    /**
     * Handles errors from the final GSM operation after the LSM operations.
     *
     * @param result
     *            Operation result.
     */
    void handleDoGlobalPostLocalExecuteError(StoreResults result);

    /**
     * Refreshes the cache on successful commit of the final GSM operation after the LSM operations.
     *
     * @param result
     *            Operation result.
     */
    void doGlobalPostLocalUpdateCache(StoreResults result);

    /**
     * Performs the undo of LSM operation on the source shard.
     *
     * @param ts
     *            Transaction scope.
     * @return Result of the operation.
     */
    StoreResults undoLocalSourceExecute(IStoreTransactionScope ts);

    /**
     * Handles errors from the undo of LSM operation on the source shard.
     *
     * @param result
     *            Operation result.
     */
    void handleUndoLocalSourceExecuteError(StoreResults result);

    /**
     * Performs the undo of GSM operation after LSM operations.
     *
     * @param ts
     *            Transaction scope.
     * @return Pending operations on the target objects if any.
     */
    StoreResults undoGlobalPostLocalExecute(IStoreTransactionScope ts);

    /**
     * Handles errors from the undo of GSM operation after LSM operations.
     *
     * @param result
     *            Operation result.
     */
    void handleUndoGlobalPostLocalExecuteError(StoreResults result);

    /**
     * Returns the ShardManagementException to be thrown corresponding to a StoreException.
     *
     * @param se
     *            Store Exception that has been raised.
     * @param state
     *            SQL operation state.
     * @return ShardManagementException to be thrown.
     */
    ShardManagementException onStoreException(StoreException se,
            StoreOperationState state);
}
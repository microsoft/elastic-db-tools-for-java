package com.microsoft.azure.elasticdb.shard.store;

/*
 * Elastic database tools for Azure SQL Database.
 * 
 * Copyright(c) Microsoft Corporation All rights reserved.
 * 
 * This program is made available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

import java.util.UUID;

/**
 * Instance of a store connection.
 */
public interface IStoreConnection extends AutoCloseable {

    /**
     * Type of store connection.
     */
    StoreConnectionKind getKind();

    /**
     * Open the store connection, and acquire a lock on the store.
     *
     * @param lockId
     *            Lock Id.
     */
    void openWithLock(UUID lockId);

    /**
     * Closes the store connection.
     */
    void close();

    /**
     * Closes the store connection after releasing lock.
     *
     * @param lockId
     *            Lock Id.
     */
    void closeWithUnlock(UUID lockId);

    /**
     * Acquires a transactional scope on the connection.
     *
     * @param kind
     *            Type of transaction scope.
     * @return Transaction scope on the store connection.
     */
    IStoreTransactionScope getTransactionScope(StoreTransactionScopeKind kind);
}
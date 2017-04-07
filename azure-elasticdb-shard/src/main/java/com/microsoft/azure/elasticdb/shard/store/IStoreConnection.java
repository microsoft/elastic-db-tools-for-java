package com.microsoft.azure.elasticdb.shard.store;

// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

import java.util.UUID;
import java.util.concurrent.Callable;

/**
 * Instance of a store connection.
 */
public interface IStoreConnection extends java.io.Closeable {
    /**
     * Type of store connection.
     */
    StoreConnectionKind getKind();

    /**
     * Open the store connection.
     */
    void Open();

    /**
     * Asynchronously opens the store connection.
     *
     * @return Task to await completion of the Open
     */
    Callable OpenAsync();

    /**
     * Open the store connection, and acquire a lock on the store.
     *
     * @param lockId Lock Id.
     */
    void OpenWithLock(UUID lockId);

    /**
     * Closes the store connection.
     */
    void Close();

    /**
     * Closes the store connection after releasing lock.
     *
     * @param lockId Lock Id.
     */
    void CloseWithUnlock(UUID lockId);

    /**
     * Acquires a transactional scope on the connection.
     *
     * @param kind Type of transaction scope.
     * @return Transaction scope on the store connection.
     */
    IStoreTransactionScope GetTransactionScope(StoreTransactionScopeKind kind);
}
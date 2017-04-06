package com.microsoft.azure.elasticdb.shard.store;

// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

import java.sql.Connection;
import java.util.concurrent.Callable;

/**
 * Instance of a user connection to store.
 */
public interface IUserStoreConnection extends java.io.Closeable {
    /**
     * Underlying SQL server connection.
     */
    Connection getConnection();

    /**
     * Opens the connection.
     */
    void Open();

    /**
     * Asynchronously opens the connection.
     *
     * @return Task to await completion of the Open
     */
    Callable OpenAsync();
}
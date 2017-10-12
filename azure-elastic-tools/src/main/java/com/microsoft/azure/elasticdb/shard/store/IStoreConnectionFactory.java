package com.microsoft.azure.elasticdb.shard.store;

/*
 * Elastic database tools for Azure SQL Database.
 * 
 * Copyright(c) Microsoft Corporation All rights reserved.
 * 
 * This program is made available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

/**
 * Factory for store connections.
 */
public interface IStoreConnectionFactory {

    /**
     * Constructs a new instance of store connection.
     *
     * @param kind
     *            Type of store connection.
     * @param connectionString
     *            Connection string for store.
     * @return An unopened instance of the store connection.
     */
    IStoreConnection getConnection(StoreConnectionKind kind,
            String connectionString);

    /**
     * Constructs a new instance of user connection.
     *
     * @param connectionString
     *            Connection string of user.
     * @return An unopened instance of the user connection.
     */
    IUserStoreConnection getUserConnection(String connectionString);
}
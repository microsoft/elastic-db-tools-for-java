package com.microsoft.azure.elasticdb.shard.sqlstore;

/*
 * Elastic database tools for Azure SQL Database.
 * 
 * Copyright(c) Microsoft Corporation All rights reserved.
 * 
 * This program is made available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

import com.microsoft.azure.elasticdb.shard.store.IStoreConnection;
import com.microsoft.azure.elasticdb.shard.store.IStoreConnectionFactory;
import com.microsoft.azure.elasticdb.shard.store.IUserStoreConnection;
import com.microsoft.azure.elasticdb.shard.store.StoreConnectionKind;

/**
 * Constructs instance of Sql Store Connection.
 */
public class SqlStoreConnectionFactory implements IStoreConnectionFactory {

    /**
     * Constructs an instance of the factory.
     */
    public SqlStoreConnectionFactory() {
    }

    /**
     * Constructs a new instance of store connection.
     *
     * @param kind
     *            Type of store connection.
     * @param connectionString
     *            Connection string for store.
     * @return An unopened instance of the store connection.
     */
    public IStoreConnection getConnection(StoreConnectionKind kind,
            String connectionString) {
        return new SqlStoreConnection(kind, connectionString);
    }

    /**
     * Constructs a new instance of user connection.
     *
     * @param connectionString
     *            Connection string of user.
     * @return An unopened instance of the user connection.
     */
    public IUserStoreConnection getUserConnection(String connectionString) {
        return new SqlUserStoreConnection(connectionString);
    }
}
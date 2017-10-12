package com.microsoft.azure.elasticdb.shard.store;

/*
 * Elastic database tools for Azure SQL Database.
 * 
 * Copyright(c) Microsoft Corporation All rights reserved.
 * 
 * This program is made available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

import java.sql.Connection;

/**
 * Instance of a user connection to store.
 */
public interface IUserStoreConnection extends java.io.Closeable {

    /**
     * Underlying SQL server connection.
     */
    Connection getConnection();

}
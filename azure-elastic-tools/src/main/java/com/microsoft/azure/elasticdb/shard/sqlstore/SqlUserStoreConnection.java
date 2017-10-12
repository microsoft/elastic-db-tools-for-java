package com.microsoft.azure.elasticdb.shard.sqlstore;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

/*
 * Elastic database tools for Azure SQL Database.
 * 
 * Copyright(c) Microsoft Corporation All rights reserved.
 * 
 * This program is made available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

import com.microsoft.azure.elasticdb.shard.store.IUserStoreConnection;
import com.microsoft.azure.elasticdb.shard.store.StoreException;

/**
 * Instance of a User Sql Store Connection.
 */
public class SqlUserStoreConnection implements IUserStoreConnection {

    /**
     * Underlying connection.
     */
    private Connection conn;

    /**
     * Creates a new instance of user store connection.
     *
     * @param connectionString
     *            Connection string.
     */
    public SqlUserStoreConnection(String connectionString) {
        try {
            conn = DriverManager.getConnection(connectionString);
        }
        catch (SQLException e) {
            e.printStackTrace();
            throw new StoreException(e.getMessage(), e);
        }
    }

    /**
     * Underlying SQL server connection.
     */
    public final Connection getConnection() {
        return conn;
    }

    @Override
    public void close() {
        try {
            if (!conn.isClosed()) {
                conn.close();
            }
        }
        catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
}
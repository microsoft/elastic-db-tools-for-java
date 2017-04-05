package com.microsoft.azure.elasticdb.samples.elasticscalestarterkit;

// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

import com.microsoft.azure.elasticdb.shard.sqlstore.SqlConnectionStringBuilder;

/**
 * Provides access to app.config settings, and contains advanced configuration settings.
 */
public final class Configuration {
    /**
     * Gets the server name for the Shard Map shardMapManager database, which contains the shard maps.
     */
    public static String getShardMapManagerServerName() {
        return getServerName();
    }

    /**
     * Gets the database name for the Shard Map shardMapManager database, which contains the shard maps.
     */
    public static String getShardMapManagerDatabaseName() {
        return "ElasticScaleStarterKit_ShardMapManagerDb";
    }

    /**
     * Gets the name for the Shard Map that contains metadata for all the shards and the mappings to those shards.
     */
    public static String getShardMapName() {
        return "CustomerIDShardMap";
    }

    /**
     * Gets the server name from the App.config file for shards to be created on.
     */
    private static String getServerName() {
        return "aehob8ow4j.database.windows.net";
    }

    /**
     * Gets the edition to use for Shards and Shard Map shardMapManager Database if the server is an Azure SQL DB server.
     * If the server is a regular SQL Server then this is ignored.
     */
    public static String getDatabaseEdition() {
        return "Basic";
    }

    /**
     * Returns a connection string that can be used to connect to the specified server and database.
     */
    public static String GetConnectionString(String serverName, String database) {
        SqlConnectionStringBuilder connStr = new SqlConnectionStringBuilder(GetCredentialsConnectionString());
        connStr.setDataSource(serverName);
        connStr.setInitialCatalog(database);
        return connStr.toString();
    }

    /**
     * Returns a connection string to use for Data-Dependent Routing and Multi-Shard Query,
     * which does not contain DataSource or InitialCatalog.
     */
    public static String GetCredentialsConnectionString() {
        // Get User name and password from the app.config file. If they don't exist, default to string.Empty.
        String userId = "prabhu";
        String password = "3YX8EpPKHnQs";

        // Get Integrated Security from the app.config file.
        // If it exists, then parse it (throw exception on failure), otherwise default to false.
        String integratedSecurityString = "false";
        boolean integratedSecurity = integratedSecurityString != null && Boolean.parseBoolean(integratedSecurityString);

        SqlConnectionStringBuilder connStr = new SqlConnectionStringBuilder();
        connStr.setUser(userId);
        connStr.setPassword(password);
        connStr.setIntegratedSecurity(integratedSecurity);
        connStr.setApplicationName("ESC_SKv1.0");
        connStr.setConnectTimeout(30);
        return connStr.toString();
    }
}
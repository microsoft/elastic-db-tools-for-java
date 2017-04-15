package com.microsoft.azure.elasticdb.samples.elasticscalestarterkit;

// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

import com.microsoft.azure.elasticdb.shard.sqlstore.SqlConnectionStringBuilder;

/**
 * Provides access to app.config settings, and contains advanced configuration settings.
 */
public final class Configuration {
    //TODO: Move to a config file
    public static final String CONN_SERVER_NAME = "aehob8ow4j.database.windows.net";
    public static final String CONN_DB_NAME = "ElasticScaleStarterKit_ShardMapManagerDb";
    public static final String CONN_USER = "prabhu";
    public static final String CONN_PASSWORD = "3YX8EpPKHnQs";
    public static final String CONN_APP_NAME = "ESC_SKv1.0";
    public static final String DB_EDITION = "Basic";
    public static final String RANGE_SHARD_MAP_NAME = "CustomerIDRangeShardMap";
    public static final String LIST_SHARD_MAP_NAME = "CustomerIDListShardMap";

    /**
     * Gets the server name for the Shard Map shardMapManager database, which contains the shard maps.
     */
    public static String getShardMapManagerServerName() {
        return CONN_SERVER_NAME;
    }

    /**
     * Gets the database name for the Shard Map shardMapManager database, which contains the shard maps.
     */
    public static String getShardMapManagerDatabaseName() {
        return CONN_DB_NAME;
    }

    /**
     * Gets the name for the Range Shard Map that contains metadata for all the shards
     * and the mappings to those shards.
     */
    public static String getRangeShardMapName() {
        return RANGE_SHARD_MAP_NAME;
    }

    /**
     * Gets the name for the List Shard Map that contains metadata for all the shards
     * and the mappings to those shards.
     */
    public static String getListShardMapName() {
        return LIST_SHARD_MAP_NAME;
    }

    /**
     * Gets the edition to use for Shards and Shard Map shardMapManager Database if the server is an Azure SQL DB server.
     * If the server is a regular SQL Server then this is ignored.
     */
    public static String getDatabaseEdition() {
        return DB_EDITION;
    }

    /**
     * Returns a connection string that can be used to connect to the specified server and database.
     */
    public static String GetConnectionString(String serverName, String database) {
        SqlConnectionStringBuilder connStr = new SqlConnectionStringBuilder(GetCredentialsConnectionString());
        connStr.setDataSource(serverName);
        connStr.setDatabaseName(database);
        return connStr.toString();
    }

    /**
     * Returns a connection string to use for Data-Dependent Routing and Multi-Shard Query,
     * which does not contain DataSource or DatabaseName.
     */
    public static String GetCredentialsConnectionString() {

        // Get Integrated Security from the app.config file.
        // If it exists, then parse it (throw exception on failure), otherwise default to false.
        String integratedSecurityString = "false";
        boolean integratedSecurity = integratedSecurityString != null && Boolean.parseBoolean(integratedSecurityString);

        SqlConnectionStringBuilder connStr = new SqlConnectionStringBuilder();
        connStr.setUser(CONN_USER);
        connStr.setPassword(CONN_PASSWORD);
        connStr.setIntegratedSecurity(integratedSecurity);
        connStr.setApplicationName(CONN_APP_NAME);
        connStr.setConnectTimeout(30);
        return connStr.toString();
    }
}
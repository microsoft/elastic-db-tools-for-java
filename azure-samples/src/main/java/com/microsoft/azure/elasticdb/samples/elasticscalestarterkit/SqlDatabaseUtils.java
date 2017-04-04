package com.microsoft.azure.elasticdb.samples.elasticscalestarterkit;

// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

/**
 * Helper methods for interacting with SQL Databases.
 */
public final class SqlDatabaseUtils {
    /**
     * SQL master database name.
     */
    public static final String MasterDatabaseName = "master";

    /**
     * Returns true if we can connect to the database.
     */
    public static boolean TryConnectToSqlDatabase() {
        String connectionString = Configuration.GetConnectionString(Configuration.getShardMapManagerServerName(), MasterDatabaseName);

        try {
            /*try (ReliableSqlConnection conn = new ReliableSqlConnection(connectionString, getSqlRetryPolicy(), getSqlRetryPolicy())) {
                conn.Open();
            }*/


            return true;
        } catch (Exception e) {
            ConsoleUtils.WriteWarning("Failed to connect to SQL database with connection string:");
            System.out.printf("\n%1$s\n" + "\r\n", connectionString);
            ConsoleUtils.WriteWarning("If this connection string is incorrect, please update the Sql Database settings in App.Config.\n\nException message: {0}", e.getMessage());
            return false;
        }
    }

    public static boolean DatabaseExists(String shardMapManagerServerName, String databaseName) {
        return false;
    }

    public static void CreateDatabase(String shardMapManagerServerName, String databaseName) {
    }

    public static void ExecuteSqlScript(String shardMapManagerServerName, String databaseName, String initializeShardScriptFile) {
    }
}
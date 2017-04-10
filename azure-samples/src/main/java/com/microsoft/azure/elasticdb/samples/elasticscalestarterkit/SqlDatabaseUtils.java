package com.microsoft.azure.elasticdb.samples.elasticscalestarterkit;

// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

import com.microsoft.azure.elasticdb.core.commons.transientfaulthandling.RetryPolicy;
import com.microsoft.sqlserver.jdbc.SQLServerConnection;

import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

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

        SQLServerConnection conn = null;
        try {
            ConsoleUtils.WriteInfo("Connecting to Azure Portal...\r\n");
            conn = (SQLServerConnection) DriverManager.getConnection(connectionString);
            ConsoleUtils.WriteInfo("Connection Successful...\r\n");
        } catch (Exception e) {
            ConsoleUtils.WriteWarning("Failed to connect to SQL database with connection string:");
            System.out.printf("\n%1$s\n" + "\r\n", connectionString);
            ConsoleUtils.WriteWarning("If this connection string is incorrect, please update the Sql Database settings in App.Config.\n\nException message: %s", e.getMessage());
            return false;
        } finally {
            connFinally(conn);
        }
        return true;
    }

    private static void connFinally(SQLServerConnection conn) {
        try {
            if (conn != null && !conn.isClosed()) {
                conn.close();
            } else {
                ConsoleUtils.WriteWarning("Returned Connection was either null or already closed.");
            }
        } catch (SQLException ex) {
            ex.printStackTrace();
        }
    }

    public static boolean DatabaseExists(String connectionString, String dbName) {
        SQLServerConnection conn = null;
        try {
            conn = (SQLServerConnection) DriverManager.getConnection(connectionString);
            String query = "select count(*) from sys.databases where name = '" + dbName + "';";
            try(Statement stmt = conn.createStatement()) {
                ResultSet rs = stmt.executeQuery(query);
                return rs.next();
            } catch (SQLException ex) {
                ex.printStackTrace();
            }
        } catch (Exception e) {
            ConsoleUtils.WriteWarning("Failed to connect to SQL database with connection string:");
            System.out.printf("\n%1$s\n" + "\r\n", connectionString);
            ConsoleUtils.WriteWarning("If this connection string is incorrect, please update the Sql Database settings in App.Config.\n\nException message: %s", e.getMessage());
            return false;
        } finally {
            connFinally(conn);
        }
        return true;
    }

    public static void CreateDatabase(String shardMapManagerServerName, String databaseName) {
    }

    public static void ExecuteSqlScript(String shardMapManagerServerName, String databaseName, String initializeShardScriptFile) {
    }

    public static RetryPolicy getSqlRetryPolicy() {
        return null; //TODO
    }

    public static void DropDatabase(String dataSource, String database) {
    }
}
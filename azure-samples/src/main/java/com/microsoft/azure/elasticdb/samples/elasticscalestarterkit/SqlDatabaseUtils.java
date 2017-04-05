package com.microsoft.azure.elasticdb.samples.elasticscalestarterkit;

// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

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
            conn = (SQLServerConnection) DriverManager.getConnection(connectionString);
        } catch (Exception e) {
            ConsoleUtils.WriteWarning("Failed to connect to SQL database with connection string:");
            System.out.printf("\n%1$s\n" + "\r\n", connectionString);
            ConsoleUtils.WriteWarning("If this connection string is incorrect, please update the Sql Database settings in App.Config.\n\nException message: {0}", e.getMessage());
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
            Statement stmt = null;
            String query = "select count(*) from sys.databases where name = '" + dbName + "';";
            try {
                stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery(query);
                while (rs.next()) {
                    return true;
                }
            } catch (SQLException ex) {
                ex.printStackTrace();
            } finally {
                if (stmt != null) {
                    stmt.close();
                }
            }
        } catch (Exception e) {
            ConsoleUtils.WriteWarning("Failed to connect to SQL database with connection string:");
            System.out.printf("\n%1$s\n" + "\r\n", connectionString);
            ConsoleUtils.WriteWarning("If this connection string is incorrect, please update the Sql Database settings in App.Config.\n\nException message: {0}", e.getMessage());
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
}
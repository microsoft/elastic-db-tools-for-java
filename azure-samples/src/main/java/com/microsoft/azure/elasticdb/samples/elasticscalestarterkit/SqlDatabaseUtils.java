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
            ConsoleUtils.WriteWarning("If this connection string is incorrect, please update the Configuration file.\n\nException message: %s", e.getMessage());
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
            try (Statement stmt = conn.createStatement()) {
                ResultSet rs = stmt.executeQuery(query);
                return rs.next();
            } catch (SQLException ex) {
                ex.printStackTrace();
            }
        } catch (Exception e) {
            ConsoleUtils.WriteWarning("Failed to connect to SQL database with connection string:");
            System.out.printf("\n%1$s\n" + "\r\n", connectionString);
            ConsoleUtils.WriteWarning("If this connection string is incorrect, please update the Configuration file.\n\nException message: %s", e.getMessage());
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

    public static void DropDatabase(String server, String db) {
        ConsoleUtils.WriteInfo("Dropping database %s", db);
        SQLServerConnection conn = null;
        String connectionString = Configuration.GetConnectionString(server, MasterDatabaseName);
        try {
            conn = (SQLServerConnection) DriverManager.getConnection(connectionString);
            String query = "SELECT CAST(SERVERPROPERTY('EngineEdition') AS NVARCHAR(128))";
            try (Statement stmt = conn.createStatement()) {
                ResultSet rs = stmt.executeQuery(query);
                while (rs.next()) {
                    query = rs.getInt(1) == 5 ?
                            String.format("DROP DATABASE %1$s", BracketEscapeName(db))
                            : String.format("ALTER DATABASE %1$s SET SINGLE_USER WITH ROLLBACK IMMEDIATE"
                            + "\r\nDROP DATABASE %1$s", BracketEscapeName(db));
                    stmt.executeUpdate(query);
                }
            } catch (SQLException ex) {
                ex.printStackTrace();
            }
        } catch (Exception e) {
            ConsoleUtils.WriteWarning("Failed to connect to SQL database with connection string:");
            System.out.printf("\n%1$s\n" + "\r\n", connectionString);
            ConsoleUtils.WriteWarning("If this connection string is incorrect, please update the Configuration file.\n\nException message: %s", e.getMessage());
        } finally {
            connFinally(conn);
        }
    }

    /**
     * Escapes a SQL object name with brackets to prevent SQL injection.
     */
    private static String BracketEscapeName(String sqlName) {
        return '[' + sqlName.replace("]", "]]") + ']';
    }
}
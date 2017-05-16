package com.microsoft.azure.elasticdb.samples.elasticscalestarterkit;

/* Copyright (c) Microsoft. All rights reserved.
Licensed under the MIT license. See LICENSE file in the project root for full license information.*/

import com.microsoft.azure.elasticdb.core.commons.transientfaulthandling.RetryPolicy;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.concurrent.TimeUnit;

/**
 * Helper methods for interacting with SQL Databases.
 */
final class SqlDatabaseUtils {

  /**
   * SQL master database name.
   */
  private static final String MasterDatabaseName = "master";

  /**
   * Returns true if we can connect to the database.
   */
  static boolean tryConnectToSqlDatabase() {
    String serverName = Configuration.getShardMapManagerServerName();
    String connectionString = Configuration.getConnectionString(serverName, MasterDatabaseName);

    Connection conn = null;
    try {
      ConsoleUtils.writeInfo("Connecting to Azure Portal...");
      conn = DriverManager.getConnection(connectionString);
      ConsoleUtils.writeInfo("Connection Successful... Server Name: " + serverName);
    } catch (Exception e) {
      ConsoleUtils.writeWarning("Failed to connect to SQL database with connection string:");
      System.out.printf("\n%1$s\n" + "\r\n", connectionString);
      ConsoleUtils.writeWarning("If this connection string is incorrect, please update the"
          + "Configuration file.\r\nException message: %s", e.getMessage());
      return false;
    } finally {
      connFinally(conn);
    }
    return true;
  }

  private static void connFinally(Connection conn) {
    try {
      if (conn != null && !conn.isClosed()) {
        conn.close();
      } else {
        ConsoleUtils.writeWarning("Returned Connection was either null or already closed.");
      }
    } catch (SQLException ex) {
      ex.printStackTrace();
    }
  }

  static boolean databaseExists(String serverName, String dbName) {
    String connectionString = Configuration.getConnectionString(serverName, dbName);
    Connection conn = null;
    try {
      conn = DriverManager.getConnection(connectionString);
      String query = "select count(*) from sys.databases where name = '" + dbName + "';";
      try (Statement stmt = conn.createStatement()) {
        ResultSet rs = stmt.executeQuery(query);
        return rs.next();
      } catch (SQLException ex) {
        ex.printStackTrace();
      }
    } catch (Exception e) {
      ConsoleUtils.writeWarning("Failed to connect to SQL database with connection string:");
      System.out.printf("\n%1$s\n" + "\r\n", connectionString);
      ConsoleUtils.writeWarning("If this connection string is incorrect, please update the"
          + "Configuration file.\r\nException message: %s", e.getMessage());
      return false;
    } finally {
      connFinally(conn);
    }
    return true;
  }

  static String createDatabase(String server, String db) {
    ConsoleUtils.writeInfo("Creating database %s", db);
    Connection conn = null;
    String connectionString = Configuration.getConnectionString(server, MasterDatabaseName);
    String dbConnectionString = "";
    try {
      conn = DriverManager.getConnection(connectionString);
      String query = "SELECT CAST(SERVERPROPERTY('EngineEdition') AS NVARCHAR(128))";
      try (Statement stmt = conn.createStatement()) {
        ResultSet rs = stmt.executeQuery(query);
        if (rs.next()) {
          if (rs.getInt(1) == 5) {
            query = String.format("CREATE DATABASE %1$s (EDITION = '%2$s')",
                bracketEscapeName(db), Configuration.getDatabaseEdition());
            stmt.executeUpdate(query);
            dbConnectionString = Configuration.getConnectionString(server, db);
            while (!databaseIsOnline((Connection)
                DriverManager.getConnection(dbConnectionString), db)) {
              ConsoleUtils.writeInfo("Waiting for database %s to come online...", db);
              TimeUnit.SECONDS.sleep(5);
            }
            ConsoleUtils.writeInfo("Database %s is online", db);
          } else {
            query = String.format("CREATE DATABASE %1$s",
                bracketEscapeName(db) + Configuration.getDatabaseEdition());
            stmt.executeUpdate(query);
            dbConnectionString = Configuration.getConnectionString(server, db);
          }
        }
      } catch (SQLException ex) {
        ex.printStackTrace();
      }
    } catch (Exception e) {
      ConsoleUtils.writeWarning("Failed to connect to SQL database with connection string:");
      System.out.printf("\n%1$s\n" + "\r\n", connectionString);
      ConsoleUtils.writeWarning("If this connection string is incorrect, please update the"
          + "Configuration file.\r\nException message: %s", e.getMessage());
    } finally {
      connFinally(conn);
    }
    return dbConnectionString;
  }

  private static boolean databaseIsOnline(Connection conn, String db) {
    try (Statement stmt = conn.createStatement()) {
      ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM sys.databases WHERE name = '"
          + db + "' and state = 0");
      return (rs.next() && rs.getInt(1) > 0);
    } catch (SQLException ex) {
      ex.printStackTrace();
      return false;
    }
  }

  static void executeSqlScript(String server, String db, String schemaFile) {
    ConsoleUtils.writeInfo("Executing script %s", schemaFile);
    Connection conn = null;
    try {
      conn = DriverManager
          .getConnection(Configuration.getConnectionString(server, db));
      try (Statement stmt = conn.createStatement()) {
        // Read the commands from the sql script file
        ArrayList<String> commands = readSqlScript(schemaFile);

        for (String cmd : commands) {
          stmt.execute(cmd);
        }
      }
    } catch (SQLException e) {
      e.printStackTrace();
    }
  }

  private static ArrayList<String> readSqlScript(String scriptFile) {
    ArrayList<String> commands = new ArrayList<>();
    try (BufferedReader tr = new BufferedReader(
        new InputStreamReader(
            Program.class.getClassLoader()
                .getResource(scriptFile).openStream(), "UTF-8"))) {
      StringBuilder sb = new StringBuilder();
      String line;
      while ((line = tr.readLine()) != null) {
        if (!line.startsWith("--")) {
          if (line.equalsIgnoreCase("GO")) {
            commands.add(sb.toString());
            sb = new StringBuilder();
          } else {
            sb.append(line).append(System.lineSeparator());
          }
        }
      }
    } catch (NullPointerException | IOException e) {
      e.printStackTrace();
    }
    return commands;
  }

  static RetryPolicy getSqlRetryPolicy() {
    return new RetryPolicy();
  }

  static void dropDatabase(String server, String db) {
    ConsoleUtils.writeInfo("Dropping database %s", db);
    Connection conn = null;
    String connectionString = Configuration.getConnectionString(server, MasterDatabaseName);
    try {
      conn = DriverManager.getConnection(connectionString);
      String query = "SELECT CAST(SERVERPROPERTY('EngineEdition') AS NVARCHAR(128))";
      try (Statement stmt = conn.createStatement()) {
        ResultSet rs = stmt.executeQuery(query);
        if (rs.next()) {
          query = rs.getInt(1) == 5
              ? String.format("DROP DATABASE %1$s", bracketEscapeName(db))
              : String.format("ALTER DATABASE %1$s SET SINGLE_USER WITH ROLLBACK IMMEDIATE"
                  + "\r\nDROP DATABASE %1$s", bracketEscapeName(db));
          stmt.executeUpdate(query);
        }
      } catch (SQLException ex) {
        ex.printStackTrace();
      }
    } catch (Exception e) {
      ConsoleUtils.writeWarning("Failed to connect to SQL database with connection string:");
      System.out.printf("\n%1$s\n" + "\r\n", connectionString);
      ConsoleUtils.writeWarning("If this connection string is incorrect, please update the"
          + "Configuration file.\r\nException message: %s", e.getMessage());
    } finally {
      connFinally(conn);
    }
  }

  /**
   * Escapes a SQL object name with brackets to prevent SQL injection.
   */
  private static String bracketEscapeName(String sqlName) {
    return '[' + sqlName.replace("]", "]]") + ']';
  }
}
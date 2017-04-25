package com.microsoft.azure.elasticdb.samples.elasticscalestarterkit;

/* Copyright (c) Microsoft. All rights reserved.
Licensed under the MIT license. See LICENSE file in the project root for full license information.*/

import com.microsoft.azure.elasticdb.shard.sqlstore.SqlConnectionStringBuilder;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * Provides access to app.config settings, and contains advanced configuration settings.
 */
final class Configuration {

  /**
   * Get the following properties from resource file.
   * CONN_SERVER_NAME
   * CONN_DB_NAME
   * CONN_USER
   * CONN_PASSWORD
   * CONN_APP_NAME
   * DB_EDITION
   * RANGE_SHARD_MAP_NAME
   * LIST_SHARD_MAP_NAME
   */
  private static Properties properties = loadProperties();

  static Properties loadProperties() {
    InputStream inStream = Configuration.class
        .getClassLoader().getResourceAsStream("resources.properties");
    Properties prop = new Properties();
    if (inStream != null) {
      try {
        prop.load(inStream);
      } catch (IOException e) {
        e.printStackTrace();
      }
    }
    return prop;
  }

  /**
   * Gets the server name for the Shard Map Manager database, which contains the shard maps.
   */
  static String getShardMapManagerServerName() {
    return properties.getProperty("CONN_SERVER_NAME");
  }

  /**
   * Gets the database name for the Shard Map Manager database, which contains the shard
   * maps.
   */
  static String getShardMapManagerDatabaseName() {
    return properties.getProperty("CONN_DB_NAME");
  }

  /**
   * Gets the name for the Range Shard Map that contains metadata for all the shards
   * and the mappings to those shards.
   */
  static String getRangeShardMapName() {
    return properties.getProperty("RANGE_SHARD_MAP_NAME");
  }

  /**
   * Gets the name for the List Shard Map that contains metadata for all the shards
   * and the mappings to those shards.
   */
  static String getListShardMapName() {
    return properties.getProperty("LIST_SHARD_MAP_NAME");
  }

  /**
   * Gets the edition to use for Shards and Shard Map Manager Database if the server is an
   * Azure SQL DB server. If the server is a regular SQL Server then this is ignored.
   */
  static String getDatabaseEdition() {
    return properties.getProperty("DB_EDITION");
  }

  /**
   * Returns a connection string that can be used to connect to the specified server and database.
   */
  static String getConnectionString(String serverName, String database) {
    SqlConnectionStringBuilder connStr = new SqlConnectionStringBuilder(
        getCredentialsConnectionString());
    connStr.setDataSource(serverName);
    connStr.setDatabaseName(database);
    return connStr.toString();
  }

  /**
   * Returns a connection string to use for Data-Dependent Routing and Multi-Shard Query,
   * which does not contain DataSource or DatabaseName.
   */
  static String getCredentialsConnectionString() {

    // Get Integrated Security from the app.config file.
    // If it exists, then parse it (throw exception on failure), otherwise default to false.
    String integratedSecurityString = "false";
    boolean integratedSecurity =
        integratedSecurityString != null && Boolean.parseBoolean(integratedSecurityString);

    SqlConnectionStringBuilder connStr = new SqlConnectionStringBuilder();
    connStr.setUser(properties.getProperty("CONN_USER"));
    connStr.setPassword(properties.getProperty("CONN_PASSWORD"));
    connStr.setIntegratedSecurity(integratedSecurity);
    connStr.setApplicationName(properties.getProperty("CONN_APP_NAME"));
    connStr.setConnectTimeout(30);
    return connStr.toString();
  }
}
package com.microsoft.azure.elasticdb.shard.mapmanager.unittests;

import com.microsoft.azure.elasticdb.shard.sqlstore.SqlConnectionStringBuilder;

/**
 * Class that is container of global constants & methods.
 */
public final class Globals {

  public static final String TEST_CONN_SERVER_NAME = "aehob8ow4j.database.windows.net";
  public static final String TEST_CONN_USER = "prabhu";
  public static final String TEST_CONN_PASSWORD = "3YX8EpPKHnQs";
  public static final String TEST_CONN_APP_NAME = "ESC_SKv1.0";
  /**
   * Connection string for connecting to test server.
   */
  public static final String SHARD_MAP_MANAGER_TEST_CONN_STRING =
      Globals.shardMapManagerConnectionString();
  /**
   * SharedMapManager database name
   */
  public static final String SHARD_MAP_MANAGER_DATABASE_NAME = "ShardMapManager";
  /**
   * Query to create database.
   */
  public static final String CREATE_DATABASE_QUERY = "CREATE DATABASE [%1$s]";
  /**
   * Query to drop database.
   */
  public static final String DROP_DATABASE_QUERY = "DROP DATABASE [%1$s]";
  /**
   * SMM connection String
   */
  public static final String SHARD_MAP_MANAGER_CONN_STRING =
      Globals.shardMapManagerConnectionString() + "DatabaseName="
          + Globals.SHARD_MAP_MANAGER_DATABASE_NAME + ";";

  public static final String SHARD_USER_CONN_STRING = Globals.shardUserConnString();

  /**
   * Connection string for global shard map manager
   */
  public static String shardMapManagerConnectionString() {
    boolean integratedSecurityString = false;

    SqlConnectionStringBuilder connStr = new SqlConnectionStringBuilder();
    connStr.setUser(TEST_CONN_USER);
    connStr.setPassword(TEST_CONN_PASSWORD);
    connStr.setDataSource(TEST_CONN_SERVER_NAME);
    connStr.setIntegratedSecurity(integratedSecurityString);
    connStr.setApplicationName(TEST_CONN_APP_NAME);
    connStr.setConnectTimeout(30);
    return connStr.toString();
  }

  public static String shardUserConnString() {

    SqlConnectionStringBuilder connStr = new SqlConnectionStringBuilder();
    connStr.setUser(TEST_CONN_USER);
    connStr.setPassword(TEST_CONN_PASSWORD);
    connStr.setDataSource(TEST_CONN_SERVER_NAME);
    connStr.setIntegratedSecurity(true);
    connStr.setApplicationName(TEST_CONN_APP_NAME);
    connStr.setConnectTimeout(30);
    return connStr.toString();
  }
}

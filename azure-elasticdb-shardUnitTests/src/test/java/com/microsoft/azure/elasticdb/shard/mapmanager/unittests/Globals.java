package com.microsoft.azure.elasticdb.shard.mapmanager.unittests;

import com.microsoft.azure.elasticdb.shard.sqlstore.SqlConnectionStringBuilder;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * Class that is container of global constants & methods.
 */
final class Globals {

  /**
   * SharedMapManager database name
   */
  static final String SHARD_MAP_MANAGER_DATABASE_NAME = "ShardMapManager_Test";
  /**
   * Query to create database.
   */
  static final String CREATE_DATABASE_QUERY = "IF EXISTS"
      + " (SELECT name FROM sys.databases WHERE name = N'%1$s') BEGIN"
      + " DROP DATABASE [%1$s] END CREATE DATABASE [%1$s]";
  /**
   * Query to drop database.
   */
  static final String DROP_DATABASE_QUERY = "IF  EXISTS"
      + " (SELECT name FROM master.dbo.sysdatabases WHERE name = N'%1$s') DROP DATABASE [%1$s]";
  private static Properties properties = loadProperties();
  private static final String TEST_CONN_USER = properties.getProperty("TEST_CONN_USER");
  private static final String TEST_CONN_PASSWORD = properties.getProperty("TEST_CONN_PASSWORD");
  /**
   * Connection string for connecting to test server.
   */
  static final String SHARD_MAP_MANAGER_TEST_CONN_STRING =
      Globals.shardMapManagerTestConnectionString();
  /**
   * SMM connection String
   */
  static final String SHARD_MAP_MANAGER_CONN_STRING = Globals.shardMapManagerConnectionString();
  static final String SHARD_USER_CONN_STRING = Globals.shardUserConnString();
  /**
   * Name of the test server
   */
  static final String TEST_CONN_SERVER_NAME = properties.getProperty("TEST_CONN_SERVER_NAME");

  private static Properties loadProperties() {
    InputStream inStream = Globals.class.getClassLoader()
        .getResourceAsStream("resources.properties");
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
   * Connection string for global shard map manager
   */
  private static String shardMapManagerConnectionString() {
    SqlConnectionStringBuilder connStr = new SqlConnectionStringBuilder();
    connStr.setDataSource(TEST_CONN_SERVER_NAME);
    connStr.setDatabaseName(SHARD_MAP_MANAGER_DATABASE_NAME);
    connStr.setIntegratedSecurity(false);
    connStr.setUser(TEST_CONN_USER);
    connStr.setPassword(TEST_CONN_PASSWORD);
    return connStr.toString();
  }

  /**
   * Connection string for global shard map manager
   */
  private static String shardMapManagerTestConnectionString() {
    SqlConnectionStringBuilder connStr = new SqlConnectionStringBuilder();
    connStr.setDataSource(TEST_CONN_SERVER_NAME);
    connStr.setIntegratedSecurity(false);
    connStr.setUser(TEST_CONN_USER);
    connStr.setPassword(TEST_CONN_PASSWORD);
    return connStr.toString();
  }

  private static String shardUserConnString() {
    SqlConnectionStringBuilder connStr = new SqlConnectionStringBuilder();
    connStr.setUser(TEST_CONN_USER);
    connStr.setPassword(TEST_CONN_PASSWORD);
    connStr.setIntegratedSecurity(true);
    return connStr.toString();
  }
}

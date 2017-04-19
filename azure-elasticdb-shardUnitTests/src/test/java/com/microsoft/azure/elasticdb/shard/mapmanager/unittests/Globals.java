package com.microsoft.azure.elasticdb.shard.mapmanager.unittests;

import com.microsoft.azure.elasticdb.shard.sqlstore.SqlConnectionStringBuilder;

/**
 * 
 *Class that is container of global constants & methods.
 *
 */
public final class Globals {
	 public static final String TEST_CONN_SERVER_NAME = "aehob8ow4j.database.windows.net";
	 public static final String TEST_CONN_USER = "prabhu";
	 public static final String TEST_CONN_PASSWORD = "3YX8EpPKHnQs";
	 public static final String TEST_CONN_APP_NAME = "ESC_SKv1.0";
	/**
	 * Connection string for connecting to test server.
	 */
	public static final String ShardMapManagerTestConnectionString = Globals.ShardMapManagerConnectionString();
	/**
	 * SharedMapManager databse name
	 */
	public static final String ShardMapManagerDatabaseName = "ShardMapManager";
	/**
	 * 		
	 *Connection string for global shard map manager
	 */
	public static String ShardMapManagerConnectionString()
	{
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
	/**
	 * Query to create database.
	 */
	public static final String CreateDatabaseQuery ="CREATE DATABASE [%1$s]";
	/**
	 *  Query to drop database.
	 */
	public static final String DropDatabaseQuery ="DROP DATABASE [%1$s]";
	/**
	 * SMM connection String
	 */
    public static final String ShardMapManagerConnectionString = Globals.ShardMapManagerConnectionString() +"DatabaseName="+Globals.ShardMapManagerDatabaseName+";"; 
}

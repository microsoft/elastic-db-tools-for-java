package com.microsoft.azure.elasticdb.query.unittests;

/*Copyright (c) Microsoft. All rights reserved.
Licensed under the MIT license. See LICENSE file in the project root for full license information.*/

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import java.util.Random;

/**
 * Common utilities used by tests
 */
public final class MultiShardTestUtils {

  /**
   * Connection string for local shard user.
   */
  public static String shardConnectionString
      = "Integrated Security=False;User ID=sa;Password=SystemAdmin;";
  private static Properties properties = loadProperties();
  private static final String TEST_SERVER_NAME = properties.getProperty("TEST_CONN_SERVER_NAME");
  private static final String TEST_CONN_USER = properties.getProperty("TEST_CONN_USER");
  private static final String TEST_CONN_PASSWORD = properties.getProperty("TEST_CONN_PASSWORD");
  /**
   * Name of the database where the ShardMapManager persists its data.
   */
  private static String shardMapManagerDbName = "ShardMapManager";
  /**
   * Connection string for global shard map manager operations.
   */
  public static String shardMapManagerConnectionString = "Data Source=" + TEST_SERVER_NAME
      + ";Initial Catalog=" + shardMapManagerDbName
      + ";Integrated Security=False;User ID=sa;Password=SystemAdmin;";
  /**
   * Table name for the sharded table we will issue fanout queries against.
   */
  private static String tableName = "ConsistentShardedTable";
  /**
   * Field on the sharded table where we will store the database name.
   */
  private static String dbNameField = "dbNameField";
  /**
   * Name of the test shard map to use.
   */
  private static String shardMapName = "TestShardMap";

  /**
   * Class level Random object.
   */
  private static Random random = new Random();

  private static Properties loadProperties() {
    InputStream inStream = MultiShardTestUtils.class.getClassLoader()
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
}
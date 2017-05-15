package com.microsoft.azure.elasticdb.samples.elasticscalestarterkit;

/* Copyright (c) Microsoft. All rights reserved.
Licensed under the MIT license. See LICENSE file in the project root for full license information.*/

import com.microsoft.azure.elasticdb.query.logging.MultiShardExecutionOptions;
import com.microsoft.azure.elasticdb.query.logging.MultiShardExecutionPolicy;
import com.microsoft.azure.elasticdb.query.multishard.MultiShardCommand;
import com.microsoft.azure.elasticdb.query.multishard.MultiShardConnection;
import com.microsoft.azure.elasticdb.query.multishard.MultiShardDataReader;
import com.microsoft.azure.elasticdb.shard.base.Shard;
import com.microsoft.azure.elasticdb.shard.map.RangeShardMap;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

final class MultiShardQuerySample {

  static void executeMultiShardQuery(RangeShardMap<Integer> shardMap,
      String credentialsConnectionString) {
    // Get the shards to connect to
    List<Shard> shards = shardMap.getShards();

    // Create the multi-shard connection
    try (MultiShardConnection conn = new MultiShardConnection(credentialsConnectionString,
        shards.toArray(new Shard[shards.size()]))) {
      // Create a simple command
      try (MultiShardCommand cmd = conn.createCommand()) {
        // Because this query is grouped by CustomerID, which is sharded,
        // we will not get duplicate rows.
        cmd.setCommandText("SELECT c.CustomerId, c.Name AS CustomerName, "
            + "COUNT(o.OrderID) AS OrderCount FROM dbo.Customers AS c INNER JOIN dbo.Orders AS o"
            + " ON c.CustomerID = o.CustomerID GROUP BY c.CustomerId, c.Name ORDER BY OrderCount");

        // Append a column with the shard name where the row came from
        cmd.setExecutionOptions(MultiShardExecutionOptions.IncludeShardNameColumn);

        // Allow for partial results in case some shards do not respond in time
        cmd.setExecutionPolicy(MultiShardExecutionPolicy.PartialResults);

        // Allow the entire command to take up to 30 seconds
        cmd.setCommandTimeout(30);

        // Execute the command.
        // We do not need to specify retry logic because MultiShardDataReader will internally retry
        // until the CommandTimeout expires.
        try (MultiShardDataReader reader = cmd.executeReader()) {
          // Get the column names
          TableFormatter formatter = new TableFormatter(
              getColumnNames(reader).toArray(new String[0]));

          int rows = 0;
          while (reader.read()) {
            // Read the values using standard DbDataReader methods
            Object[] values = new Object[reader.fieldCount];
            reader.getValues(values);

            // Extract just database name from the $ShardLocation pseudocolumn to make the output
            // formater cleaner.
            // Note that the $ShardLocation pseudocolumn is always the last column
            int shardLocationOrdinal = values.length - 1;
            values[shardLocationOrdinal] = extractDatabaseName(
                values[shardLocationOrdinal].toString());

            // Add values to output formatter
            formatter.addRow(values);

            rows++;
          }

          System.out.println(formatter.toString());
          System.out.printf("(%1$s rows returned)" + "\r\n", rows);
        }
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  /**
   * Gets the column names from a data reader.
   */
  private static List<String> getColumnNames(MultiShardDataReader reader) {
    ArrayList<String> columnNames = new ArrayList<>();
    try {
      for (int i = 0; i < reader.getMetaData().getColumnCount(); i++) {
        columnNames.add(reader.getMetaData().getColumnName(i));
      }
    } catch (SQLException e) {
      e.printStackTrace();
    }
    return columnNames;
  }

  /**
   * Extracts the database name from the provided shard location string.
   */
  private static String extractDatabaseName(String shardLocationString) {
    String[] matches = shardLocationString.split("([)|(DataSource=)|(Database=)|(])", 0);
    return matches[1];
  }
}
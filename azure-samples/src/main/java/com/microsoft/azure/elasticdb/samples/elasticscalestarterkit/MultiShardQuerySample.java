package com.microsoft.azure.elasticdb.samples.elasticscalestarterkit;

/* Copyright (c) Microsoft. All rights reserved.
Licensed under the MIT license. See LICENSE file in the project root for full license information.*/

import com.microsoft.azure.elasticdb.query.logging.MultiShardExecutionOptions;
import com.microsoft.azure.elasticdb.query.logging.MultiShardExecutionPolicy;
import com.microsoft.azure.elasticdb.query.multishard.MultiShardConnection;
import com.microsoft.azure.elasticdb.query.multishard.MultiShardResultSet;
import com.microsoft.azure.elasticdb.query.multishard.MultiShardStatement;
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
      try (MultiShardStatement cmd = conn.createCommand()) {
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

        // Execute the command. We do not need to specify retry logic because MultiShardResultSet
        // will internally retry until the CommandTimeout expires.
        try (MultiShardResultSet resultSet = cmd.executeQuery()) {
          int rows = 0;
          if (resultSet.next()) {
            // Get the column names
            List<String> columnNames = getColumnNames(resultSet);
            columnNames.add("ShardLocation");

            TableFormatter formatter = new TableFormatter(
                columnNames.toArray(new String[columnNames.size()]));

            do {
              // Read the values using standard Result Set methods
              int customerId = resultSet.getInt(1);
              String customerName = resultSet.getString(2);
              int orderId = resultSet.getInt(3);

              // Extract just database name from the $ShardLocation pseudo-column to make the output
              // format cleaner. Note that $ShardLocation pseudo-column is always the last column
              String location = resultSet.getLocation();

              String[] values = new String[]{
                  Integer.toString(customerId),
                  customerName,
                  Integer.toString(orderId),
                  location
              };

              // Add values to output formatter
              formatter.addRow(values);

              rows++;
            } while (resultSet.next());

            System.out.println(formatter.toString());
          }
          System.out.printf("(%1$s rows returned)" + "\r\n", rows);
        }
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  /**
   * Gets the column names from a data resultSet.
   */
  private static List<String> getColumnNames(MultiShardResultSet resultSet) {
    ArrayList<String> columnNames = new ArrayList<>();
    try {
      for (int i = 1; i <= resultSet.getMetaData().getColumnCount(); i++) {
        columnNames.add(resultSet.getMetaData().getColumnName(i));
      }
    } catch (SQLException e) {
      e.printStackTrace();
    }
    return columnNames;
  }
}
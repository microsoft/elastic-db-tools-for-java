package com.microsoft.azure.elasticdb.samples.elasticscalestarterkit;

/* Copyright (c) Microsoft. All rights reserved.
Licensed under the MIT license. See LICENSE file in the project root for full license information.*/

import com.microsoft.azure.elasticdb.shard.base.Shard;
import com.microsoft.azure.elasticdb.shard.map.RangeShardMap;
import java.util.List;

public final class MultiShardQuerySample {

  public static void ExecuteMultiShardQuery(RangeShardMap<Integer> shardMap,
      String credentialsConnectionString) {
    // Get the shards to connect to
    List<Shard> shards = shardMap.GetShards();

    // Create the multi-shard connection
    //TODO
        /*try (MultiShardConnection conn = new MultiShardConnection(shards, credentialsConnectionString)) {
            // Create a simple command
            try (MultiShardCommand cmd = conn.CreateCommand()) {
                // Because this query is grouped by CustomerID, which is sharded,
                // we will not get duplicate rows.
                cmd.CommandText = "" + "\r\n" +
                        "                        SELECT " + "\r\n" +
                        "                            c.CustomerId, " + "\r\n" +
                        "                            c.Name AS CustomerName, " + "\r\n" +
                        "                            COUNT(o.OrderID) AS OrderCount" + "\r\n" +
                        "                        FROM " + "\r\n" +
                        "                            dbo.Customers AS c INNER JOIN " + "\r\n" +
                        "                            dbo.Orders AS o" + "\r\n" +
                        "                            ON c.CustomerID = o.CustomerID" + "\r\n" +
                        "                        GROUP BY " + "\r\n" +
                        "                            c.CustomerId, " + "\r\n" +
                        "                            c.Name" + "\r\n" +
                        "                        ORDER BY " + "\r\n" +
                        "                            OrderCount";

                // Append a column with the shard name where the row came from
                cmd.ExecutionOptions = MultiShardExecutionOptions.IncludeShardNameColumn;

                // Allow for partial results in case some shards do not respond in time
                cmd.ExecutionPolicy = MultiShardExecutionPolicy.PartialResults;

                // Allow the entire command to take up to 30 seconds
                cmd.CommandTimeout = 30;

                // Execute the command.
                // We do not need to specify retry logic because MultiShardDataReader will internally retry until the CommandTimeout expires.
                try (MultiShardDataReader reader = cmd.ExecuteReader()) {
                    // Get the column names
                    TableFormatter formatter = new TableFormatter(GetColumnNames(reader).toArray(new String[0]));

                    int rows = 0;
                    while (reader.Read()) {
                        // Read the values using standard DbDataReader methods
                        Object[] values = new Object[reader.FieldCount];
                        reader.GetValues(values);

                        // Extract just database name from the $ShardLocation pseudocolumn to make the output formater cleaner.
                        // Note that the $ShardLocation pseudocolumn is always the last column
                        int shardLocationOrdinal = values.length - 1;
                        values[shardLocationOrdinal] = ExtractDatabaseName(values[shardLocationOrdinal].toString());

                        // Add values to output formatter
                        formatter.addRow(values);

                        rows++;
                    }

                    System.out.println(formatter.toString());
                    System.out.printf("(%1$s rows returned)" + "\r\n", rows);
                }
            }
        }*/
  }

  /**
   * Gets the column names from a data reader.
   */
    /*private static List<String> GetColumnNames(DbDataReader reader) {
        ArrayList<String> columnNames = new ArrayList<String>();
        for (DataRow r : reader.GetSchemaTable().Rows) {
            columnNames.add(r[SchemaTableColumn.ColumnName].toString());
        }

        return columnNames;
    }*/

  /**
   * Extracts the database name from the provided shard location string.
   */
    /*private static String ExtractDatabaseName(String shardLocationString) {
        String[] pattern = new String[]{"[", "DataSource=", "Database=", "]"};
        String[] matches = shardLocationString.split(pattern, StringSplitOptions.RemoveEmptyEntries);
        return matches[1];
    }*/
}
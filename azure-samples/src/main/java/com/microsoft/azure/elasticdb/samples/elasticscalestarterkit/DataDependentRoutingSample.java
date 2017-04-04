package com.microsoft.azure.elasticdb.samples.elasticscalestarterkit;

// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

import com.microsoft.azure.elasticdb.shard.map.RangeShardMap;
import com.microsoft.azure.elasticdb.shard.map.ShardMap;

import java.util.Random;

public final class DataDependentRoutingSample {
    private static String[] s_customerNames = new String[]{"AdventureWorks Cycles", "Contoso Ltd.", "Microsoft Corp.", "Northwind Traders", "ProseWare, Inc.", "Lucerne Publishing", "Fabrikam, Inc.", "Coho Winery", "Alpine Ski House", "Humongous Insurance"};

    private static Random s_r = new Random();

    public static void ExecuteDataDependentRoutingQuery(RangeShardMap<Integer> shardMap, String credentialsConnectionString) {
        // A real application handling a request would need to determine the request's customer ID before connecting to the database.
        // Since this is a demo app, we just choose a random key out of the range that is mapped. Here we assume that the ranges
        // start at 0, are contiguous, and are bounded (i.e. there is no range where HighIsMax == true)

        //TODO: .Max(m -> m.Value.High)
        /*int currentMaxHighKey = shardMap.GetMappings().Max(m -> m.Value.High);
        int customerId = GetCustomerId(currentMaxHighKey);
        String customerName = s_customerNames[s_r.nextInt(s_customerNames.length)];
        int regionId = 0;
        int productId = 0;

        AddCustomer(shardMap, credentialsConnectionString, customerId, customerName, regionId);

        AddOrder(shardMap, credentialsConnectionString, customerId, productId);*/
    }

    /**
     * Adds a customer to the customers table (or updates the customer if that id already exists).
     */
    private static void AddCustomer(ShardMap shardMap, String credentialsConnectionString, int customerId, String name, int regionId) {
        // Open and execute the command with retry for transient faults. Note that if the command fails, the connection is closed, so
        // the entire block is wrapped in a retry. This means that only one command should be executed per block, since if we had multiple
        // commands then the first command may be executed multiple times if later commands fail.
        SqlDatabaseUtils.getSqlRetryPolicy().ExecuteAction(() -> {
            // Looks up the key in the shard map and opens a connection to the shard
            try (SqlConnection conn = shardMap.OpenConnectionForKey(customerId, credentialsConnectionString)) {
                // Create a simple command that will insert or update the customer information
                SqlCommand cmd = conn.CreateCommand();
                cmd.CommandText = "" + "\r\n" +
                        "                    IF EXISTS (SELECT 1 FROM Customers WHERE CustomerId = @customerId)" + "\r\n" +
                        "                        UPDATE Customers" + "\r\n" +
                        "                            SET Name = @name, RegionId = @regionId" + "\r\n" +
                        "                            WHERE CustomerId = @customerId" + "\r\n" +
                        "                    ELSE" + "\r\n" +
                        "                        INSERT INTO Customers (CustomerId, Name, RegionId)" + "\r\n" +
                        "                        VALUES (@customerId, @name, @regionId)";
                cmd.Parameters.AddWithValue("@customerId", customerId);
                cmd.Parameters.AddWithValue("@name", name);
                cmd.Parameters.AddWithValue("@regionId", regionId);
                cmd.CommandTimeout = 60;

                // Execute the command
                cmd.ExecuteNonQuery();
            }
        });
    }

    /**
     * Adds an order to the orders table for the customer.
     */
    private static void AddOrder(ShardMap shardMap, String credentialsConnectionString, int customerId, int productId) {
        SqlDatabaseUtils.getSqlRetryPolicy().ExecuteAction(() -> {
            // Looks up the key in the shard map and opens a connection to the shard
            try (SqlConnection conn = shardMap.OpenConnectionForKey(customerId, credentialsConnectionString)) {
                // Create a simple command that will insert a new order
                SqlCommand cmd = conn.CreateCommand();

                // Create a simple command
                cmd.CommandText = "INSERT INTO dbo.Orders (CustomerId, OrderDate, ProductId)" + "\r\n" +
                        "                                        VALUES (@customerId, @orderDate, @productId)";
                cmd.Parameters.AddWithValue("@customerId", customerId);
                cmd.Parameters.AddWithValue("@orderDate", java.time.LocalDateTime.now().Date);
                cmd.Parameters.AddWithValue("@productId", productId);
                cmd.CommandTimeout = 60;

                // Execute the command
                cmd.ExecuteNonQuery();
            }
        });

        ConsoleUtils.WriteInfo("Inserted order for customer ID: {0}", customerId);
    }

    /**
     * Gets a customer ID to insert into the customers table.
     */
    private static int GetCustomerId(int maxid) {
        // If this were a real app and we were inserting customer IDs, we would need a
        // service that generates unique new customer IDs.

        // Since this is a demo, just create a random customer ID. To keep the numbers
        // manageable for demo purposes, only use a range of integers that lies within existing ranges.

        return s_r.nextInt(maxid);
    }
}
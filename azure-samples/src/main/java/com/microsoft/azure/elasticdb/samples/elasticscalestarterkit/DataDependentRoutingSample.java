package com.microsoft.azure.elasticdb.samples.elasticscalestarterkit;

// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

import com.microsoft.azure.elasticdb.shard.base.Range;
import com.microsoft.azure.elasticdb.shard.base.RangeMapping;
import com.microsoft.azure.elasticdb.shard.map.RangeShardMap;
import com.microsoft.azure.elasticdb.shard.map.ShardMap;
import com.microsoft.sqlserver.jdbc.SQLServerConnection;
import com.microsoft.sqlserver.jdbc.SQLServerException;
import com.microsoft.sqlserver.jdbc.SQLServerStatement;

import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.LocalDate;
import java.util.Comparator;
import java.util.Random;

final class DataDependentRoutingSample {
    private static String[] s_customerNames = new String[]{"AdventureWorks Cycles", "Contoso Ltd.", "Microsoft Corp.", "Northwind Traders", "ProseWare, Inc.", "Lucerne Publishing", "Fabrikam, Inc.", "Coho Winery", "Alpine Ski House", "Humongous Insurance"};

    private static Random s_r = new Random();

    static void ExecuteDataDependentRoutingQuery(RangeShardMap<Integer> shardMap, String credentialsConnectionString) {
        // A real application handling a request would need to determine the request's customer ID
        // before connecting to the database. Since this is a demo app, we just choose a random key
        // out of the range that is mapped. Here we assume that the ranges start at 0, are contiguous,
        // and are bounded (i.e. there is no range where HighIsMax == true)

        int currentMaxHighKey = (Integer) shardMap.GetMappings()
                .stream()
                .map(RangeMapping::getValue)
                .map(Range::getHigh)
                .max(Comparator.comparingInt(v -> (Integer) v))
                .orElse(0);
        int customerId = GetCustomerId(currentMaxHighKey);
        String customerName = s_customerNames[s_r.nextInt(s_customerNames.length)];
        int regionId = 0;
        int productId = 0;

        AddCustomer(shardMap, credentialsConnectionString, customerId, customerName, regionId);

        AddOrder(shardMap, credentialsConnectionString, customerId, productId);
    }

    /**
     * Adds a customer to the customers table (or updates the customer if that id already exists).
     */
    private static void AddCustomer(ShardMap shardMap, String credentialsConnectionString, int customerId, String name, int regionId) {
        /*Open and execute the command with retry for transient faults.
        Note that if the command fails, the connection is closed, so the entire block is wrapped in a retry.
        This means that only one command should be executed per block, since if we had multiple commands then
        the first command may be executed multiple times if later commands fail.*/
        SqlDatabaseUtils.getSqlRetryPolicy().ExecuteAction(() -> {
            // Looks up the key in the shard map and opens a connection to the shard
            try (SQLServerConnection conn = shardMap.OpenConnectionForKey(customerId, credentialsConnectionString)) {
                // Create a simple command that will insert or update the customer information
                SQLServerStatement cmd = (SQLServerStatement) conn.createStatement();
                String query = "IF EXISTS (SELECT 1 FROM Customers WHERE CustomerId = " + customerId + ")" + "\r\n" +
                        "UPDATE Customers SET Name = '" + name + "', RegionId = " + regionId +
                        " WHERE CustomerId = " + customerId + "\r\n" + " ELSE " + "\r\n" +
                        "INSERT INTO Customers (CustomerId, Name, RegionId)" + "\r\n" +
                        "VALUES (" + customerId + ", '" + name + "', " + regionId + ")";
                cmd.setQueryTimeout(60);

                // Execute the command
                cmd.execute(query);
            } catch (SQLServerException e) {
                e.printStackTrace();
            }
        });
    }

    /**
     * Adds an order to the orders table for the customer.
     */
    private static void AddOrder(ShardMap shardMap, String credentialsConnectionString, int customerId, int productId) {
        SqlDatabaseUtils.getSqlRetryPolicy().ExecuteAction(() -> {
            // Looks up the key in the shard map and opens a connection to the shard
            try (SQLServerConnection conn = shardMap.OpenConnectionForKey(customerId, credentialsConnectionString)) {
                // Create a simple command that will insert a new order
                PreparedStatement ps = conn.prepareStatement("INSERT INTO dbo.Orders (CustomerId, OrderDate, ProductId) VALUES (?, ?, ?)");

                ps.setInt(1, customerId);
                ps.setDate(2, Date.valueOf(LocalDate.now()));
                ps.setInt(3, productId);
                ps.executeUpdate();
            } catch (SQLServerException e) {
                e.printStackTrace();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        });

        ConsoleUtils.WriteInfo("Inserted order for customer ID: %s", customerId);
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
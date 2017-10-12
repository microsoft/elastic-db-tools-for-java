package com.microsoft.azure.elasticdb.query.multishard;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/*
 * Elastic database tools for Azure SQL Database.
 * 
 * Copyright(c) Microsoft Corporation All rights reserved.
 * 
 * This program is made available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

import com.microsoft.azure.elasticdb.shard.base.Shard;
import com.microsoft.azure.elasticdb.shard.base.ShardLocation;
import com.microsoft.azure.elasticdb.shard.sqlstore.SqlConnectionStringBuilder;
import com.microsoft.azure.elasticdb.shard.utils.StringUtilsLocal;

/**
 * Represents a connection to a set of shards and provides the ability to process queries across the shard set. Purpose: Creates connections to the
 * given set of shards and governs their lifetime Notes: This class is NOT thread-safe.
 */
public final class MultiShardConnection implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    /**
     * The suffix to append to each shard's ApplicationName. Will help with server-side telemetry.
     */
    public static String ApplicationNameSuffix = "ESC_MSQv" + GlobalConstants.MultiShardQueryVersionInfo;

    /**
     * Whether this instance has already been disposed.
     */
    private boolean isDisposed = false;

    /**
     * Gets the collection of <see cref="Shard"/>s associated with this connection.
     */
    private List<Shard> shards;

    private List<Pair<ShardLocation, Connection>> shardConnections;

    private String connectionString;

    /**
     * Initializes a new instance of the <see cref="MultiShardConnection"/> class.
     *
     * @param connectionString
     *            These credentials will be used to connect to the <see cref="Shard"/>s. The same credentials are used on all shards. Therefore, all
     *            shards need to provide the appropriate permissions for these credentials to execute the command.
     * @param shards
     *            The collection of <see cref="Shard"/>s used for this connection instances.
     *
     *
     *            Multiple Active Result Sets (MARS) are not supported and are disabled for any processing at the shards.
     */
    public MultiShardConnection(String connectionString,
            Shard... shards) {
        if (StringUtilsLocal.isNullOrEmpty(connectionString)) {
            throw new IllegalArgumentException("connectionString");
        }

        // Enhance the ApplicationName with this library's name as a suffix
        // Devnote: If connection string specifies Active Directory authentication and runtime is not
        // .NET 4.6 or higher, then below call will throw.
        SqlConnectionStringBuilder connectionStringBuilder = (new SqlConnectionStringBuilder(connectionString))
                .withApplicationNameSuffix(ApplicationNameSuffix);
        List<Shard> shardList = Arrays.asList(shards);
        validateConnectionArguments(shardList, "shards", connectionStringBuilder);

        this.connectionString = connectionString;
        this.shards = shardList;
        this.shardConnections = shardList.stream().map(s -> {
            try {
                return (createDbConnectionForLocation(s.getLocation(), connectionStringBuilder));
            }
            catch (SQLException e) {
                throw new RuntimeException(e.getMessage(), e);
            }
        }).collect(Collectors.toList());
    }

    /**
     * Initializes a new instance of the <see cref="MultiShardConnection"/> class.
     *
     * @param shardLocations
     *            The collection of <see cref="ShardLocation"/>s used for this connection instances.
     * @param connectionString
     *            These credentials will be used to connect to the <see cref="Shard"/>s. The same credentials are used on all shards. Therefore, all
     *            shards need to provide the appropriate permissions for these credentials to execute the command.
     *
     *
     *            Multiple Active Result Sets (MARS) are not supported and are disabled for any processing at the shards.
     */
    public MultiShardConnection(String connectionString,
            ShardLocation... shardLocations) {
        if (StringUtilsLocal.isNullOrEmpty(connectionString)) {
            throw new IllegalArgumentException("connectionString");
        }

        // Enhance the ApplicationName with this library's name as a suffix
        // Devnote: If connection string specifies Active Directory authentication and runtime is not
        // .NET 4.6 or higher, then below call will throw.
        SqlConnectionStringBuilder connectionStringBuilder = (new SqlConnectionStringBuilder(connectionString))
                .withApplicationNameSuffix(ApplicationNameSuffix);
        List<ShardLocation> shardLocationList = Arrays.asList(shardLocations);
        validateConnectionArguments(shardLocationList, "shardLocations", connectionStringBuilder);

        List<Pair<ShardLocation, Connection>> connForLocation = shardLocationList.stream().map(s -> {
            try {
                return (createDbConnectionForLocation(s, connectionStringBuilder));
            }
            catch (SQLException e) {
                throw new RuntimeException(e.getMessage(), e);
            }
        }).collect(Collectors.toList());

        this.connectionString = connectionString;
        this.shardConnections = connForLocation;
        this.shards = null;
    }

    /**
     * Creates an instance of this class. TEST ONLY
     *
     * @param shardConnections
     *            Connections to the shards
     */
    public MultiShardConnection(ArrayList<Pair<ShardLocation, Connection>> shardConnections) {
        if (shardConnections == null || shardConnections.size() == 0) {
            throw new IllegalArgumentException("connections");
        }
        try {
            this.connectionString = shardConnections.get(0).getRight().getMetaData().getURL();
        }
        catch (SQLException e) {
            throw new IllegalArgumentException("connectionString");
        }
        this.shards = null;
        this.shardConnections = shardConnections;
    }

    private static <T> void validateConnectionArguments(List<T> namedCollection,
            String collectionName,
            SqlConnectionStringBuilder connectionStringBuilder) {
        if (namedCollection == null) {
            throw new IllegalArgumentException(collectionName);
        }

        if (0 == namedCollection.size()) {
            throw new IllegalArgumentException(String.format("No %1$s provided.", collectionName));
        }

        // Datasource must not be set
        if (!StringUtilsLocal.isNullOrEmpty(connectionStringBuilder.getDataSource())) {
            throw new IllegalArgumentException("DataSource must not be set in the connectionStringBuilder");
        }

        // Initial catalog must not be set
        if (!StringUtilsLocal.isNullOrEmpty(connectionStringBuilder.getDataSource())) {
            throw new IllegalArgumentException("InitialCatalog must not be set in the connectionStringBuilder");
        }
    }

    private static Pair<ShardLocation, Connection> createDbConnectionForLocation(ShardLocation shardLocation,
            SqlConnectionStringBuilder connStr) throws SQLException {
        connStr.setDatabaseName(shardLocation.getDatabase());
        connStr.setDataSource(shardLocation.getDataSource());
        return new ImmutablePair<>(shardLocation, DriverManager.getConnection(connStr.getConnectionString()));
    }

    /**
     * Gets the collection of <see cref="Shard"/>s associated with this connection.
     */
    public List<Shard> getShards() {
        return shards;
    }

    /**
     * Gets the collection of <see cref="ShardLocation"/>s associated with this connection.
     */
    public List<ShardLocation> getShardLocations() {
        return this.getShardConnections().stream().map(Pair::getLeft).collect(Collectors.toList());
    }

    /**
     * Gets the collection of ShardLocations and Connections associated with this connection.
     */
    public List<Pair<ShardLocation, Connection>> getShardConnections() {
        return shardConnections;
    }

    /**
     * Gets the connection string associated with this connection.
     */
    public String getConnectionString() {
        return this.connectionString;
    }

    /**
     * Creates and returns a <see cref="MultiShardStatement"/> object. The <see cref="MultiShardStatement"/> object can then be used to execute a
     * command against all shards specified in the connection.
     *
     * @return the <see cref="MultiShardStatement"/> with CommandText set to null.
     */
    public MultiShardStatement createCommand() {
        return MultiShardStatement.create(this, null);
    }

    /**
     * Releases all resources used by this object.
     */
    public void close() throws IOException {
        if (!isDisposed) {
            // Dispose off the shard connections
            this.getShardConnections().forEach((c) -> {
                if (c.getRight() != null) {
                    try {
                        c.getRight().close();
                    }
                    catch (SQLException e) {
                        e.printStackTrace();
                    }
                }
            });

            isDisposed = true;

            log.warn("MultiShardConnection.close - Connection was disposed");
        }
    }

    /**
     * Closes any open connections to shards. Does a best-effort close and doesn't throw.
     */
    public void closeOpenConnections() {
        for (Pair<ShardLocation, Connection> conn : this.getShardConnections()) {
            try {
                if (conn.getRight() != null && !conn.getRight().isClosed()) {
                    conn.getRight().close();
                }
            }
            catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * Check if connection is closed or not.
     *
     * @return true if open, false if closed.
     */
    public boolean isClosed() {
        return isDisposed;
    }
}

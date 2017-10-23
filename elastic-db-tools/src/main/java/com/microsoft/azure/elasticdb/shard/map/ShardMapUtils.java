package com.microsoft.azure.elasticdb.shard.map;

/*
 * Elastic database tools for Azure SQL Database.
 * 
 * Copyright(c) Microsoft Corporation All rights reserved.
 * 
 * This program is made available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

import com.microsoft.azure.elasticdb.shard.mapmanager.ShardMapManager;
import com.microsoft.azure.elasticdb.shard.sqlstore.SqlConnectionStringBuilder;
import com.microsoft.azure.elasticdb.shard.store.StoreShardMap;

/**
 * Helper methods related to shard map instantiation.
 */
public final class ShardMapUtils {

    /**
     * SqlConnectionStringBuilder property that allows one to specify the number of reconnect attempts on connection failure.
     */
    public static final String ConnectRetryCount = "ConnectRetryCount";

    /**
     * SqlConnectionStringBuilder property that allows specifying active directoty authentication to connect to SQL instance.
     */
    public static final String Authentication = "Authentication";

    /**
     * String representation of SqlAuthenticationMethod.ActiveDirectoryIntegrated SqlAuthenticationMethod.ActiveDirectoryIntegrated.ToString() cannot
     * be used because it may not be available in the .NET framework version that we are running in
     */
    public static final String ActiveDirectoryIntegratedStr = "ActiveDirectoryIntegrated";

    /**
     * Whether this SqlClient instance supports Connection Resiliency.
     */
    private static boolean IsConnectionResiliencySupported;

    static {
        // Connection resiliency is supported if this SqlClient instance
        // allows setting the retry count on connection failure
        SqlConnectionStringBuilder bldr = new SqlConnectionStringBuilder();
        if (bldr.containsKey(ConnectRetryCount)) {
            setIsConnectionResiliencySupported(true);
        }
    }

    public static boolean getIsConnectionResiliencySupported() {
        return IsConnectionResiliencySupported;
    }

    public static void setIsConnectionResiliencySupported(boolean value) {
        IsConnectionResiliencySupported = value;
    }

    /**
     * Converts StoreShardMap to ShardMap.
     *
     * @param shardMapManager
     *            Reference to shard map manager.
     * @param ssm
     *            Storage representation for ShardMap.
     * @return ShardMap object corresponding to storange representation.
     */
    public static ShardMap createShardMapFromStoreShardMap(ShardMapManager shardMapManager,
            StoreShardMap ssm) {
        switch (ssm.getMapType()) {
            case List:
                // Create ListShardMap<KeyT>
                return new ListShardMap<>(shardMapManager, ssm);
            case Range:
                // Create RangeShardMap<KeyT>
                return new RangeShardMap<>(shardMapManager, ssm);
            default:
                return null;
        }
    }
}
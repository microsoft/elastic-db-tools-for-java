package com.microsoft.azure.elasticdb.shard.map;

// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

import com.microsoft.azure.elasticdb.shard.mapmanager.ShardMapManager;
import com.microsoft.azure.elasticdb.shard.store.IStoreShardMap;

/**
 * Helper methods related to shard map instantiation.
 */
public final class ShardMapUtils {
    /**
     * SqlConnectionStringBuilder property that allows one
     * to specify the number of reconnect attempts on connection failure
     */
    public static final String ConnectRetryCount = "ConnectRetryCount";

    /**
     * SqlConnectionStringBuilder property that allows specifying
     * active directoty authentication to connect to SQL instance.
     */
    public static final String Authentication = "Authentication";

    /**
     * String representation of SqlAuthenticationMethod.ActiveDirectoryIntegrated
     * SqlAuthenticationMethod.ActiveDirectoryIntegrated.ToString() cannot be used
     * because it may not be available in the .NET framework version that we are running in
     */
    public static final String ActiveDirectoryIntegratedStr = "ActiveDirectoryIntegrated";
    /**
     * Whether this SqlClient instance supports Connection Resiliency
     */
    private static boolean IsConnectionResiliencySupported;

    static {
        // Connection resiliency is supported if this SqlClient instance
        // allows setting the retry count on connection failure
        SqlConnectionStringBuilder bldr = new SqlConnectionStringBuilder();
        if (bldr.ContainsKey(ConnectRetryCount)) {
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
     * Converts IStoreShardMap to ShardMap.
     *
     * @param manager Reference to shard map manager.
     * @param ssm     Storage representation for ShardMap.
     * @return ShardMap object corresponding to storange representation.
     */
    public static ShardMap CreateShardMapFromStoreShardMap(ShardMapManager manager, IStoreShardMap ssm) {
        switch (ssm.getMapType()) {
            case ShardMapType.List:
                // Create ListShardMap<TKey>
                return (ShardMap) Activator.CreateInstance(ListShardMap<>.class.MakeGenericType(ShardKey.TypeFromShardKeyType(ssm.KeyType)), BindingFlags.NonPublic.getValue() | BindingFlags.Instance.getValue(), null, new Object[]{manager, ssm}, CultureInfo.InvariantCulture);

            default:
                assert ssm.getMapType() == ShardMapType.Range;
                // Create RangeShardMap<TKey>
                return (ShardMap) Activator.CreateInstance(RangeShardMap<>.class.MakeGenericType(ShardKey.TypeFromShardKeyType(ssm.KeyType)), BindingFlags.NonPublic.getValue() | BindingFlags.Instance.getValue(), null, new Object[]{manager, ssm}, CultureInfo.InvariantCulture);
        }
    }
}
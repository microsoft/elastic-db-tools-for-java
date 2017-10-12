package com.microsoft.azure.elasticdb.shard.map;

/*
 * Elastic database tools for Azure SQL Database.
 * 
 * Copyright(c) Microsoft Corporation All rights reserved.
 * 
 * This program is made available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

import com.microsoft.azure.elasticdb.shard.base.ShardKey;
import com.microsoft.azure.elasticdb.shard.base.ShardKeyType;
import com.microsoft.azure.elasticdb.shard.mapmanager.ShardManagementErrorCategory;
import com.microsoft.azure.elasticdb.shard.mapmanager.ShardManagementErrorCode;
import com.microsoft.azure.elasticdb.shard.mapmanager.ShardManagementException;
import com.microsoft.azure.elasticdb.shard.store.StoreShardMap;
import com.microsoft.azure.elasticdb.shard.utils.Errors;
import com.microsoft.azure.elasticdb.shard.utils.ExceptionUtils;

/**
 * Extension methods on ShardMaps that allow down-casting.
 */
public final class ShardMapExtensions {

    /**
     * Downcasts to ListShardMap of KeyT. <typeparam name="KeyT">Key type.</typeparam>
     *
     * @param shardMap
     *            Input shard map.
     * @return ListShardMap representation of this object.
     */
    public static <KeyT> ListShardMap<KeyT> asListShardMap(ShardMap shardMap) {
        ExceptionUtils.disallowNullArgument(shardMap, "shardMap");

        return ShardMapExtensions.asListShardMap(shardMap, true);
    }

    /**
     * Downcasts to ListShardMap of KeyT. <typeparam name="KeyT">Key type.</typeparam>
     *
     * @param shardMap
     *            Input shard map.
     * @param throwOnFailure
     *            Whether to throw exception or return null on failure.
     * @return ListShardMap representation of this object.
     */
    public static <KeyT> ListShardMap<KeyT> asListShardMap(ShardMap shardMap,
            boolean throwOnFailure) {
        ListShardMap<KeyT> lsm = null;

        if (shardMap != null && shardMap.getMapType() == ShardMapType.List) {
            lsm = (ListShardMap<KeyT>) ((shardMap instanceof ListShardMap) ? shardMap : null);
        }

        if (lsm == null && throwOnFailure) {
            throw ShardMapExtensions.getConversionException(shardMap.getStoreShardMap(), "Unknown", "List");
        }

        return lsm;
    }

    /**
     * Downcasts to RangeShardMap of KeyT. <typeparam name="KeyT">Key type.</typeparam>
     *
     * @param shardMap
     *            Input shard map.
     * @return RangeShardMap representation of this object.
     */
    public static <KeyT> RangeShardMap<KeyT> asRangeShardMap(ShardMap shardMap) {
        ExceptionUtils.disallowNullArgument(shardMap, "shardMap");

        return ShardMapExtensions.asRangeShardMap(shardMap, true);
    }

    /**
     * Downcasts to RangeShardMap of KeyT. <typeparam name="KeyT">Key type.</typeparam>
     *
     * @param shardMap
     *            Input shard map.
     * @param throwOnFailure
     *            Whether to throw exception or return null on failure.
     * @return RangeShardMap representation of this object.
     */
    public static <KeyT> RangeShardMap<KeyT> asRangeShardMap(ShardMap shardMap,
            boolean throwOnFailure) {
        RangeShardMap<KeyT> rsm = null;

        if (shardMap != null && shardMap.getMapType() == ShardMapType.Range) {
            rsm = (RangeShardMap<KeyT>) ((shardMap instanceof RangeShardMap) ? shardMap : null);
        }

        if (rsm == null && throwOnFailure) {
            throw ShardMapExtensions.getConversionException(shardMap.getStoreShardMap(), "Unknown", "Range");
        }

        return rsm;
    }

    /**
     * Raise conversion exception. <typeparam name="KeyT">Key type.</typeparam>
     *
     * @param ssm
     *            Shard map whose conversion failed.
     * @param targetKind
     *            Requested type of shard map.
     */
    public static ShardManagementException getConversionException(StoreShardMap ssm,
            String sourceKind,
            String targetKind) {
        return new ShardManagementException(ShardManagementErrorCategory.ShardMapManager, ShardManagementErrorCode.ShardMapTypeConversionError,
                Errors._ShardMapExtensions_AsTypedShardMap_ConversionFailure, ssm.getName(), targetKind, sourceKind, ssm.getMapType().toString(),
                ssm.getKeyType() == ShardKeyType.None ? "" : ShardKey.typeFromShardKeyType(ssm.getKeyType()).getName());
    }
}
package com.microsoft.azure.elasticdb.shard.map;

// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

import com.microsoft.azure.elasticdb.shard.mapmanager.ShardManagementException;
import com.microsoft.azure.elasticdb.shard.store.StoreShardMap;
import com.microsoft.azure.elasticdb.shard.utils.ExceptionUtils;

/**
 * Extension methods on ShardMaps that allow down-casting.
 */
public final class ShardMapExtensions {

  /**
   * Downcasts to ListShardMap of TKey.
   * <p>
   * <typeparam name="TKey">Key type.</typeparam>
   *
   * @param shardMap Input shard map.
   * @return ListShardMap representation of this object.
   */
  public static <TKey> ListShardMap<TKey> AsListShardMap(ShardMap shardMap) {
    ExceptionUtils.DisallowNullArgument(shardMap, "shardMap");

    return ShardMapExtensions.<TKey>AsListShardMap(shardMap, true);
  }

  /**
   * Downcasts to ListShardMap of TKey.
   * <p>
   * <typeparam name="TKey">Key type.</typeparam>
   *
   * @param shardMap Input shard map.
   * @param throwOnFailure Whether to throw exception or return null on failure.
   * @return ListShardMap representation of this object.
   */
  public static <TKey> ListShardMap<TKey> AsListShardMap(ShardMap shardMap,
      boolean throwOnFailure) {
    ListShardMap<TKey> lsm = null;

    if (shardMap != null && shardMap.getMapType() == ShardMapType.List) {
      lsm = (ListShardMap<TKey>) ((shardMap instanceof ListShardMap) ? shardMap : null);
    }

    if (lsm == null && throwOnFailure) {
      throw ShardMapExtensions.<TKey>GetConversionException(shardMap.getStoreShardMap(), "List");
    }

    return lsm;
  }


  /**
   * Downcasts to RangeShardMap of TKey.
   * <p>
   * <typeparam name="TKey">Key type.</typeparam>
   *
   * @param shardMap Input shard map.
   * @return RangeShardMap representation of this object.
   */
  public static <TKey> RangeShardMap<TKey> AsRangeShardMap(ShardMap shardMap) {
    ExceptionUtils.DisallowNullArgument(shardMap, "shardMap");

    return ShardMapExtensions.<TKey>AsRangeShardMap(shardMap, true);
  }

  /**
   * Downcasts to RangeShardMap of TKey.
   * <p>
   * <typeparam name="TKey">Key type.</typeparam>
   *
   * @param shardMap Input shard map.
   * @param throwOnFailure Whether to throw exception or return null on failure.
   * @return RangeShardMap representation of this object.
   */
  public static <TKey> RangeShardMap<TKey> AsRangeShardMap(ShardMap shardMap,
      boolean throwOnFailure) {
    RangeShardMap<TKey> rsm = null;

    if (shardMap != null && shardMap.getMapType() == ShardMapType.Range) {
      rsm = (RangeShardMap<TKey>) ((shardMap instanceof RangeShardMap) ? shardMap : null);
    }

    if (rsm == null && throwOnFailure) {
      throw ShardMapExtensions.<TKey>GetConversionException(shardMap.getStoreShardMap(), "Range");
    }

    return rsm;
  }

  /**
   * Raise conversion exception.
   * <p>
   * <typeparam name="TKey">Key type.</typeparam>
   *
   * @param ssm Shard map whose conversion failed.
   * @param targetKind Requested type of shard map.
   */
  private static <TKey> ShardManagementException GetConversionException(StoreShardMap ssm,
      String targetKind) {
    return null; //TODO: new ShardManagementException(ShardManagementErrorCategory.ShardMapManager, ShardManagementErrorCode.ShardMapTypeConversionError, Errors._ShardMapExtensions_AsTypedShardMap_ConversionFailure, ssm.getName(), targetKind, TKey.class.Name, ssm.getMapType().toString(), ssm.getKeyType() == ShardKeyType.None ? "" : ShardKey.TypeFromShardKeyType(ssm.getKeyType()).getName());
  }
}
package com.microsoft.azure.elasticdb.shard.mapper;

/* Copyright (c) Microsoft. All rights reserved.
Licensed under the MIT license. See LICENSE file in the project root for full license information.*/

import com.microsoft.azure.elasticdb.core.commons.helpers.ReferenceObjectHelper;
import com.microsoft.azure.elasticdb.shard.base.LockOwnerIdOpType;
import com.microsoft.azure.elasticdb.shard.base.MappingStatus;
import com.microsoft.azure.elasticdb.shard.base.PointMapping;
import com.microsoft.azure.elasticdb.shard.base.PointMappingUpdate;
import com.microsoft.azure.elasticdb.shard.base.Range;
import com.microsoft.azure.elasticdb.shard.base.Shard;
import com.microsoft.azure.elasticdb.shard.map.ShardMap;
import com.microsoft.azure.elasticdb.shard.mapmanager.ShardManagementErrorCategory;
import com.microsoft.azure.elasticdb.shard.mapmanager.ShardManagementErrorCode;
import com.microsoft.azure.elasticdb.shard.mapmanager.ShardManagementException;
import com.microsoft.azure.elasticdb.shard.mapmanager.ShardMapManager;
import com.microsoft.azure.elasticdb.shard.storeops.base.StoreOperationRequestBuilder;
import com.microsoft.azure.elasticdb.shard.utils.Errors;
import java.sql.Connection;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Callable;

/**
 * Mapper from single keys (points) to their corresponding shards.
 *
 * <typeparam name="KeyT">Key type.</typeparam>
 */
public final class ListShardMapper extends BaseShardMapper implements
    IShardMapper<PointMapping, Object> {

  /**
   * List shard mapper, which managers point mappings.
   *
   * @param shardMapManager Reference to ShardMapManager.
   * @param sm Containing shard map.
   */
  public ListShardMapper(ShardMapManager shardMapManager, ShardMap sm) {
    super(shardMapManager, sm);
  }

  /**
   * Given a key value, obtains a SqlConnection to the shard in the mapping
   * that contains the key value.
   *
   * @param key Input key value.
   * @param connectionString Connection string with credential information, the DataSource and
   * Database are obtained from the results of the lookup operation for key.
   * @return An opened SqlConnection.
   */
  public Connection openConnectionForKey(Object key, String connectionString) {
    return openConnectionForKey(key, connectionString, ConnectionOptions.Validate);
  }

  /**
   * Given a key value, obtains a SqlConnection to the shard in the mapping
   * that contains the key value.
   *
   * @param key Input key value.
   * @param connectionString Connection string with credential information, the DataSource and
   * Database are obtained from the results of the lookup operation for key.
   * @param options Options for validation operations to perform on opened connection.
   * @return An opened SqlConnection.
   */
  public Connection openConnectionForKey(Object key, String connectionString,
      ConnectionOptions options) {
    return this.openConnectionForKey(key, PointMapping::new,
        ShardManagementErrorCategory.ListShardMap, connectionString, options);
  }

  /**
   * Given a key value, asynchronously obtains a SqlConnection to the shard in the mapping
   * that contains the key value.
   *
   * @param key Input key value.
   * @param connectionString Connection string with credential information, the DataSource and
   * Database are obtained from the results of the lookup operation for key.
   * @return A Task encapsulating an opened SqlConnection. All non usage-error exceptions will be
   * reported via the returned Task
   */
  public Callable<Connection> openConnectionForKeyAsync(Object key,
      String connectionString) {
    return openConnectionForKeyAsync(key, connectionString, ConnectionOptions.Validate);
  }

  /**
   * Given a key value, asynchronously obtains a SqlConnection to the shard in the mapping
   * that contains the key value.
   *
   * @param key Input key value.
   * @param connectionString Connection string with credential information, the DataSource and
   * Database are obtained from the results of the lookup operation for key.
   * @param options Options for validation operations to perform on opened connection.
   * @return A Task encapsulating an opened SqlConnection. All non usage-error exceptions will be
   * reported via the returned Task
   */
  public Callable<Connection> openConnectionForKeyAsync(Object key,
      String connectionString, ConnectionOptions options) {
    return this.openConnectionForKeyAsync(key, PointMapping::new,
        ShardManagementErrorCategory.ListShardMap, connectionString, options);
  }

  /**
   * Marks the given mapping offline.
   *
   * @param mapping Input point mapping.
   * @return An offline mapping.
   */
  public PointMapping markMappingOffline(PointMapping mapping) {
    return markMappingOffline(mapping, new UUID(0L, 0L));
  }

  /**
   * Marks the given mapping offline.
   *
   * @param mapping Input point mapping.
   * @param lockOwnerId Lock owner id of this mapping
   * @return An offline mapping.
   */
  public PointMapping markMappingOffline(PointMapping mapping, UUID lockOwnerId) {
    PointMappingUpdate tempVar = new PointMappingUpdate();
    tempVar.setStatus(MappingStatus.Offline);
    return BaseShardMapper.setStatus(mapping, mapping.getStatus(), s -> MappingStatus.Offline,
        s -> tempVar, (mp, tv, lo) -> this.update(mapping, tempVar, lockOwnerId), lockOwnerId);
  }

  /**
   * Marks the given mapping online.
   *
   * @param mapping Input point mapping.
   * @return An online mapping.
   */
  public PointMapping markMappingOnline(PointMapping mapping) {
    return markMappingOnline(mapping, new UUID(0L, 0L));
  }

  /**
   * Marks the given mapping online.
   *
   * @param mapping Input point mapping.
   * @param lockOwnerId Lock owner id of this mapping
   * @return An online mapping.
   */
  public PointMapping markMappingOnline(PointMapping mapping, UUID lockOwnerId) {
    PointMappingUpdate tempVar = new PointMappingUpdate();
    tempVar.setStatus(MappingStatus.Online);
    return BaseShardMapper.setStatus(mapping, mapping.getStatus(), s -> MappingStatus.Online,
        s -> tempVar, (mp, tv, lo) -> this.update(mapping, tempVar, lockOwnerId), lockOwnerId);
  }

  /**
   * Adds a point mapping.
   *
   * @param mapping Mapping being added.
   * @return The added mapping object.
   */
  public PointMapping add(PointMapping mapping) {
    return this.add(mapping, PointMapping::new);
  }

  /**
   * Removes a point mapping.
   *
   * @param mapping Mapping being removed.
   */
  public void remove(PointMapping mapping) {
    remove(mapping, new UUID(0L, 0L));
  }

  /**
   * Removes a point mapping.
   *
   * @param mapping Mapping being removed.
   * @param lockOwnerId Lock owner id of the mapping
   */
  public void remove(PointMapping mapping, UUID lockOwnerId) {
    this.remove(mapping, PointMapping::new, lockOwnerId);
  }

  /**
   * Looks up the key value and returns the corresponding mapping.
   *
   * @param key Input key value.
   * @param useCache Whether to use cache for lookups.
   * @return Mapping that contains the key value.
   */
  public PointMapping lookup(Object key, boolean useCache) {
    PointMapping p = this.lookup(key, useCache, PointMapping::new,
        ShardManagementErrorCategory.ListShardMap);

    if (p == null) {
      throw new ShardManagementException(ShardManagementErrorCategory.ListShardMap,
          ShardManagementErrorCode.MappingNotFoundForKey,
          Errors._Store_ShardMapper_MappingNotFoundForKeyGlobal, this.getShardMap().getName(),
          StoreOperationRequestBuilder.SP_FIND_SHARD_MAPPING_BY_KEY_GLOBAL, "Lookup");
    }

    return p;
  }

  /**
   * Tries to looks up the key value and returns the corresponding mapping.
   *
   * @param key Input key value.
   * @param useCache Whether to use cache for lookups.
   * @param mapping Mapping that contains the key value.
   * @return <c>true</c> if mapping is found, <c>false</c> otherwise.
   */
  public boolean tryLookup(Object key, boolean useCache,
      ReferenceObjectHelper<PointMapping> mapping) {
    PointMapping p = this.lookup(key, useCache, PointMapping::new,
        ShardManagementErrorCategory.ListShardMap);

    mapping.argValue = p;

    return p != null;
  }

  /**
   * Gets all the mappings that exist within given range.
   *
   * @param range Optional range value, if null, we cover everything.
   * @param shard Optional shard parameter, if null, we cover all shards.
   * @return Read-only collection of mappings that overlap with given range.
   */
  public List<PointMapping> getMappingsForRange(Range range, Shard shard) {
    return this.getMappingsForRange(range, shard, PointMapping::new,
        ShardManagementErrorCategory.ListShardMap, "PointMapping");
  }

  /**
   * Allows for update to a point mapping with the updates provided in
   * the <paramref name="update"/> parameter.
   *
   * @param currentMapping Mapping being updated.
   * @param update Updated properties of the Shard.
   * @return New instance of mapping with updated information.
   */
  public PointMapping update(PointMapping currentMapping, PointMappingUpdate update) {
    return update(currentMapping, update, new UUID(0L, 0L));
  }

  /**
   * Allows for update to a point mapping with the updates provided in
   * the <paramref name="update"/> parameter.
   *
   * @param currentMapping Mapping being updated.
   * @param update Updated properties of the Shard.
   * @param lockOwnerId Lock owner id of this mapping
   * @return New instance of mapping with updated information.
   */
  public PointMapping update(PointMapping currentMapping, PointMappingUpdate update,
      UUID lockOwnerId) {
    return this.update(currentMapping, update, PointMapping::new, MappingStatus::getValue,
        i -> (MappingStatus.forValue(i)), lockOwnerId);
  }

  /**
   * Gets the lock owner of a mapping.
   *
   * @param mapping The mapping
   * @return Lock owner for the mapping.
   */
  public UUID getLockOwnerForMapping(PointMapping mapping) {
    return this.getLockOwnerForMapping(mapping, ShardManagementErrorCategory.ListShardMap);
  }

  /**
   * Locks or unlocks a given mapping or all mappings.
   *
   * @param mapping Optional mapping
   * @param lockOwnerId The lock owner id
   * @param lockOwnerIdOpType Operation to perform on this mapping with the given lockOwnerId
   */
  public void lockOrUnlockMappings(PointMapping mapping, UUID lockOwnerId,
      LockOwnerIdOpType lockOwnerIdOpType) {
    this.lockOrUnlockMappings(mapping, lockOwnerId, lockOwnerIdOpType,
        ShardManagementErrorCategory.ListShardMap);
  }
}
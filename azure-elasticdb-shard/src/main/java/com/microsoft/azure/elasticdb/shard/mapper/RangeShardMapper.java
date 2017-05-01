package com.microsoft.azure.elasticdb.shard.mapper;

/* Copyright (c) Microsoft. All rights reserved.
Licensed under the MIT license. See LICENSE file in the project root for full license information.*/

import com.microsoft.azure.elasticdb.core.commons.helpers.ReferenceObjectHelper;
import com.microsoft.azure.elasticdb.shard.base.LockOwnerIdOpType;
import com.microsoft.azure.elasticdb.shard.base.MappingStatus;
import com.microsoft.azure.elasticdb.shard.base.Range;
import com.microsoft.azure.elasticdb.shard.base.RangeMapping;
import com.microsoft.azure.elasticdb.shard.base.RangeMappingUpdate;
import com.microsoft.azure.elasticdb.shard.base.Shard;
import com.microsoft.azure.elasticdb.shard.base.ShardKey;
import com.microsoft.azure.elasticdb.shard.map.ShardMap;
import com.microsoft.azure.elasticdb.shard.mapmanager.ShardManagementErrorCategory;
import com.microsoft.azure.elasticdb.shard.mapmanager.ShardManagementErrorCode;
import com.microsoft.azure.elasticdb.shard.mapmanager.ShardManagementException;
import com.microsoft.azure.elasticdb.shard.mapmanager.ShardMapManager;
import com.microsoft.azure.elasticdb.shard.store.StoreMapping;
import com.microsoft.azure.elasticdb.shard.store.StoreShard;
import com.microsoft.azure.elasticdb.shard.storeops.base.StoreOperationRequestBuilder;
import com.microsoft.azure.elasticdb.shard.utils.Errors;
import com.microsoft.sqlserver.jdbc.SQLServerConnection;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Callable;

/**
 * Mapper from a range of keys to their corresponding shards.
 *
 * <typeparam name="KeyT">Key type.</typeparam>
 */
public class RangeShardMapper extends BaseShardMapper implements
    IShardMapper<RangeMapping, Object> {

  /**
   * Range shard mapper, which managers range mappings.
   *
   * @param shardMapManager Reference to ShardMapManager.
   * @param sm Containing shard map.
   */
  public RangeShardMapper(ShardMapManager shardMapManager, ShardMap sm) {
    super(shardMapManager, sm);
  }

  public final SQLServerConnection openConnectionForKey(Object key, String connectionString) {
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
  public final SQLServerConnection openConnectionForKey(Object key, String connectionString,
      ConnectionOptions options) {
    return this.openConnectionForKey(key, (smm, sm, ssm) -> new RangeMapping(smm, sm, ssm),
        ShardManagementErrorCategory.RangeShardMap, connectionString, options);
  }

  public final Callable<SQLServerConnection> openConnectionForKeyAsync(Object key,
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
   * @return A Task encapsulating an opened SqlConnection.
   */
  public final Callable<SQLServerConnection> openConnectionForKeyAsync(Object key,
      String connectionString, ConnectionOptions options) {
    return this.<RangeMapping, Object>openConnectionForKeyAsync(key,
        (smm, sm, ssm) -> new RangeMapping(smm, sm, ssm),
        ShardManagementErrorCategory.RangeShardMap, connectionString, options);
  }

  /**
   * Marks the given mapping offline.
   *
   * @param mapping Input range mapping.
   * @param lockOwnerId Lock owner id of this mapping
   * @return An offline mapping.
   */
  public final RangeMapping markMappingOffline(RangeMapping mapping, UUID lockOwnerId) {
    RangeMappingUpdate tempVar = new RangeMappingUpdate();
    tempVar.setStatus(MappingStatus.Offline);
    //TODO: Not sure if the below line works. Need to test.
    return BaseShardMapper.<RangeMapping, RangeMappingUpdate, MappingStatus>setStatus(mapping,
        mapping.getStatus(), s -> MappingStatus.Offline, s -> tempVar,
        (mp, tv, lo) -> this.update(mapping, tempVar, lockOwnerId), lockOwnerId);
  }

  /**
   * Marks the given mapping online.
   *
   * @param mapping Input range mapping.
   * @param lockOwnerId Lock owner id of this mapping
   * @return An online mapping.
   */
  public final RangeMapping markMappingOnline(RangeMapping mapping, UUID lockOwnerId) {
    RangeMappingUpdate tempVar = new RangeMappingUpdate();
    tempVar.setStatus(MappingStatus.Online);
    //TODO: Not sure if the below line works. Need to test.
    return BaseShardMapper.<RangeMapping, RangeMappingUpdate, MappingStatus>setStatus(mapping,
        mapping.getStatus(), s -> MappingStatus.Online, s -> tempVar,
        (mp, tv, lo) -> this.update(mapping, tempVar, lockOwnerId), lockOwnerId);
  }

  /**
   * Adds a range mapping.
   *
   * @param mapping Mapping being added.
   * @return The added mapping object.
   */
  public final RangeMapping add(RangeMapping mapping) {
    return this.<RangeMapping>add(mapping, (smm, sm, ssm) -> new RangeMapping(smm, sm, ssm));
  }

  /**
   * Removes a range mapping.
   *
   * @param mapping Mapping being removed.
   * @param lockOwnerId Lock owner id of this mapping
   */
  public final void remove(RangeMapping mapping, UUID lockOwnerId) {
    this.<RangeMapping>remove(mapping, (smm, sm, ssm) -> new RangeMapping(smm, sm, ssm),
        lockOwnerId);
  }

  /**
   * Looks up the key value and returns the corresponding mapping.
   *
   * @param key Input key value.
   * @param useCache Whether to use cache for lookups.
   * @return Mapping that contains the key value.
   */
  public final RangeMapping lookup(Object key, boolean useCache) {
    RangeMapping p = this.lookup(key, useCache, (smm, sm, ssm) -> new RangeMapping(smm, sm, ssm),
        ShardManagementErrorCategory.RangeShardMap);

    if (p == null) {
      throw new ShardManagementException(ShardManagementErrorCategory.RangeShardMap,
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
  public final boolean tryLookup(Object key, boolean useCache,
      ReferenceObjectHelper<RangeMapping> mapping) {
    RangeMapping p = this.lookup(key, useCache, (smm, sm, ssm) -> new RangeMapping(smm, sm, ssm),
        ShardManagementErrorCategory.RangeShardMap);

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
  public final List<RangeMapping> getMappingsForRange(Range range, Shard shard) {
    return getMappingsForRange(range, shard, (smm, sm, ssm) -> new RangeMapping(smm, sm, ssm),
        ShardManagementErrorCategory.RangeShardMap, "RangeMapping");
  }

  /**
   * Allows for update to a range mapping with the updates provided in
   * the <paramref name="update"/> parameter.
   *
   * @param currentMapping Mapping being updated.
   * @param update Updated properties of the Shard.
   * @param lockOwnerId Lock owner id of this mapping
   * @return New instance of mapping with updated information.
   */
  public final RangeMapping update(RangeMapping currentMapping, RangeMappingUpdate update,
      UUID lockOwnerId) {
    return this.<RangeMapping, RangeMappingUpdate, MappingStatus>update(currentMapping, update,
        (smm, sm, ssm) -> new RangeMapping(smm, sm, ssm), rms -> rms.getValue(),
        i -> MappingStatus.forValue(i), lockOwnerId);
  }

  /**
   * Splits the given mapping into 2 at the given key. The new mappings point to the same shard
   * as the existing mapping.
   *
   * @param existingMapping Given existing mapping.
   * @param splitAt Split point.
   * @param lockOwnerId Lock owner id of this mapping
   * @return Read-only collection of 2 new mappings thus created.
   */
  public final List<RangeMapping> split(RangeMapping existingMapping, Object splitAt,
      UUID lockOwnerId) {
    this.<RangeMapping>ensureMappingBelongsToShardMap(existingMapping,
        "Split", "existingMapping");

    ShardKey shardKey = new ShardKey(ShardKey.shardKeyTypeFromType(Object.class), splitAt);

    if (!existingMapping.getRange().contains(shardKey)
        || existingMapping.getRange().getLow().equals(shardKey)
        || existingMapping.getRange().getHigh().equals(shardKey)) {
      throw new IllegalArgumentException("splitAt",
          new Throwable(Errors._ShardMapping_SplitPointOutOfRange));
    }

    StoreShard newShard = new StoreShard(existingMapping.getShard().getStoreShard().getId(),
        UUID.randomUUID(), existingMapping.getShardMapId(),
        existingMapping.getShard().getStoreShard().getLocation(),
        existingMapping.getShard().getStoreShard().getStatus());

    StoreMapping mapping = existingMapping.getStoreMapping();
    StoreMapping mappingToRemove = new StoreMapping(mapping.getId(), mapping.getShardMapId(),
        mapping.getMinValue(), mapping.getMaxValue(), mapping.getStatus(), mapping.getLockOwnerId(),
        newShard);

    StoreMapping[] mappingsToAdd = new StoreMapping[]{
        new StoreMapping(UUID.randomUUID(), newShard.getShardMapId(),
            existingMapping.getRange().getLow().getRawValue(), shardKey.getRawValue(),
            existingMapping.getStatus().getValue(), lockOwnerId, newShard),
        new StoreMapping(UUID.randomUUID(), newShard.getShardMapId(), shardKey.getRawValue(),
            existingMapping.getRange().getHigh().getRawValue(),
            existingMapping.getStatus().getValue(), lockOwnerId, newShard)
    };

    /*try (IStoreOperation op = this.shardMapManager.getStoreOperationFactory()
        .createReplaceMappingsOperation(this.shardMapManager, StoreOperationCode.SplitMapping,
            this.getShardMap().getStoreShardMap(), new Pair<StoreMapping, UUID>[]{
                new Pair<StoreMapping, UUID>(mappingToRemove, lockOwnerId)}, mappingsToAdd
                .Select(mappingToAdd -> new Pair<StoreMapping, UUID>(mappingToAdd, lockOwnerId))
                .ToArray())) {
      op.doOperation();
    }

    return mappingsToAdd.Select(m -> new RangeMapping(this.shardMapManager, this.ShardMap, m))
        .ToList().AsReadOnly();*/
    return null; //TODO
  }

  /**
   * Merges 2 contiguous mappings into a single mapping. Both left and right mappings should point
   * to the same location and must be contiguous.
   *
   * @param left Left mapping.
   * @param right Right mapping.
   * @param leftLockOwnerId Lock owner id of the left mapping
   * @param rightLockOwnerId Lock owner id of the right mapping
   * @return Mapping that results from the merge operation.
   */
  public final RangeMapping merge(RangeMapping left, RangeMapping right, UUID leftLockOwnerId,
      UUID rightLockOwnerId) {
    /*this.<RangeMapping>EnsureMappingBelongsToShardMap(left, "Merge", "left");
    this.<RangeMapping>EnsureMappingBelongsToShardMap(right, "Merge", "right");

    if (!left.Shard.getLocation().equals(right.Shard.getLocation())) {
      throw new IllegalArgumentException(StringUtilsLocal
          .FormatInvariant(Errors._ShardMapping_MergeDifferentShards, this.ShardMap.Name,
              left.Shard.getLocation(), right.Shard.getLocation()), "left");
    }

    if (left.Range.intersects(right.Range) || left.Range.High != right.Range.Low) {
      throw new IllegalArgumentException("left", Errors._ShardMapping_MergeNotAdjacent);
    }

    if (left.Status != right.Status) {
      throw new IllegalArgumentException(StringUtilsLocal
          .FormatInvariant(Errors._ShardMapping_DifferentStatus, this.ShardMap.Name), "left");
    }

    StoreShard newShard = new DefaultStoreShard(left.Shard.getStoreShard().Id, UUID.randomUUID(),
        left.Shard.getStoreShard().ShardMapId, left.Shard.getStoreShard().getLocation(),
        left.Shard.getStoreShard().Status);

    StoreMapping mappingToRemoveLeft = new StoreMapping(left.StoreMapping.Id,
        left.StoreMapping.ShardMapId, newShard, left.StoreMapping.MinValue,
        left.StoreMapping.MaxValue, left.StoreMapping.Status, left.StoreMapping.LockOwnerId);

    StoreMapping mappingToRemoveRight = new StoreMapping(right.StoreMapping.Id,
        right.StoreMapping.ShardMapId, newShard, right.StoreMapping.MinValue,
        right.StoreMapping.MaxValue, right.StoreMapping.Status, right.StoreMapping.LockOwnerId);

    StoreMapping mappingToAdd = new StoreMapping(UUID.randomUUID(), newShard.ShardMapId, newShard,
        left.Range.Low.RawValue, right.Range.getHigh().getRawValue(), (int) left.Status,
        leftLockOwnerId);

    try (IStoreOperation op = this.shardMapManager.getStoreOperationFactory()
        .CreateReplaceMappingsOperation(this.shardMapManager, StoreOperationCode.MergeMappings,
            this.ShardMap.StoreShardMap, new Pair<StoreMapping, UUID>[]{
                new Pair<StoreMapping, UUID>(mappingToRemoveLeft, leftLockOwnerId),
                new Pair<StoreMapping, UUID>(mappingToRemoveRight, rightLockOwnerId)
            }, new Pair<StoreMapping, UUID>[]{
                new Pair<StoreMapping, UUID>(mappingToAdd, leftLockOwnerId)})) {
      op.Do();
    }

    return new RangeMapping(this.shardMapManager, this.ShardMap, mappingToAdd);*/
    return null; //TODO
  }

  /**
   * Gets the lock owner of a mapping.
   *
   * @param mapping The mapping
   * @return Lock owner for the mapping.
   */
  public final UUID getLockOwnerForMapping(RangeMapping mapping) {
    return this.<RangeMapping>getLockOwnerForMapping(mapping,
        ShardManagementErrorCategory.RangeShardMap);
  }

  /**
   * Locks or unlocks a given mapping or all mappings.
   *
   * @param mapping Optional mapping
   * @param lockOwnerId The lock onwer id
   * @param lockOwnerIdOpType Operation to perform on this mapping with the given lockOwnerId
   */
  public final void lockOrUnlockMappings(RangeMapping mapping, UUID lockOwnerId,
      LockOwnerIdOpType lockOwnerIdOpType) {
    this.<RangeMapping>lockOrUnlockMappings(mapping, lockOwnerId, lockOwnerIdOpType,
        ShardManagementErrorCategory.RangeShardMap);
  }
}

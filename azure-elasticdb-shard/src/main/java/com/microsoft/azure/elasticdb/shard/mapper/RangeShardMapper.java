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
import com.microsoft.azure.elasticdb.shard.base.ShardRange;
import com.microsoft.azure.elasticdb.shard.map.ShardMap;
import com.microsoft.azure.elasticdb.shard.mapmanager.ShardManagementErrorCategory;
import com.microsoft.azure.elasticdb.shard.mapmanager.ShardManagementErrorCode;
import com.microsoft.azure.elasticdb.shard.mapmanager.ShardManagementException;
import com.microsoft.azure.elasticdb.shard.mapmanager.ShardMapManager;
import com.microsoft.azure.elasticdb.shard.store.StoreMapping;
import com.microsoft.azure.elasticdb.shard.store.StoreShard;
import com.microsoft.azure.elasticdb.shard.storeops.base.IStoreOperation;
import com.microsoft.azure.elasticdb.shard.storeops.base.StoreOperationCode;
import com.microsoft.azure.elasticdb.shard.storeops.base.StoreOperationRequestBuilder;
import com.microsoft.azure.elasticdb.shard.utils.Errors;
import com.microsoft.azure.elasticdb.shard.utils.ExceptionUtils;
import com.microsoft.azure.elasticdb.shard.utils.StringUtilsLocal;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.stream.Collectors;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.MutablePair;
import org.apache.commons.lang3.tuple.Pair;

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

  public final Connection openConnectionForKey(Object key, String connectionString) {
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
  public final Connection openConnectionForKey(Object key, String connectionString,
      ConnectionOptions options) {
    return this.openConnectionForKey(key, RangeMapping::new,
        ShardManagementErrorCategory.RangeShardMap, connectionString, options);
  }

  public final Callable<Connection> openConnectionForKeyAsync(Object key,
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
  public final Callable<Connection> openConnectionForKeyAsync(Object key,
      String connectionString, ConnectionOptions options) {
    return this.openConnectionForKeyAsync(key, RangeMapping::new,
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
    return BaseShardMapper.setStatus(mapping, mapping.getStatus(), s -> MappingStatus.Offline,
        s -> tempVar, (mp, tv, lo) -> this.update(mapping, tempVar, lockOwnerId), lockOwnerId);
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
    return BaseShardMapper.setStatus(mapping, mapping.getStatus(), s -> MappingStatus.Online,
        s -> tempVar, (mp, tv, lo) -> this.update(mapping, tempVar, lockOwnerId), lockOwnerId);
  }

  /**
   * Adds a range mapping.
   *
   * @param mapping Mapping being added.
   * @return The added mapping object.
   */
  public final RangeMapping add(RangeMapping mapping) {
    return this.add(mapping, RangeMapping::new);
  }

  /**
   * Removes a range mapping.
   *
   * @param mapping Mapping being removed.
   * @param lockOwnerId Lock owner id of this mapping
   */
  public final void remove(RangeMapping mapping, UUID lockOwnerId) {
    this.remove(mapping, RangeMapping::new, lockOwnerId);
  }

  /**
   * Looks up the key value and returns the corresponding mapping.
   *
   * @param key Input key value.
   * @param useCache Whether to use cache for lookups.
   * @return Mapping that contains the key value.
   */
  public final RangeMapping lookup(Object key, boolean useCache) {
    RangeMapping p = this.lookup(key, useCache, RangeMapping::new,
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
    RangeMapping p = this.lookup(key, useCache, RangeMapping::new,
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
    return getMappingsForRange(range, shard, RangeMapping::new,
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
    return this.update(currentMapping, update,
        RangeMapping::new, MappingStatus::getValue,
        MappingStatus::forValue, lockOwnerId);
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
    this.ensureMappingBelongsToShardMap(existingMapping, "Split", "existingMapping");

    ShardKey shardKey = new ShardKey(ShardKey.shardKeyTypeFromType(splitAt.getClass()), splitAt);

    ShardRange r = existingMapping.getRange();
    if (!r.contains(shardKey) || r.getLow().equals(shardKey) || r.getHigh().equals(shardKey)) {
      throw new IllegalArgumentException("splitAt",
          new Throwable(Errors._ShardMapping_SplitPointOutOfRange));
    }

    StoreShard shard = existingMapping.getShard().getStoreShard();
    StoreShard newShard = new StoreShard(shard.getId(), UUID.randomUUID(),
        existingMapping.getShardMapId(), shard.getLocation(), shard.getStatus());

    StoreMapping mapping = existingMapping.getStoreMapping();
    StoreMapping mappingToRemove = new StoreMapping(mapping.getId(), mapping.getShardMapId(),
        mapping.getMinValue(), mapping.getMaxValue(), mapping.getStatus(), mapping.getLockOwnerId(),
        newShard);

    List<StoreMapping> mappingsToAdd = new ArrayList<>();
    mappingsToAdd.add(new StoreMapping(UUID.randomUUID(), newShard.getShardMapId(),
        existingMapping.getRange().getLow().getRawValue(), shardKey.getRawValue(),
        existingMapping.getStatus().getValue(), lockOwnerId, newShard));
    mappingsToAdd.add(new StoreMapping(UUID.randomUUID(), newShard.getShardMapId(),
        shardKey.getRawValue(), existingMapping.getRange().getHigh().getRawValue(),
        existingMapping.getStatus().getValue(), lockOwnerId, newShard)
    );

    List<Pair<StoreMapping, UUID>> listPair = new ArrayList<>();
    listPair.add(new MutablePair<>(mappingToRemove, lockOwnerId));
    try (IStoreOperation op = this.shardMapManager.getStoreOperationFactory()
        .createReplaceMappingsOperation(this.shardMapManager, StoreOperationCode.SplitMapping,
            this.getShardMap().getStoreShardMap(), listPair, mappingsToAdd.stream()
                .map(mappingToAdd -> new ImmutablePair<>(mappingToAdd, lockOwnerId))
                .collect(Collectors.toList()))) {
      op.doOperation();
    } catch (Exception e) {
      e.printStackTrace();
      ExceptionUtils.throwShardManagementOrStoreException(e);
    }

    return Collections.unmodifiableList(mappingsToAdd.stream()
        .map(m -> new RangeMapping(this.shardMapManager, this.shardMap, m))
        .collect(Collectors.toList()));
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
    this.ensureMappingBelongsToShardMap(left, "Merge", "left");
    this.ensureMappingBelongsToShardMap(right, "Merge", "right");

    if (!left.getShard().getLocation().equals(right.getShard().getLocation())) {
      throw new IllegalArgumentException(StringUtilsLocal.formatInvariant(
          Errors._ShardMapping_MergeDifferentShards, this.getShardMap().getName(),
          left.getShard().getLocation(), right.getShard().getLocation()));
    }

    if (left.getRange().intersects(right.getRange()) || left.getRange().getHigh() != right
        .getRange().getLow()) {
      throw new IllegalArgumentException(Errors._ShardMapping_MergeNotAdjacent);
    }

    if (left.getStatus() != right.getStatus()) {
      throw new IllegalArgumentException(StringUtilsLocal.formatInvariant(
          Errors._ShardMapping_DifferentStatus, this.getShardMap().getName()));
    }

    StoreShard ss = left.getShard().getStoreShard();
    StoreShard newShard = new StoreShard(ss.getId(), UUID.randomUUID(), ss.getShardMapId(),
        ss.getLocation(), ss.getStatus());

    StoreMapping leftMap = left.getStoreMapping();
    StoreMapping mappingToRemoveLeft = new StoreMapping(leftMap.getId(), leftMap.getShardMapId(),
        leftMap.getMinValue(), leftMap.getMaxValue(), leftMap.getStatus(), leftMap.getLockOwnerId(),
        newShard);

    StoreMapping rightMap = right.getStoreMapping();
    StoreMapping mappingToRemoveRight = new StoreMapping(rightMap.getId(), rightMap.getShardMapId(),
        rightMap.getMinValue(), rightMap.getMaxValue(), rightMap.getStatus(),
        rightMap.getLockOwnerId(), newShard);

    StoreMapping mappingToAdd = new StoreMapping(UUID.randomUUID(), newShard.getShardMapId(),
        left.getRange().getLow().getRawValue(), right.getRange().getHigh().getRawValue(),
        left.getStatus().getValue(), leftLockOwnerId, newShard);

    List<Pair<StoreMapping, UUID>> listPairRemove = new ArrayList<>();
    listPairRemove.add(new ImmutablePair<>(mappingToRemoveLeft, leftLockOwnerId));
    listPairRemove.add(new ImmutablePair<>(mappingToRemoveRight, rightLockOwnerId));

    List<Pair<StoreMapping, UUID>> listPairAdd = new ArrayList<>();
    listPairAdd.add(new ImmutablePair<>(mappingToAdd, leftLockOwnerId));

    try (IStoreOperation op = this.shardMapManager.getStoreOperationFactory()
        .createReplaceMappingsOperation(this.shardMapManager, StoreOperationCode.MergeMappings,
            this.shardMap.getStoreShardMap(), listPairRemove, listPairAdd)) {
      op.doOperation();
    } catch (Exception e) {
      e.printStackTrace();
      throw (ShardManagementException) e.getCause();
    }

    return new RangeMapping(this.shardMapManager, this.shardMap, mappingToAdd);
  }

  /**
   * Gets the lock owner of a mapping.
   *
   * @param mapping The mapping
   * @return Lock owner for the mapping.
   */
  public final UUID getLockOwnerForMapping(RangeMapping mapping) {
    return this.getLockOwnerForMapping(mapping,
        ShardManagementErrorCategory.RangeShardMap);
  }

  /**
   * Locks or unlocks a given mapping or all mappings.
   *
   * @param mapping Optional mapping
   * @param lockOwnerId The lock owner id
   * @param lockOwnerIdOpType Operation to perform on this mapping with the given lockOwnerId
   */
  public final void lockOrUnlockMappings(RangeMapping mapping, UUID lockOwnerId,
      LockOwnerIdOpType lockOwnerIdOpType) {
    this.lockOrUnlockMappings(mapping, lockOwnerId, lockOwnerIdOpType,
        ShardManagementErrorCategory.RangeShardMap);
  }
}

package com.microsoft.azure.elasticdb.shard.map;

/* Copyright (c) Microsoft. All rights reserved.
Licensed under the MIT license. See LICENSE file in the project root for full license information.*/

import com.google.common.base.Stopwatch;
import com.microsoft.azure.elasticdb.core.commons.helpers.ReferenceObjectHelper;
import com.microsoft.azure.elasticdb.core.commons.logging.ActivityIdScope;
import com.microsoft.azure.elasticdb.shard.base.LockOwnerIdOpType;
import com.microsoft.azure.elasticdb.shard.base.MappingLockToken;
import com.microsoft.azure.elasticdb.shard.base.MappingStatus;
import com.microsoft.azure.elasticdb.shard.base.PointMapping;
import com.microsoft.azure.elasticdb.shard.base.PointMappingCreationInfo;
import com.microsoft.azure.elasticdb.shard.base.PointMappingUpdate;
import com.microsoft.azure.elasticdb.shard.base.Range;
import com.microsoft.azure.elasticdb.shard.base.Shard;
import com.microsoft.azure.elasticdb.shard.mapmanager.ShardMapManager;
import com.microsoft.azure.elasticdb.shard.mapper.IShardMapper;
import com.microsoft.azure.elasticdb.shard.mapper.ListShardMapper;
import com.microsoft.azure.elasticdb.shard.store.StoreShardMap;
import com.microsoft.azure.elasticdb.shard.utils.ExceptionUtils;
import java.lang.invoke.MethodHandles;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Represents a shard map of points where points are of the specified key.
 * <typeparam name="TKey">Key type.</typeparam>
 */
public final class ListShardMap<TKey> extends ShardMap implements Cloneable {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  /**
   * Mapper b/w points and shards.
   */
  private ListShardMapper lsm;

  /**
   * Constructs a new instance.
   *
   * @param shardMapManager Reference to ShardMapManager.
   * @param ssm Storage representation.
   */
  public ListShardMap(ShardMapManager shardMapManager, StoreShardMap ssm) {
    super(shardMapManager, ssm);
    lsm = new ListShardMapper(shardMapManager, this);
  }

  /**
   * Creates and adds a point mapping to ShardMap.
   *
   * @param creationInfo Information about mapping to be added.
   * @return Newly created mapping.
   */
  public PointMapping createPointMapping(PointMappingCreationInfo creationInfo) {
    ExceptionUtils.DisallowNullArgument(creationInfo, "args");

    try (ActivityIdScope activityIdScope = new ActivityIdScope(UUID.randomUUID())) {
      Stopwatch stopwatch = Stopwatch.createStarted();

      String mappingKey = creationInfo.getKey().getRawValue().toString();
      log.info("CreatePointMapping Start; ShardMap name: {}; Point Mapping: {} ", this.getName(),
          mappingKey);

      PointMapping pointMapping = lsm
          .Add(new PointMapping(this.getShardMapManager(), creationInfo));

      stopwatch.stop();

      log.info("CreatePointMapping Complete; ShardMap name: {}; Point Mapping: {}; Duration: {}",
          this.getName(), mappingKey, stopwatch.elapsed(TimeUnit.MILLISECONDS));

      return pointMapping;
    }
  }

  /**
   * Creates and adds a point mapping to ShardMap.
   *
   * @param point Point for which to create the mapping.
   * @param shard Shard associated with the point mapping.
   * @return Newly created mapping.
   */
  public PointMapping createPointMapping(TKey point, Shard shard) {
    ExceptionUtils.DisallowNullArgument(shard, "shard");

    try (ActivityIdScope activityIdScope = new ActivityIdScope(UUID.randomUUID())) {
      PointMappingCreationInfo args = new PointMappingCreationInfo(point, shard,
          MappingStatus.Online);

      String mappingKey = args.getKey().toString();
      log.info("CreatePointMapping Start; ShardMap name: {}; Point Mapping: {}", this.getName(),
          mappingKey);

      Stopwatch stopwatch = Stopwatch.createStarted();

      PointMapping pointMapping = lsm.Add(new PointMapping(this.getShardMapManager(), args));

      stopwatch.stop();

      log.info("CreatePointMapping Complete; ShardMap name: {}; Point Mapping: {}; Duration: {}",
          this.getName(), mappingKey, stopwatch.elapsed(TimeUnit.MILLISECONDS));

      return pointMapping;
    }
  }

  /**
   * Removes a point mapping.
   *
   * @param mapping Mapping being removed.
   */
  public void deleteMapping(PointMapping mapping) {
    ExceptionUtils.DisallowNullArgument(mapping, "mapping");

    try (ActivityIdScope activityIdScope = new ActivityIdScope(UUID.randomUUID())) {
      String mappingKey = mapping.getKey().getRawValue().toString();
      log.info("DeletePointMapping Start; ShardMap name: {}; Point Mapping: {}", this.getName(),
          mappingKey);

      Stopwatch stopwatch = Stopwatch.createStarted();

      lsm.Remove(mapping);

      stopwatch.stop();

      log.info("DeletePointMapping Completed; ShardMap name: {}; Point Mapping: {}; Duration: {}",
          this.getName(), mappingKey, stopwatch.elapsed(TimeUnit.MILLISECONDS));
    }
  }

  /**
   * Looks up the key value and returns the corresponding mapping.
   *
   * @param key Input key value.
   * @return Mapping that contains the key value.
   */
  public PointMapping getMappingForKey(TKey key) {
    try (ActivityIdScope activityIdScope = new ActivityIdScope(UUID.randomUUID())) {
      log.info("LookupPointMapping", "Start; ShardMap name: {}; Point Mapping Key Type:{}",
          this.getName(), key.getClass());

      Stopwatch stopwatch = Stopwatch.createStarted();

      PointMapping pointMapping = lsm.Lookup(key, false);

      stopwatch.stop();

      log.info("LookupPointMapping",
          "Complete; ShardMap name: {}; Point Mapping Key Type: {}; Duration: {}", this.getName(),
          key.getClass(), stopwatch.elapsed(TimeUnit.MILLISECONDS));

      return pointMapping;
    }
  }

  /**
   * Tries to looks up the key value and place the corresponding mapping in <paramref
   * name="pointMapping"/>.
   *
   * @param key Input key value.
   * @param pointMapping Mapping that contains the key value.
   * @return <c>true</c> if mapping is found, <c>false</c> otherwise.
   */
  public boolean tryGetMappingForKey(TKey key, ReferenceObjectHelper<PointMapping> pointMapping) {
    try (ActivityIdScope activityIdScope = new ActivityIdScope(UUID.randomUUID())) {
      log.info("TryLookupPointMapping", "Start; ShardMap name: {}; Point Mapping Key Type:{}",
          this.getName(), key.getClass());

      Stopwatch stopwatch = Stopwatch.createStarted();

      boolean result = lsm.TryLookup(key, false, pointMapping);

      stopwatch.stop();

      log.info("TryLookupPointMapping",
          "Complete; ShardMap name: {}; Point Mapping Key Type: {}; Duration: {}", this.getName(),
          key.getClass(), stopwatch.elapsed(TimeUnit.MILLISECONDS));

      return result;
    }
  }

  /**
   * Gets all the point mappings for the shard map.
   *
   * @return Read-only collection of all point mappings on the shard map.
   */
  public List<PointMapping> getMappings() {
    try (ActivityIdScope activityIdScope = new ActivityIdScope(UUID.randomUUID())) {
      log.info("GetPointMappings", "Start;");

      Stopwatch stopwatch = Stopwatch.createStarted();

      List<PointMapping> pointMappings = lsm.GetMappingsForRange(null, null);

      stopwatch.stop();

      log.info("GetPointMappings", "Complete; Duration:{}",
          stopwatch.elapsed(TimeUnit.MILLISECONDS));

      return pointMappings;
    }
  }

  /**
   * Gets all the mappings that exist within given range.
   *
   * @param range Point value, any mapping overlapping with the range will be returned.
   * @return Read-only collection of mappings that satisfy the given range constraint.
   */
  public List<PointMapping> getMappings(Range range) {
    ExceptionUtils.DisallowNullArgument(range, "range");

    try (ActivityIdScope activityIdScope = new ActivityIdScope(UUID.randomUUID())) {
      log.info("GetPointMappings", "Start; Range:{}", range);

      Stopwatch stopwatch = Stopwatch.createStarted();

      List<PointMapping> pointMappings = lsm.GetMappingsForRange(range, null);

      stopwatch.stop();

      log.info("GetPointMappings", "Complete; Range: {}; Duration:{}", range,
          stopwatch.elapsed(TimeUnit.MILLISECONDS));

      return pointMappings;
    }
  }

  /**
   * Gets all the mappings that exist for the given shard.
   *
   * @param shard Shard for which the mappings will be returned.
   * @return Read-only collection of mappings that satisfy the given shard constraint.
   */
  public List<PointMapping> getMappings(Shard shard) {
    ExceptionUtils.DisallowNullArgument(shard, "shard");

    try (ActivityIdScope activityIdScope = new ActivityIdScope(UUID.randomUUID())) {
      log.info("GetPointMappings", "Start; Shard:{}", shard.getLocation());

      Stopwatch stopwatch = Stopwatch.createStarted();

      List<PointMapping> pointMappings = lsm.GetMappingsForRange(null, shard);

      stopwatch.stop();

      log.info("GetPointMappings", "Complete; Shard: {}; Duration:{}", shard.getLocation(),
          stopwatch.elapsed(TimeUnit.MILLISECONDS));

      return pointMappings;
    }
  }

  /**
   * Gets all the mappings that exist within given range and given shard.
   *
   * @param range Point value, any mapping overlapping with the range will be returned.
   * @param shard Shard for which the mappings will be returned.
   * @return Read-only collection of mappings that satisfy the given range and shard constraints.
   */
  public List<PointMapping> getMappings(Range range, Shard shard) {
    ExceptionUtils.DisallowNullArgument(range, "range");
    ExceptionUtils.DisallowNullArgument(shard, "shard");

    try (ActivityIdScope activityIdScope = new ActivityIdScope(UUID.randomUUID())) {
      log.info("GetPointMappings", "Start; Shard: {}; Range:{}", shard.getLocation(), range);

      Stopwatch stopwatch = Stopwatch.createStarted();

      List<PointMapping> pointMappings = lsm.GetMappingsForRange(range, shard);

      stopwatch.stop();

      log.info("GetPointMappings", "Complete; Shard: {}; Duration:{}", shard.getLocation(),
          stopwatch.elapsed(TimeUnit.MILLISECONDS));

      return pointMappings;
    }
  }

  /**
   * Marks the specified mapping offline.
   *
   * @param mapping Input point mapping.
   * @return An offline mapping.
   */
  public PointMapping markMappingOffline(PointMapping mapping) {
    ExceptionUtils.DisallowNullArgument(mapping, "mapping");

    try (ActivityIdScope activityIdScope = new ActivityIdScope(UUID.randomUUID())) {
      log.info("MarkMappingOffline", "Start; ");

      Stopwatch stopwatch = Stopwatch.createStarted();

      PointMapping result = lsm.MarkMappingOffline(mapping);

      stopwatch.stop();

      log.info("MarkMappingOffline", "Complete; Duration:{}",
          stopwatch.elapsed(TimeUnit.MILLISECONDS));

      return result;
    }
  }

  /**
   * Marks the specified mapping online.
   *
   * @param mapping Input point mapping.
   * @return An online mapping.
   */
  public PointMapping markMappingOnline(PointMapping mapping) {
    ExceptionUtils.DisallowNullArgument(mapping, "mapping");

    try (ActivityIdScope activityIdScope = new ActivityIdScope(UUID.randomUUID())) {
      log.info("MarkMappingOnline", "Start; ");

      Stopwatch stopwatch = Stopwatch.createStarted();

      PointMapping result = lsm.MarkMappingOnline(mapping);

      stopwatch.stop();

      log.info("MarkMappingOnline", "Complete; Duration:{}",
          stopwatch.elapsed(TimeUnit.MILLISECONDS));

      return result;
    }
  }

  /**
   * Updates a <see cref="PointMapping{TKey}"/> with the updates provided in
   * the <paramref name="update"/> parameter.
   *
   * @param currentMapping Mapping being updated.
   * @param update Updated properties of the mapping.
   * @return New instance of mapping with updated information.
   */
  public PointMapping updateMapping(PointMapping currentMapping, PointMappingUpdate update) {
    return this.updateMapping(currentMapping, update, MappingLockToken.NoLock);
  }

  /**
   * Updates a point mapping with the changes provided in
   * the <paramref name="update"/> parameter.
   *
   * @param currentMapping Mapping being updated.
   * @param update Updated properties of the Shard.
   * @param mappingLockToken An instance of <see cref="MappingLockToken"/>
   * @return New instance of mapping with updated information.
   */
  public PointMapping updateMapping(PointMapping currentMapping, PointMappingUpdate update,
      MappingLockToken mappingLockToken) {
    ExceptionUtils.DisallowNullArgument(currentMapping, "currentMapping");
    ExceptionUtils.DisallowNullArgument(update, "update");
    ExceptionUtils.DisallowNullArgument(mappingLockToken, "mappingLockToken");

    try (ActivityIdScope activityIdScope = new ActivityIdScope(UUID.randomUUID())) {
      String mappingKey = currentMapping.getKey().getRawValue().toString();
      log.info("UpdatePointMapping", "Start; ShardMap name: {}; Current Point Mapping:{}",
          this.getName(), mappingKey);

      Stopwatch stopwatch = Stopwatch.createStarted();

      PointMapping pointMapping = lsm
          .Update(currentMapping, update, mappingLockToken.getLockOwnerId());

      stopwatch.stop();

      log.info("UpdatePointMapping",
          "Complete; ShardMap name: {}; Current Point Mapping: {}; Duration: {}", this.getName(),
          mappingKey, stopwatch.elapsed(TimeUnit.MILLISECONDS));

      return pointMapping;
    }
  }

  /**
   * Gets the lock owner id of the specified mapping.
   *
   * @param mapping Input range mapping.
   * @return An instance of <see cref="MappingLockToken"/>
   */
  public MappingLockToken getMappingLockOwner(PointMapping mapping) {
    ExceptionUtils.DisallowNullArgument(mapping, "mapping");

    try (ActivityIdScope activityIdScope = new ActivityIdScope(UUID.randomUUID())) {
      log.info("LookupLockOwner", "Start");

      Stopwatch stopwatch = Stopwatch.createStarted();

      UUID storeLockOwnerId = lsm.GetLockOwnerForMapping(mapping);

      stopwatch.stop();

      log.info("LookupLockOwner", "Complete; Duration: {}; StoreLockOwnerId:{}",
          stopwatch.elapsed(TimeUnit.MILLISECONDS), storeLockOwnerId);

      return new MappingLockToken(storeLockOwnerId);
    }
  }

  /**
   * Locks the mapping for the specified owner
   * The state of a locked mapping can only be modified by the lock owner.
   *
   * @param mapping Input range mapping.
   * @param mappingLockToken An instance of <see cref="MappingLockToken"/>
   */
  public void lockMapping(PointMapping mapping, MappingLockToken mappingLockToken) {
    ExceptionUtils.DisallowNullArgument(mapping, "mapping");
    ExceptionUtils.DisallowNullArgument(mappingLockToken, "mappingLockToken");

    try (ActivityIdScope activityIdScope = new ActivityIdScope(UUID.randomUUID())) {
      // Generate a lock owner id
      UUID lockOwnerId = mappingLockToken.getLockOwnerId();

      log.info("Lock", "Start; LockOwnerId:{}", lockOwnerId);

      Stopwatch stopwatch = Stopwatch.createStarted();

      lsm.LockOrUnlockMappings(mapping, lockOwnerId, LockOwnerIdOpType.Lock);

      stopwatch.stop();

      log.info("Lock", "Complete; Duration: {}; StoreLockOwnerId:{}",
          stopwatch.elapsed(TimeUnit.MILLISECONDS), lockOwnerId);
    }
  }

  /**
   * Unlocks the specified mapping
   *
   * @param mapping Input range mapping.
   * @param mappingLockToken An instance of <see cref="MappingLockToken"/>
   */
  public void unlockMapping(PointMapping mapping, MappingLockToken mappingLockToken) {
    ExceptionUtils.DisallowNullArgument(mapping, "mapping");
    ExceptionUtils.DisallowNullArgument(mappingLockToken, "mappingLockToken");

    try (ActivityIdScope activityIdScope = new ActivityIdScope(UUID.randomUUID())) {
      UUID lockOwnerId = mappingLockToken.getLockOwnerId();
      log.info("Unlock", "Start; LockOwnerId:{}", lockOwnerId);

      Stopwatch stopwatch = Stopwatch.createStarted();

      lsm.LockOrUnlockMappings(mapping, lockOwnerId, LockOwnerIdOpType.UnlockMappingForId);

      stopwatch.stop();

      log.info("UnLock", "Complete; Duration: {}; StoreLockOwnerId:{}",
          stopwatch.elapsed(TimeUnit.MILLISECONDS), lockOwnerId);
    }
  }

  /**
   * Unlocks all mappings in this map that belong to the given <see cref="MappingLockToken"/>.
   *
   * @param mappingLockToken An instance of <see cref="MappingLockToken"/>
   */
  public void unlockMapping(MappingLockToken mappingLockToken) {
    ExceptionUtils.DisallowNullArgument(mappingLockToken, "mappingLockToken");

    try (ActivityIdScope activityIdScope = new ActivityIdScope(UUID.randomUUID())) {
      UUID lockOwnerId = mappingLockToken.getLockOwnerId();
      log.info("UnlockAllMappingsWithLockOwnerId", "Start; LockOwnerId:{}", lockOwnerId);

      Stopwatch stopwatch = Stopwatch.createStarted();

      lsm.LockOrUnlockMappings(null, lockOwnerId, LockOwnerIdOpType.UnlockAllMappingsForId);

      stopwatch.stop();

      log.info("UnlockAllMappingsWithLockOwnerId", "Complete; Duration:{}",
          stopwatch.elapsed(TimeUnit.MILLISECONDS));
    }
  }

  /**
   * Gets the mapper. This method is used by OpenConnection/Lookup of V.
   * <typeparam name="V">Shard provider type.</typeparam>
   *
   * @return ListShardMapper for given key type.
   */
  @Override
  public <V> IShardMapper getMapper() {
    return lsm;
  }

  ///#region ICloneable<ShardMap>

  /**
   * Clones the specified shard map.
   *
   * @return A cloned instance of the shard map.
   */
  public ShardMap clone() {
    return this.cloneCore();
  }

  /**
   * Clones the current shard map instance.
   *
   * @return Cloned shard map instance.
   */
  @Override
  protected ShardMap cloneCore() {
    return new ListShardMap(shardMapManager, storeShardMap);
  }

  ///#endregion ICloneable<ShardMap>
}

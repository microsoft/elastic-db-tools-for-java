package com.microsoft.azure.elasticdb.shard.map;

// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

import com.google.common.base.Stopwatch;
import com.microsoft.azure.elasticdb.core.commons.helpers.ReferenceObjectHelper;
import com.microsoft.azure.elasticdb.core.commons.logging.ActivityIdScope;
import com.microsoft.azure.elasticdb.shard.base.*;
import com.microsoft.azure.elasticdb.shard.mapmanager.ShardMapManager;
import com.microsoft.azure.elasticdb.shard.mapper.IShardMapper;
import com.microsoft.azure.elasticdb.shard.mapper.IShardMapper1;
import com.microsoft.azure.elasticdb.shard.mapper.RangeShardMapper;
import com.microsoft.azure.elasticdb.shard.store.StoreShardMap;
import com.microsoft.azure.elasticdb.shard.utils.ExceptionUtils;
import com.microsoft.azure.elasticdb.shard.utils.ICloneable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

/**
 * Represents a shard map of ranges.
 * <p>
 * <typeparam name="TKey">Key type.</typeparam>
 */
public final class RangeShardMap<TKey> extends ShardMap implements ICloneable<ShardMap> {

    final static Logger log = LoggerFactory.getLogger(RangeShardMap.class);

    /**
     * Mapping b/w key ranges and shards.
     */
    public RangeShardMapper<TKey> rsm;

    /**
     * Constructs a new instance.
     *
     * @param manager Reference to ShardMapManager.
     * @param ssm     Storage representation.
     */
    public RangeShardMap(ShardMapManager manager, StoreShardMap ssm) {
        super(manager, ssm);
        assert manager != null;
        assert ssm != null;
        this.rsm = new RangeShardMapper<TKey>(this.getShardMapManager(), this);
    }

    ///#region Sync OpenConnection methods

    /**
     * Opens a regular <see cref="SqlConnection"/> to the shard
     * to which the specified key value is mapped, with <see cref="ConnectionOptions.Validate"/>.
     *
     * @param key              Input key value.
     * @param connectionString Connection string with credential information such as SQL Server credentials or Integrated Security settings.
     *                         The hostname of the server and the database name for the shard are obtained from the lookup operation for key.
     * @return An opened SqlConnection.
     * <p>
     * Note that the <see cref="SqlConnection"/> object returned by this call is not protected against transient faults.
     * Callers should follow best practices to protect the connection against transient faults
     * in their application code, e.g., by using the transient fault handling
     * functionality in the Enterprise Library from Microsoft Patterns and Practices team.
     */
    /*@Override
    public Connection OpenConnectionForKey(TKey key, String connectionString) {
        return this.OpenConnectionForKey(key, connectionString, ConnectionOptions.Validate);
    }*/

    ///#endregion

    ///#region Async OpenConnection methods

    /**
     * Opens a regular <see cref="SqlConnection"/> to the shard
     * to which the specified key value is mapped.
     *
     * @param key              Input key value.
     * @param connectionString Connection string with credential information such as SQL Server credentials or Integrated Security settings.
     *                         The hostname of the server and the database name for the shard are obtained from the lookup operation for key.
     * @param options          Options for validation operations to perform on opened connection.
     * @return An opened SqlConnection.
     * <p>
     * Note that the <see cref="SqlConnection"/> object returned by this call is not protected against transient faults.
     * Callers should follow best practices to protect the connection against transient faults
     * in their application code, e.g., by using the transient fault handling
     * functionality in the Enterprise Library from Microsoft Patterns and Practices team.
     */
    /*@Override
    public Connection OpenConnectionForKey(TKey key, String connectionString, ConnectionOptions options) {
        ExceptionUtils.DisallowNullArgument(connectionString, "connectionString");

        try (ActivityIdScope activityIdScope = new ActivityIdScope(UUID.randomUUID())) {
            return this.rsm.OpenConnectionForKey(key, connectionString, options);
        }
    }*/

    /**
     * Asynchronously opens a regular <see cref="SqlConnection"/> to the shard
     * to which the specified key value is mapped, with <see cref="ConnectionOptions.Validate"/>.
     *
     * @param key              Input key value.
     * @param connectionString Connection string with credential information such as SQL Server credentials or Integrated Security settings.
     *                         The hostname of the server and the database name for the shard are obtained from the lookup operation for key.
     * @return A Task encapsulating an opened SqlConnection.
     * <p>
     * Note that the <see cref="SqlConnection"/> object returned by this call is not protected against transient faults.
     * Callers should follow best practices to protect the connection against transient faults
     * in their application code, e.g., by using the transient fault handling
     * functionality in the Enterprise Library from Microsoft Patterns and Practices team.
     * All non-usage errors will be propagated via the returned Task.
     */
    /*@Override
    public Callable<SQLServerConnection> OpenConnectionForKeyAsync(TKey key, String connectionString) {
        return this.OpenConnectionForKeyAsync(key, connectionString, ConnectionOptions.Validate);
    }*/

    ///#endregion

    /**
     * Asynchronously opens a regular <see cref="SqlConnection"/> to the shard
     * to which the specified key value is mapped.
     *
     * @param key              Input key value.
     * @param connectionString Connection string with credential information such as SQL Server credentials or Integrated Security settings.
     *                         The hostname of the server and the database name for the shard are obtained from the lookup operation for key.
     * @param options          Options for validation operations to perform on opened connection.
     * @return A Task encapsulating an opened SqlConnection.
     * <p>
     * Note that the <see cref="SqlConnection"/> object returned by this call is not protected against transient faults.
     * Callers should follow best practices to protect the connection against transient faults
     * in their application code, e.g., by using the transient fault handling
     * functionality in the Enterprise Library from Microsoft Patterns and Practices team.
     * All non-usage errors will be propagated via the returned Task.
     */
    /*@Override
    public Callable<SQLServerConnection> OpenConnectionForKeyAsync(TKey key, String connectionString, ConnectionOptions options) {
        ExceptionUtils.DisallowNullArgument(connectionString, "connectionString");

        try (ActivityIdScope activityIdScope = new ActivityIdScope(UUID.randomUUID())) {
            return this.rsm.OpenConnectionForKeyAsync(key, connectionString, options);
        }
    }*/

    /**
     * Creates and adds a range mapping to ShardMap.
     *
     * @param creationInfo Information about mapping to be added.
     * @return Newly created mapping.
     */
    public RangeMapping<TKey> CreateRangeMapping(RangeMappingCreationInfo<TKey> creationInfo) {
        ExceptionUtils.DisallowNullArgument(creationInfo, "args");

        try (ActivityIdScope activityIdScope = new ActivityIdScope(UUID.randomUUID())) {
            log.info("CreateRangeMapping Start; Shard: {}", creationInfo.getShard().getLocation());

            Stopwatch stopwatch = Stopwatch.createStarted();

            RangeMapping<TKey> rangeMapping = this.rsm.Add(new RangeMapping<TKey>(this.getShardMapManager(), creationInfo));

            stopwatch.stop();

            log.info("CreateRangeMapping Complete; Shard: {}; Duration: {}", creationInfo.getShard().getLocation(), stopwatch.elapsed(TimeUnit.MILLISECONDS));

            return rangeMapping;
        }
    }

    /**
     * Creates and adds a range mapping to ShardMap.
     *
     * @param range Range for which to create the mapping.
     * @param shard Shard associated with the range mapping.
     * @return Newly created mapping.
     */
    public RangeMapping<TKey> CreateRangeMapping(Range<TKey> range, Shard shard) {
        ExceptionUtils.DisallowNullArgument(range, "range");
        ExceptionUtils.DisallowNullArgument(shard, "shard");

        try (ActivityIdScope activityIdScope = new ActivityIdScope(UUID.randomUUID())) {
            RangeMappingCreationInfo<TKey> args = new RangeMappingCreationInfo<TKey>(range, shard, MappingStatus.Online);

            log.info("CreateRangeMapping Start; Shard: {}", shard.getLocation());

            Stopwatch stopwatch = Stopwatch.createStarted();

            RangeMapping<TKey> rangeMapping = this.rsm.Add(new RangeMapping<TKey>(this.getShardMapManager(), args));

            stopwatch.stop();

            log.info("CreateRangeMapping Complete; Shard: {}; Duration: {}", shard.getLocation(), stopwatch.elapsed(TimeUnit.MILLISECONDS));

            return rangeMapping;
        }
    }

    /**
     * Removes a range mapping.
     *
     * @param mapping Mapping being removed.
     */
    public void DeleteMapping(RangeMapping<TKey> mapping) {
        this.DeleteMapping(mapping, MappingLockToken.NoLock);
    }

    /**
     * Removes a range mapping.
     *
     * @param mapping          Mapping being removed.
     * @param mappingLockToken An instance of <see cref="MappingLockToken"/>
     */
    public void DeleteMapping(RangeMapping<TKey> mapping, MappingLockToken mappingLockToken) {
        ExceptionUtils.DisallowNullArgument(mapping, "mapping");
        ExceptionUtils.DisallowNullArgument(mappingLockToken, "mappingLockToken");

        try (ActivityIdScope activityIdScope = new ActivityIdScope(UUID.randomUUID())) {
            //log.info("CreatePointMapping Start; ShardMap name: {}; Point Mapping: {}", this.getName(), mappingKey);
            log.info("DeleteMapping Start; Shard: {}", mapping.getShard().getLocation());

            Stopwatch stopwatch = Stopwatch.createStarted();

            this.rsm.Remove(mapping, mappingLockToken.getLockOwnerId());

            stopwatch.stop();

            log.info("DeleteMapping Complete; Shard: {}; Duration: {}", mapping.getShard().getLocation(), stopwatch.elapsed(TimeUnit.MILLISECONDS));
        }
    }

    /**
     * Looks up the key value and returns the corresponding mapping.
     *
     * @param key Input key value.
     * @return Mapping that contains the key value.
     */
    public RangeMapping<TKey> GetMappingForKey(TKey key) {
        try (ActivityIdScope activityIdScope = new ActivityIdScope(UUID.randomUUID())) {
            log.info("GetMapping Start; Range Mapping Key Type: {}", key.getClass());

            Stopwatch stopwatch = Stopwatch.createStarted();

            RangeMapping<TKey> rangeMapping = this.rsm.Lookup(key, false);

            stopwatch.stop();

            log.info("GetMapping Complete; Range Mapping Key Type: {} Duration: {}", key.getClass(), stopwatch.elapsed(TimeUnit.MILLISECONDS));

            return rangeMapping;
        }
    }

    /**
     * Tries to looks up the key value and place the corresponding mapping in <paramref name="rangeMapping"/>.
     *
     * @param key          Input key value.
     * @param rangeMapping Mapping that contains the key value.
     * @return <c>true</c> if mapping is found, <c>false</c> otherwise.
     */
    public boolean TryGetMappingForKey(TKey key, ReferenceObjectHelper<RangeMapping<TKey>> rangeMapping) {
        try (ActivityIdScope activityIdScope = new ActivityIdScope(UUID.randomUUID())) {
            log.info("TryLookupRangeMapping Start; ShardMap name: {}; Range Mapping Key Type: {}", this.getName(), key.getClass());

            Stopwatch stopwatch = Stopwatch.createStarted();

            boolean result = this.rsm.TryLookup(key, false, rangeMapping);

            stopwatch.stop();

            log.info("TryLookupRangeMapping Complete; ShardMap name: {}; Range Mapping Key Type: {}; Duration: {}", this.getName(), key.getClass(), stopwatch.elapsed(TimeUnit.MILLISECONDS));

            return result;
        }
    }

    /**
     * Gets all the range mappings for the shard map.
     *
     * @return Read-only collection of all range mappings on the shard map.
     */
    public List<RangeMapping<TKey>> GetMappings() {
        try (ActivityIdScope activityIdScope = new ActivityIdScope(UUID.randomUUID())) {
            log.info("GetMappings Start;");

            Stopwatch stopwatch = Stopwatch.createStarted();

            List<RangeMapping<TKey>> rangeMappings = this.rsm.GetMappingsForRange(null, null);

            stopwatch.stop();

            log.info("GetMappings Complete; Duration: {}", stopwatch.elapsed(TimeUnit.MILLISECONDS));

            return rangeMappings;
        }
    }

    /**
     * Gets all the range mappings that exist within given range.
     *
     * @param range Range value, any mapping overlapping with the range will be returned.
     * @return Read-only collection of mappings that satisfy the given range constraint.
     */
    public List<RangeMapping<TKey>> GetMappings(Range<TKey> range) {
        ExceptionUtils.DisallowNullArgument(range, "range");

        try (ActivityIdScope activityIdScope = new ActivityIdScope(UUID.randomUUID())) {
            log.info("GetMappings Start; Range: {}", range);

            Stopwatch stopwatch = Stopwatch.createStarted();

            List<RangeMapping<TKey>> rangeMappings = this.rsm.GetMappingsForRange(range, null);

            stopwatch.stop();

            log.info("GetMappings Complete; Range: {}; Duration: {}", range, stopwatch.elapsed(TimeUnit.MILLISECONDS));

            return rangeMappings;
        }
    }

    /**
     * Gets all the range mappings that exist for the given shard.
     *
     * @param shard Shard for which the mappings will be returned.
     * @return Read-only collection of mappings that satisfy the given shard constraint.
     */
    public List<RangeMapping<TKey>> GetMappings(Shard shard) {
        ExceptionUtils.DisallowNullArgument(shard, "shard");

        try (ActivityIdScope activityIdScope = new ActivityIdScope(UUID.randomUUID())) {
            log.info("GetMappings Start; Shard: {}", shard.getLocation());

            Stopwatch stopwatch = Stopwatch.createStarted();

            List<RangeMapping<TKey>> rangeMappings = this.rsm.GetMappingsForRange(null, shard);

            stopwatch.stop();

            log.info("GetMappings Complete; Shard: {}; Duration: {}", shard.getLocation(), stopwatch.elapsed(TimeUnit.MILLISECONDS));

            return rangeMappings;
        }
    }

    /**
     * Gets all the range mappings that exist within given range and given shard.
     *
     * @param range Range value, any mapping overlapping with the range will be returned.
     * @param shard Shard for which the mappings will be returned.
     * @return Read-only collection of mappings that satisfy the given range and shard constraints.
     */
    public List<RangeMapping<TKey>> GetMappings(Range<TKey> range, Shard shard) {
        ExceptionUtils.DisallowNullArgument(range, "range");
        ExceptionUtils.DisallowNullArgument(shard, "shard");

        try (ActivityIdScope activityIdScope = new ActivityIdScope(UUID.randomUUID())) {
            log.info("GetMappings Start; Shard: {}; Range: {}", shard.getLocation(), range);

            Stopwatch stopwatch = Stopwatch.createStarted();

            List<RangeMapping<TKey>> rangeMappings = this.rsm.GetMappingsForRange(range, shard);

            stopwatch.stop();

            log.info("GetMappings Complete; Shard: {}; Duration: {}", shard.getLocation(), stopwatch.elapsed(TimeUnit.MILLISECONDS));

            return rangeMappings;
        }
    }

    /**
     * Marks the specified mapping offline.
     *
     * @param mapping Input range mapping.
     * @return An offline mapping.
     */
    public RangeMapping<TKey> MarkMappingOffline(RangeMapping<TKey> mapping) {
        return this.MarkMappingOffline(mapping, MappingLockToken.NoLock);
    }

    /**
     * Marks the specified mapping offline.
     *
     * @param mapping          Input range mapping.
     * @param mappingLockToken An instance of <see cref="MappingLockToken"/>
     * @return An offline mapping.
     */
    public RangeMapping<TKey> MarkMappingOffline(RangeMapping<TKey> mapping, MappingLockToken mappingLockToken) {
        ExceptionUtils.DisallowNullArgument(mapping, "mapping");
        ExceptionUtils.DisallowNullArgument(mappingLockToken, "mappingLockToken");

        try (ActivityIdScope activityIdScope = new ActivityIdScope(UUID.randomUUID())) {
            log.info("MarkMappingOffline Start; ");

            Stopwatch stopwatch = Stopwatch.createStarted();

            RangeMapping<TKey> result = this.rsm.MarkMappingOffline(mapping, mappingLockToken.getLockOwnerId());

            stopwatch.stop();

            log.info("MarkMappingOffline Complete; Duration: {}", stopwatch.elapsed(TimeUnit.MILLISECONDS));

            return result;
        }
    }

    /**
     * Marks the specified mapping online.
     *
     * @param mapping Input range mapping.
     * @return An online mapping.
     */
    public RangeMapping<TKey> MarkMappingOnline(RangeMapping<TKey> mapping) {
        return this.MarkMappingOnline(mapping, MappingLockToken.NoLock);
    }

    /**
     * Marks the specified mapping online.
     *
     * @param mapping          Input range mapping.
     * @param mappingLockToken An instance of <see cref="MappingLockToken"/>
     * @return An online mapping.
     */
    public RangeMapping<TKey> MarkMappingOnline(RangeMapping<TKey> mapping, MappingLockToken mappingLockToken) {
        ExceptionUtils.DisallowNullArgument(mapping, "mapping");
        ExceptionUtils.DisallowNullArgument(mappingLockToken, "mappingLockToken");

        try (ActivityIdScope activityIdScope = new ActivityIdScope(UUID.randomUUID())) {
            log.info("MarkMappingOnline Start; ");

            Stopwatch stopwatch = Stopwatch.createStarted();

            RangeMapping<TKey> result = this.rsm.MarkMappingOnline(mapping, mappingLockToken.getLockOwnerId());

            stopwatch.stop();

            log.info("MarkMappingOnline Complete; Duration: {}", stopwatch.elapsed(TimeUnit.MILLISECONDS));

            return result;
        }
    }

    /**
     * Gets the lock owner id of the specified mapping.
     *
     * @param mapping Input range mapping.
     * @return An instance of <see cref="MappingLockToken"/>
     */
    public MappingLockToken GetMappingLockOwner(RangeMapping<TKey> mapping) {
        ExceptionUtils.DisallowNullArgument(mapping, "mapping");

        try (ActivityIdScope activityIdScope = new ActivityIdScope(UUID.randomUUID())) {
            log.info("LookupLockOwner Start");

            Stopwatch stopwatch = Stopwatch.createStarted();

            UUID storeLockOwnerId = this.rsm.GetLockOwnerForMapping(mapping);

            stopwatch.stop();

            log.info("LookupLockOwner Complete; Duration: {}; StoreLockOwnerId: {}", stopwatch.elapsed(TimeUnit.MILLISECONDS), storeLockOwnerId);

            return new MappingLockToken(storeLockOwnerId);
        }
    }

    /**
     * Locks the mapping for the specified owner
     * The state of a locked mapping can only be modified by the lock owner.
     *
     * @param mapping          Input range mapping.
     * @param mappingLockToken An instance of <see cref="MappingLockToken"/>
     */
    public void LockMapping(RangeMapping<TKey> mapping, MappingLockToken mappingLockToken) {
        ExceptionUtils.DisallowNullArgument(mapping, "mapping");
        ExceptionUtils.DisallowNullArgument(mappingLockToken, "mappingLockToken");

        try (ActivityIdScope activityIdScope = new ActivityIdScope(UUID.randomUUID())) {
            // Generate a lock owner id
            UUID lockOwnerId = mappingLockToken.getLockOwnerId();

            log.info("Lock Start; LockOwnerId: {}", lockOwnerId);

            Stopwatch stopwatch = Stopwatch.createStarted();

            this.rsm.LockOrUnlockMappings(mapping, lockOwnerId, LockOwnerIdOpType.Lock);

            stopwatch.stop();

            log.info("Lock Complete; Duration: {}; StoreLockOwnerId: {}", stopwatch.elapsed(TimeUnit.MILLISECONDS), lockOwnerId);
        }
    }

    /**
     * Unlocks the specified mapping
     *
     * @param mapping          Input range mapping.
     * @param mappingLockToken An instance of <see cref="MappingLockToken"/>
     */
    public void UnlockMapping(RangeMapping<TKey> mapping, MappingLockToken mappingLockToken) {
        ExceptionUtils.DisallowNullArgument(mapping, "mapping");
        ExceptionUtils.DisallowNullArgument(mappingLockToken, "mappingLockToken");

        try (ActivityIdScope activityIdScope = new ActivityIdScope(UUID.randomUUID())) {
            UUID lockOwnerId = mappingLockToken.getLockOwnerId();
            log.info("Unlock Start; LockOwnerId: {}", lockOwnerId);

            Stopwatch stopwatch = Stopwatch.createStarted();

            this.rsm.LockOrUnlockMappings(mapping, lockOwnerId, LockOwnerIdOpType.UnlockMappingForId);

            stopwatch.stop();

            log.info("UnLock Complete; Duration: {}; StoreLockOwnerId: {}", stopwatch.elapsed(TimeUnit.MILLISECONDS), lockOwnerId);
        }
    }

    /**
     * Unlocks all mappings in this map that belong to the given <see cref="MappingLockToken"/>
     *
     * @param mappingLockToken An instance of <see cref="MappingLockToken"/>
     */
    public void UnlockMapping(MappingLockToken mappingLockToken) {
        ExceptionUtils.DisallowNullArgument(mappingLockToken, "mappingLockToken");

        try (ActivityIdScope activityIdScope = new ActivityIdScope(UUID.randomUUID())) {
            UUID lockOwnerId = mappingLockToken.getLockOwnerId();
            log.info("UnlockAllMappingsWithLockOwnerId Start; LockOwnerId: {}", lockOwnerId);

            Stopwatch stopwatch = Stopwatch.createStarted();

            this.rsm.LockOrUnlockMappings(null, lockOwnerId, LockOwnerIdOpType.UnlockAllMappingsForId);

            stopwatch.stop();

            log.info("UnlockAllMappingsWithLockOwnerId Complete; Duration: {}", stopwatch.elapsed(TimeUnit.MILLISECONDS));
        }
    }

    /**
     * Updates a <see cref="RangeMapping{TKey}"/> with the updates provided in
     * the <paramref name="update"/> parameter.
     *
     * @param currentMapping Mapping being updated.
     * @param update         Updated properties of the mapping.
     * @return New instance of mapping with updated information.
     */
    public RangeMapping<TKey> UpdateMapping(RangeMapping<TKey> currentMapping, RangeMappingUpdate update) {
        return this.UpdateMapping(currentMapping, update, MappingLockToken.NoLock);
    }

    /**
     * Updates a <see cref="RangeMapping{TKey}"/> with the updates provided in
     * the <paramref name="update"/> parameter.
     *
     * @param currentMapping   Mapping being updated.
     * @param update           Updated properties of the mapping.
     * @param mappingLockToken An instance of <see cref="MappingLockToken"/>
     * @return New instance of mapping with updated information.
     */
    public RangeMapping<TKey> UpdateMapping(RangeMapping<TKey> currentMapping, RangeMappingUpdate update, MappingLockToken mappingLockToken) {
        ExceptionUtils.DisallowNullArgument(currentMapping, "currentMapping");
        ExceptionUtils.DisallowNullArgument(update, "update");
        ExceptionUtils.DisallowNullArgument(mappingLockToken, "mappingLockToken");

        try (ActivityIdScope activityIdScope = new ActivityIdScope(UUID.randomUUID())) {
            log.info("UpdateMapping Start; Current mapping shard: {}", currentMapping.getShard().getLocation());

            Stopwatch stopwatch = Stopwatch.createStarted();

            RangeMapping<TKey> rangeMapping = this.rsm.Update(currentMapping, update, mappingLockToken.getLockOwnerId());

            stopwatch.stop();

            log.info("UpdateMapping Complete; Current mapping shard: {}; Duration: {}", currentMapping.getShard().getLocation(), stopwatch.elapsed(TimeUnit.MILLISECONDS));

            return rangeMapping;
        }
    }

    /**
     * Splits the specified mapping into two new mappings using the specified key as boundary.
     * The new mappings point to the same shard as the existing mapping.
     *
     * @param existingMapping Existing mapping.
     * @param splitAt         Split point.
     * @return Read-only collection of two new mappings that were created.
     */
    public List<RangeMapping<TKey>> SplitMapping(RangeMapping<TKey> existingMapping, TKey splitAt) {
        return this.SplitMapping(existingMapping, splitAt, MappingLockToken.NoLock);
    }

    /**
     * Splits the specified mapping into two new mappings using the specified key as boundary.
     * The new mappings point to the same shard as the existing mapping.
     *
     * @param existingMapping  Existing mapping.
     * @param splitAt          Split point.
     * @param mappingLockToken An instance of <see cref="MappingLockToken"/>
     * @return Read-only collection of two new mappings that were created.
     */
    public List<RangeMapping<TKey>> SplitMapping(RangeMapping<TKey> existingMapping, TKey splitAt, MappingLockToken mappingLockToken) {
        ExceptionUtils.DisallowNullArgument(existingMapping, "existingMapping");
        ExceptionUtils.DisallowNullArgument(mappingLockToken, "mappingLockToken");

        try (ActivityIdScope activityIdScope = new ActivityIdScope(UUID.randomUUID())) {
            log.info("SplitMapping Start; Shard: {}", existingMapping.getShard().getLocation());

            Stopwatch stopwatch = Stopwatch.createStarted();

            List<RangeMapping<TKey>> rangeMapping = this.rsm.Split(existingMapping, splitAt, mappingLockToken.getLockOwnerId());

            stopwatch.stop();

            log.info("SplitMapping Complete; Shard: {}; Duration: {}", existingMapping.getShard().getLocation(), stopwatch.elapsed(TimeUnit.MILLISECONDS));

            return rangeMapping;
        }
    }

    /**
     * Merges 2 contiguous mappings into a single mapping. Both left and right mappings should point
     * to the same location and must be contiguous.
     *
     * @param left  Left mapping.
     * @param right Right mapping.
     * @return Mapping that results from the merge operation.
     */
    public RangeMapping<TKey> MergeMappings(RangeMapping<TKey> left, RangeMapping<TKey> right) {
        return this.MergeMappings(left, right, MappingLockToken.NoLock, MappingLockToken.NoLock);
    }

    /**
     * Merges 2 contiguous mappings into a single mapping. Both left and right mappings should point
     * to the same location and must be contiguous.
     *
     * @param left                  Left mapping.
     * @param right                 Right mapping.
     * @param leftMappingLockToken  An instance of <see cref="MappingLockToken"/> for the left mapping
     * @param rightMappingLockToken An instance of <see cref="MappingLockToken"/> for the right mapping
     * @return Mapping that results from the merge operation.
     */
    public RangeMapping<TKey> MergeMappings(RangeMapping<TKey> left, RangeMapping<TKey> right, MappingLockToken leftMappingLockToken, MappingLockToken rightMappingLockToken) {
        ExceptionUtils.DisallowNullArgument(left, "left");
        ExceptionUtils.DisallowNullArgument(right, "right");
        ExceptionUtils.DisallowNullArgument(leftMappingLockToken, "leftMappingLockToken");
        ExceptionUtils.DisallowNullArgument(rightMappingLockToken, "rightMappingLockToken");

        try (ActivityIdScope activityIdScope = new ActivityIdScope(UUID.randomUUID())) {
            log.info("SplitMapping Start; Left Shard: {}; Right Shard: {}", left.getShard().getLocation(), right.getShard().getLocation());

            Stopwatch stopwatch = Stopwatch.createStarted();

            RangeMapping<TKey> rangeMapping = this.rsm.Merge(left, right, leftMappingLockToken.getLockOwnerId(), rightMappingLockToken.getLockOwnerId());

            stopwatch.stop();

            log.info("SplitMapping Complete; Duration: {}", stopwatch.elapsed(TimeUnit.MILLISECONDS));

            return rangeMapping;
        }
    }

    /**
     * Gets the mapper. This method is used by OpenConnection/Lookup of V.
     * <p>
     * <typeparam name="V">Shard provider type.</typeparam>
     *
     * @return RangeShardMapper for given key type.
     */
    @Override
    public <V> IShardMapper1<V> GetMapper() {
        return (IShardMapper1<V>) ((this.rsm instanceof IShardMapper) ? this.rsm : null);
    }

    ///#region ICloneable<ShardMap>

    /**
     * Clones the given range shard map.
     *
     * @return A cloned instance of the range shard map.
     */
    public RangeShardMap<TKey> Clone() {
        ShardMap tempVar = this.CloneCore();
        return (RangeShardMap<TKey>) ((tempVar instanceof RangeShardMap<?>) ? tempVar : null);
    }

    /**
     * Clones the given shard map.
     *
     * @return A cloned instance of the shard map.
     */
    /*public ShardMap Clone() {
        return this.CloneCore();
    }*/

    ///#endregion ICloneable<ShardMap>

    /**
     * Clones the current shard map instance.
     *
     * @return Cloned shard map instance.
     */
    @Override
    protected ShardMap CloneCore() {
        return new RangeShardMap<TKey>(this.getShardMapManager(), this.getStoreShardMap());
    }
}
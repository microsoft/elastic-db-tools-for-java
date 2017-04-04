package com.microsoft.azure.elasticdb.shard.map;

// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

import com.microsoft.azure.elasticdb.core.commons.logging.ActivityIdScope;
import com.microsoft.azure.elasticdb.core.commons.logging.TraceSourceConstants;
import com.microsoft.azure.elasticdb.shard.base.*;
import com.microsoft.azure.elasticdb.shard.mapmanager.ShardMapManager;
import com.microsoft.azure.elasticdb.shard.mapper.ConnectionOptions;
import com.microsoft.azure.elasticdb.shard.mapper.RangeShardMapper;
import com.microsoft.azure.elasticdb.shard.store.IStoreShardMap;
import com.microsoft.azure.elasticdb.shard.utils.ExceptionUtils;
import com.microsoft.azure.elasticdb.shard.utils.ICloneable;
import javafx.concurrent.Task;

import java.io.IOException;
import java.sql.Connection;
import java.util.UUID;

/**
 * Represents a shard map of ranges.
 * <p>
 * <typeparam name="TKey">Key type.</typeparam>
 */
public final class RangeShardMap<TKey> extends ShardMap implements ICloneable<ShardMap>, ICloneable<RangeShardMap<TKey>> {
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
    public RangeShardMap(ShardMapManager manager, IStoreShardMap ssm) {
        super(manager, ssm);
        assert manager != null;
        assert ssm != null;
        this.rsm = new RangeShardMapper<TKey>(this.getManager(), this);
    }

    ///#region Sync OpenConnection methods

    /**
     * The Tracer
     */
    private static ILogger getTracer() {
        return TraceHelper.Tracer;
    }

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
    public Connection OpenConnectionForKey(TKey key, String connectionString) {
        return this.OpenConnectionForKey(key, connectionString, ConnectionOptions.Validate);
    }

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
    public Connection OpenConnectionForKey(TKey key, String connectionString, ConnectionOptions options) {
        ExceptionUtils.DisallowNullArgument(connectionString, "connectionString");

        try (ActivityIdScope activityIdScope = new ActivityIdScope(UUID.randomUUID())) {
            return this.rsm.OpenConnectionForKey(key, connectionString, options);
        }
    }

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
    public Task<Connection> OpenConnectionForKeyAsync(TKey key, String connectionString) {
        return this.OpenConnectionForKeyAsync(key, connectionString, ConnectionOptions.Validate);
    }

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
    public Task<Connection> OpenConnectionForKeyAsync(TKey key, String connectionString, ConnectionOptions options) {
        ExceptionUtils.DisallowNullArgument(connectionString, "connectionString");

        try (ActivityIdScope activityIdScope = new ActivityIdScope(UUID.randomUUID())) {
            return this.rsm.OpenConnectionForKeyAsync(key, connectionString, options);
        }
    }

    /**
     * Creates and adds a range mapping to ShardMap.
     *
     * @param creationInfo Information about mapping to be added.
     * @return Newly created mapping.
     */
    public RangeMapping<TKey> CreateRangeMapping(RangeMappingCreationInfo<TKey> creationInfo) {
        ExceptionUtils.DisallowNullArgument(creationInfo, "args");

        try (ActivityIdScope activityIdScope = new ActivityIdScope(UUID.randomUUID())) {
            /*getTracer().TraceInfo(TraceSourceConstants.ComponentNames.RangeShardMap, "CreateRangeMapping", "Start; Shard: {0}", creationInfo.Shard.Location);
            Stopwatch stopwatch = Stopwatch.StartNew();*/

            RangeMapping<TKey> rangeMapping = this.rsm.Add(new RangeMapping<TKey>(this.getManager(), creationInfo));

            /*stopwatch.Stop();
            getTracer().TraceInfo(TraceSourceConstants.ComponentNames.RangeShardMap, "CreateRangeMapping", "Complete; Shard: {0}; Duration: {1}", creationInfo.Shard.Location, stopwatch.Elapsed);*/

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

            //getTracer().TraceInfo(TraceSourceConstants.ComponentNames.RangeShardMap, "CreateRangeMapping", "Start; Shard: {0}", shard.Location);

            //Stopwatch stopwatch = Stopwatch.StartNew();

            RangeMapping<TKey> rangeMapping = this.rsm.Add(new RangeMapping<TKey>(this.getManager(), args));

            //stopwatch.Stop();

            //getTracer().TraceInfo(TraceSourceConstants.ComponentNames.RangeShardMap, "CreateRangeMapping", "Complete; Shard: {0}; Duration: {1}", shard.Location, stopwatch.Elapsed);

            return rangeMapping;
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
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
            getTracer().TraceInfo(TraceSourceConstants.ComponentNames.RangeShardMap, "DeleteMapping", "Start; Shard: {0}", mapping.Shard.Location);

            Stopwatch stopwatch = Stopwatch.StartNew();

            this.rsm.Remove(mapping, mappingLockToken.LockOwnerId);

            stopwatch.Stop();

            getTracer().TraceInfo(TraceSourceConstants.ComponentNames.RangeShardMap, "DeleteMapping", "Complete; Shard: {0}; Duration: {1}", mapping.Shard.Location, stopwatch.Elapsed);
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
            getTracer().TraceInfo(TraceSourceConstants.ComponentNames.RangeShardMap, "GetMapping", "Start; Range Mapping Key Type: {0}", TKey.class);

            Stopwatch stopwatch = Stopwatch.StartNew();

            RangeMapping<TKey> rangeMapping = this.rsm.Lookup(key, false);

            stopwatch.Stop();

            getTracer().TraceInfo(TraceSourceConstants.ComponentNames.RangeShardMap, "GetMapping", "Complete; Range Mapping Key Type: {0} Duration: {1}", TKey.class, stopwatch.Elapsed);

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
            getTracer().TraceInfo(TraceSourceConstants.ComponentNames.RangeShardMap, "TryLookupRangeMapping", "Start; ShardMap name: {0}; Range Mapping Key Type: {1}", this.Name, TKey.class);

            Stopwatch stopwatch = Stopwatch.StartNew();

            boolean result = this.rsm.TryLookup(key, false, rangeMapping);

            stopwatch.Stop();

            getTracer().TraceInfo(TraceSourceConstants.ComponentNames.RangeShardMap, "TryLookupRangeMapping", "Complete; ShardMap name: {0}; Range Mapping Key Type: {1}; Duration: {2}", this.Name, TKey.class, stopwatch.Elapsed);

            return result;
        }
    }

    /**
     * Gets all the range mappings for the shard map.
     *
     * @return Read-only collection of all range mappings on the shard map.
     */
    public IReadOnlyList<RangeMapping<TKey>> GetMappings() {
        try (ActivityIdScope activityIdScope = new ActivityIdScope(UUID.randomUUID())) {
            /*getTracer().TraceInfo(TraceSourceConstants.ComponentNames.RangeShardMap, "GetMappings", "Start;");
            Stopwatch stopwatch = Stopwatch.StartNew();*/

            IReadOnlyList<RangeMapping<TKey>> rangeMappings = this.rsm.GetMappingsForRange(null, null);

            /*stopwatch.Stop();
            getTracer().TraceInfo(TraceSourceConstants.ComponentNames.RangeShardMap, "GetMappings", "Complete; Duration: {0}", stopwatch.Elapsed);*/

            return rangeMappings;
        }
    }

    /**
     * Gets all the range mappings that exist within given range.
     *
     * @param range Range value, any mapping overlapping with the range will be returned.
     * @return Read-only collection of mappings that satisfy the given range constraint.
     */
    public IReadOnlyList<RangeMapping<TKey>> GetMappings(Range<TKey> range) {
        ExceptionUtils.DisallowNullArgument(range, "range");

        try (ActivityIdScope activityIdScope = new ActivityIdScope(UUID.randomUUID())) {
            getTracer().TraceInfo(TraceSourceConstants.ComponentNames.RangeShardMap, "GetMappings", "Start; Range: {0}", range);

            Stopwatch stopwatch = Stopwatch.StartNew();

            IReadOnlyList<RangeMapping<TKey>> rangeMappings = this.rsm.GetMappingsForRange(range, null);

            stopwatch.Stop();

            getTracer().TraceInfo(TraceSourceConstants.ComponentNames.RangeShardMap, "GetMappings", "Complete; Range: {0}; Duration: {1}", range, stopwatch.Elapsed);

            return rangeMappings;
        }
    }

    /**
     * Gets all the range mappings that exist for the given shard.
     *
     * @param shard Shard for which the mappings will be returned.
     * @return Read-only collection of mappings that satisfy the given shard constraint.
     */
    public IReadOnlyList<RangeMapping<TKey>> GetMappings(Shard shard) {
        ExceptionUtils.DisallowNullArgument(shard, "shard");

        try (ActivityIdScope activityIdScope = new ActivityIdScope(UUID.randomUUID())) {
            getTracer().TraceInfo(TraceSourceConstants.ComponentNames.RangeShardMap, "GetMappings", "Start; Shard: {0}", shard.Location);

            Stopwatch stopwatch = Stopwatch.StartNew();

            IReadOnlyList<RangeMapping<TKey>> rangeMappings = this.rsm.GetMappingsForRange(null, shard);

            stopwatch.Stop();

            getTracer().TraceInfo(TraceSourceConstants.ComponentNames.RangeShardMap, "GetMappings", "Complete; Shard: {0}; Duration: {1}", shard.Location, stopwatch.Elapsed);

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
    public IReadOnlyList<RangeMapping<TKey>> GetMappings(Range<TKey> range, Shard shard) {
        ExceptionUtils.DisallowNullArgument(range, "range");
        ExceptionUtils.DisallowNullArgument(shard, "shard");

        try (ActivityIdScope activityIdScope = new ActivityIdScope(UUID.randomUUID())) {
            getTracer().TraceInfo(TraceSourceConstants.ComponentNames.RangeShardMap, "GetMappings", "Start; Shard: {0}; Range: {1}", shard.Location, range);

            Stopwatch stopwatch = Stopwatch.StartNew();

            IReadOnlyList<RangeMapping<TKey>> rangeMappings = this.rsm.GetMappingsForRange(range, shard);

            stopwatch.Stop();

            getTracer().TraceInfo(TraceSourceConstants.ComponentNames.RangeShardMap, "GetMappings", "Complete; Shard: {0}; Duration: {1}", shard.Location, stopwatch.Elapsed);

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
            getTracer().TraceInfo(TraceSourceConstants.ComponentNames.RangeShardMap, "MarkMappingOffline", "Start; ");

            Stopwatch stopwatch = Stopwatch.StartNew();

            RangeMapping<TKey> result = this.rsm.MarkMappingOffline(mapping, mappingLockToken.LockOwnerId);

            stopwatch.Stop();

            getTracer().TraceInfo(TraceSourceConstants.ComponentNames.RangeShardMap, "MarkMappingOffline", "Complete; Duration: {0}", stopwatch.Elapsed);

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
            getTracer().TraceInfo(TraceSourceConstants.ComponentNames.RangeShardMap, "MarkMappingOnline", "Start; ");

            Stopwatch stopwatch = Stopwatch.StartNew();

            RangeMapping<TKey> result = this.rsm.MarkMappingOnline(mapping, mappingLockToken.LockOwnerId);

            stopwatch.Stop();

            getTracer().TraceInfo(TraceSourceConstants.ComponentNames.RangeShardMap, "MarkMappingOnline", "Complete; Duration: {0}", stopwatch.Elapsed);

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
            getTracer().TraceInfo(TraceSourceConstants.ComponentNames.RangeShardMap, "LookupLockOwner", "Start");

            Stopwatch stopwatch = Stopwatch.StartNew();

            UUID storeLockOwnerId = this.rsm.GetLockOwnerForMapping(mapping);

            stopwatch.Stop();

            getTracer().TraceInfo(TraceSourceConstants.ComponentNames.RangeShardMap, "LookupLockOwner", "Complete; Duration: {0}; StoreLockOwnerId: {1}", stopwatch.Elapsed, storeLockOwnerId);

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
            UUID lockOwnerId = mappingLockToken.LockOwnerId;

            getTracer().TraceInfo(TraceSourceConstants.ComponentNames.RangeShardMap, "Lock", "Start; LockOwnerId: {0}", lockOwnerId);

            Stopwatch stopwatch = Stopwatch.StartNew();

            this.rsm.LockOrUnlockMappings(mapping, lockOwnerId, LockOwnerIdOpType.Lock);

            stopwatch.Stop();

            getTracer().TraceInfo(TraceSourceConstants.ComponentNames.RangeShardMap, "Lock", "Complete; Duration: {0}; StoreLockOwnerId: {1}", stopwatch.Elapsed, lockOwnerId);
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
            UUID lockOwnerId = mappingLockToken.LockOwnerId;
            getTracer().TraceInfo(TraceSourceConstants.ComponentNames.RangeShardMap, "Unlock", "Start; LockOwnerId: {0}", lockOwnerId);

            Stopwatch stopwatch = Stopwatch.StartNew();

            this.rsm.LockOrUnlockMappings(mapping, lockOwnerId, LockOwnerIdOpType.UnlockMappingForId);

            stopwatch.Stop();

            getTracer().TraceInfo(TraceSourceConstants.ComponentNames.RangeShardMap, "UnLock", "Complete; Duration: {0}; StoreLockOwnerId: {1}", stopwatch.Elapsed, lockOwnerId);
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
            UUID lockOwnerId = mappingLockToken.LockOwnerId;
            getTracer().TraceInfo(TraceSourceConstants.ComponentNames.RangeShardMap, "UnlockAllMappingsWithLockOwnerId", "Start; LockOwnerId: {0}", lockOwnerId);

            Stopwatch stopwatch = Stopwatch.StartNew();

            this.rsm.LockOrUnlockMappings(null, lockOwnerId, LockOwnerIdOpType.UnlockAllMappingsForId);

            stopwatch.Stop();

            getTracer().TraceInfo(TraceSourceConstants.ComponentNames.RangeShardMap, "UnlockAllMappingsWithLockOwnerId", "Complete; Duration: {0}", stopwatch.Elapsed);
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
            getTracer().TraceInfo(TraceSourceConstants.ComponentNames.RangeShardMap, "UpdateMapping", "Start; Current mapping shard: {0}", currentMapping.Shard.Location);

            Stopwatch stopwatch = Stopwatch.StartNew();

            RangeMapping<TKey> rangeMapping = this.rsm.Update(currentMapping, update, mappingLockToken.LockOwnerId);

            stopwatch.Stop();

            getTracer().TraceInfo(TraceSourceConstants.ComponentNames.RangeShardMap, "UpdateMapping", "Complete; Current mapping shard: {0}; Duration: {1}", currentMapping.Shard.Location, stopwatch.Elapsed);

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
    public IReadOnlyList<RangeMapping<TKey>> SplitMapping(RangeMapping<TKey> existingMapping, TKey splitAt) {
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
    public IReadOnlyList<RangeMapping<TKey>> SplitMapping(RangeMapping<TKey> existingMapping, TKey splitAt, MappingLockToken mappingLockToken) {
        ExceptionUtils.DisallowNullArgument(existingMapping, "existingMapping");
        ExceptionUtils.DisallowNullArgument(mappingLockToken, "mappingLockToken");

        try (ActivityIdScope activityIdScope = new ActivityIdScope(UUID.randomUUID())) {
            getTracer().TraceInfo(TraceSourceConstants.ComponentNames.RangeShardMap, "SplitMapping", "Start; Shard: {0}", existingMapping.Shard.Location);

            Stopwatch stopwatch = Stopwatch.StartNew();

            IReadOnlyList<RangeMapping<TKey>> rangeMapping = this.rsm.Split(existingMapping, splitAt, mappingLockToken.LockOwnerId);

            stopwatch.Stop();

            getTracer().TraceInfo(TraceSourceConstants.ComponentNames.RangeShardMap, "SplitMapping", "Complete; Shard: {0}; Duration: {1}", existingMapping.Shard.Location, stopwatch.Elapsed);

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
            getTracer().TraceInfo(TraceSourceConstants.ComponentNames.RangeShardMap, "SplitMapping", "Start; Left Shard: {0}; Right Shard: {1}", left.Shard.Location, right.Shard.Location);

            Stopwatch stopwatch = Stopwatch.StartNew();

            RangeMapping<TKey> rangeMapping = this.rsm.Merge(left, right, leftMappingLockToken.LockOwnerId, rightMappingLockToken.LockOwnerId);

            stopwatch.Stop();

            getTracer().TraceInfo(TraceSourceConstants.ComponentNames.RangeShardMap, "SplitMapping", "Complete; Duration: {0}", stopwatch.Elapsed);

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
    public <V> IShardMapper<V> GetMapper() {
        return (IShardMapper<V>) ((this.rsm instanceof IShardMapper<V>) ? this.rsm : null);
    }

    ///#region ICloneable<ShardMap>

    /**
     * Clones the given range shard map.
     *
     * @return A cloned instance of the range shard map.
     */
    public RangeShardMap<TKey> Clone() {
        ShardMap tempVar = this.CloneCore();
        return (RangeShardMap<TKey>) ((tempVar instanceof RangeShardMap<TKey>) ? tempVar : null);
    }

    /**
     * Clones the given shard map.
     *
     * @return A cloned instance of the shard map.
     */
    public ShardMap Clone() {
        return this.CloneCore();
    }

    ///#endregion ICloneable<ShardMap>

    /**
     * Clones the current shard map instance.
     *
     * @return Cloned shard map instance.
     */
    @Override
    protected ShardMap CloneCore() {
        return new RangeShardMap<TKey>(this.Manager, this.StoreShardMap);
    }
}
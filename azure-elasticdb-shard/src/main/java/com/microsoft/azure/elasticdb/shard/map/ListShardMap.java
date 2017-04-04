package com.microsoft.azure.elasticdb.shard.map;

// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

import com.microsoft.azure.elasticdb.shard.mapmanager.ShardMapManager;
import com.microsoft.azure.elasticdb.shard.store.IStoreShardMap;

import java.util.*;

/**
 * Represents a shard map of points where points are of the specified key.
 * <p>
 * <typeparam name="TKey">Key type.</typeparam>
 */
public final class ListShardMap<TKey> extends ShardMap implements Cloneable<ShardMap>, Cloneable<ListShardMap<TKey>> {
    /**
     * Mapper b/w points and shards.
     */
    private ListShardMapper<TKey> _lsm;

    /**
     * Constructs a new instance.
     *
     * @param manager Reference to ShardMapManager.
     * @param ssm     Storage representation.
     */
    public ListShardMap(ShardMapManager manager, IStoreShardMap ssm) {
        super(manager, ssm);
        assert manager != null;
        assert ssm != null;

        _lsm = new ListShardMapper<TKey>(this.Manager, this);
    }

    ///#region Sync OpenConnection Methods

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
    public SqlConnection OpenConnectionForKey(TKey key, String connectionString) {
        return this.OpenConnectionForKey(key, connectionString, ConnectionOptions.Validate);
    }

    ///#endregion

    ///#region Async OpenConnection Methods

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
    public SqlConnection OpenConnectionForKey(TKey key, String connectionString, ConnectionOptions options) {
        ExceptionUtils.DisallowNullArgument(connectionString, "connectionString");

        try (ActivityIdScope activityIdScope = new ActivityIdScope(UUID.randomUUID())) {
            return _lsm.OpenConnectionForKey(key, connectionString, options);
        }
    }

    /**
     * Asynchronously opens a regular <see cref="SqlConnection"/> to the shard
     * to which the specified key value is mapped, with <see cref="ConnectionOptions.Validate"/>.
     *
     * @param key              Input key value.
     * @param connectionString Connection string with credential information such as SQL Server credentials or Integrated Security settings.
     *                         The hostname of the server and the database name for the shard are obtained from the lookup operation for key.
     * @return A Task encapsulating an open SqlConnection as the result
     * <p>
     * Note that the <see cref="SqlConnection"/> object returned by this call is not protected against transient faults.
     * Callers should follow best practices to protect the connection against transient faults
     * in their application code, e.g., by using the transient fault handling
     * functionality in the Enterprise Library from Microsoft Patterns and Practices team.
     * All non-usage error related exceptions are reported via the returned Task.
     */
    public Task<SqlConnection> OpenConnectionForKeyAsync(TKey key, String connectionString) {
        return this.OpenConnectionForKeyAsync(key, connectionString, ConnectionOptions.Validate);
    }

    ///#endregion

//#if FUTUREWORK

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
     * All non-usage error related exceptions are reported via the returned Task.
     */
    public Task<SqlConnection> OpenConnectionForKeyAsync(TKey key, String connectionString, ConnectionOptions options) {
        ExceptionUtils.DisallowNullArgument(connectionString, "connectionString");

        try (ActivityIdScope activityIdScope = new ActivityIdScope(UUID.randomUUID())) {
            return _lsm.OpenConnectionForKeyAsync(key, connectionString, options);
        }
    }
//#endif

    /**
     * Creates and adds many point mappings to ShardMap.
     *
     * @param argsList List of objects containing information about mappings to be added.
     */
    public List<PointMapping<TKey>> CreateFromPointMappings(List<PointMappingCreationArgs<TKey>> argsList) {
        ExceptionUtils.DisallowNullArgument(argsList, "argsList");

        // Partition the mappings by shardlocation.
        Map<ShardLocation, List<PointMapping<TKey>>> pointMappings = new HashMap<ShardLocation, List<PointMapping<TKey>>>();
        for (PointMappingCreationArgs<TKey> args : argsList) {
            ExceptionUtils.DisallowNullArgument(args, "args");
            if (!pointMappings.containsKey(args.Shard.Location)) {
                pointMappings.put(args.Shard.Location, new ArrayList<PointMapping<TKey>>());
            }
            pointMappings.get(args.Shard.Location).Add(new PointMapping<TKey>(this.Manager, this.Id, args));
        }

        // For each shardlocation bulk add all the mappings to local only.
        ConcurrentQueue<RuntimeException> exceptions = new ConcurrentQueue<RuntimeException>();
        Parallel.ForEach(pointMappings, (kvp) -> {
            try {
                this.lsm.AddLocals(kvp.Value, kvp.Key);
            } catch (RuntimeException e) {
                exceptions.Enqueue(e);
            }
        });

        if (exceptions.size() > 0) {
            throw new AggregateException(exceptions);
        }

        // Rebuild the global from locals.
        RecoveryManager recoveryManager = this.Manager.GetRecoveryManager();
        recoveryManager.RebuildShardMapManager(pointMappings.keySet());
        return pointMappings.values().SelectMany(x -> x.AsEnumerable());
    }

    /**
     * Creates and adds a point mapping to ShardMap.
     *
     * @param creationInfo Information about mapping to be added.
     * @return Newly created mapping.
     */
    public PointMapping<TKey> CreatePointMapping(PointMappingCreationInfo<TKey> creationInfo) {
        ExceptionUtils.DisallowNullArgument(creationInfo, "args");

        try (ActivityIdScope activityIdScope = new ActivityIdScope(UUID.randomUUID())) {
            Stopwatch stopwatch = Stopwatch.StartNew();

            String mappingKey = BitConverter.toString(creationInfo.Key.RawValue);
            getTracer().TraceInfo(TraceSourceConstants.ComponentNames.ListShardMap, "CreatePointMapping", "Start; ShardMap name: {0}; Point Mapping: {1} ", this.Name, mappingKey);

            PointMapping<TKey> pointMapping = _lsm.Add(new PointMapping<TKey>(this.Manager, creationInfo));

            stopwatch.Stop();

            getTracer().TraceInfo(TraceSourceConstants.ComponentNames.ListShardMap, "CreatePointMapping", "Complete; ShardMap name: {0}; Point Mapping: {1}; Duration: {2}", this.Name, mappingKey, stopwatch.Elapsed);

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
    public PointMapping<TKey> CreatePointMapping(TKey point, Shard shard) {
        ExceptionUtils.DisallowNullArgument(shard, "shard");

        try (ActivityIdScope activityIdScope = new ActivityIdScope(UUID.randomUUID())) {
            PointMappingCreationInfo<TKey> args = new PointMappingCreationInfo<TKey>(point, shard, MappingStatus.Online);

            String mappingKey = BitConverter.toString(args.Key.RawValue);
            getTracer().TraceInfo(TraceSourceConstants.ComponentNames.ListShardMap, "CreatePointMapping", "Start; ShardMap name: {0}; Point Mapping: {1}", this.Name, mappingKey);

            Stopwatch stopwatch = Stopwatch.StartNew();

            PointMapping<TKey> pointMapping = _lsm.Add(new PointMapping<TKey>(this.Manager, args));

            stopwatch.Stop();

            getTracer().TraceInfo(TraceSourceConstants.ComponentNames.ListShardMap, "CreatePointMapping", "Complete; ShardMap name: {0}; Point Mapping: {1}; Duration: {2}", this.Name, mappingKey, stopwatch.Elapsed);

            return pointMapping;
        }
    }

    /**
     * Removes a point mapping.
     *
     * @param mapping Mapping being removed.
     */
    public void DeleteMapping(PointMapping<TKey> mapping) {
        ExceptionUtils.DisallowNullArgument(mapping, "mapping");

        try (ActivityIdScope activityIdScope = new ActivityIdScope(UUID.randomUUID())) {
            String mappingKey = BitConverter.toString(mapping.Key.RawValue);
            getTracer().TraceInfo(TraceSourceConstants.ComponentNames.ListShardMap, "DeletePointMapping", "Start; ShardMap name: {0}; Point Mapping: {1}", this.Name, mappingKey);

            Stopwatch stopwatch = Stopwatch.StartNew();

            _lsm.Remove(mapping);

            stopwatch.Stop();

            getTracer().TraceInfo(TraceSourceConstants.ComponentNames.ListShardMap, "DeletePointMapping", "Completed; ShardMap name: {0}; Point Mapping: {1}; Duration: {2}", this.Name, mappingKey, stopwatch.Elapsed);
        }
    }

    /**
     * Looks up the key value and returns the corresponding mapping.
     *
     * @param key Input key value.
     * @return Mapping that contains the key value.
     */
    public PointMapping<TKey> GetMappingForKey(TKey key) {
        try (ActivityIdScope activityIdScope = new ActivityIdScope(UUID.randomUUID())) {
            getTracer().TraceInfo(TraceSourceConstants.ComponentNames.ListShardMap, "LookupPointMapping", "Start; ShardMap name: {0}; Point Mapping Key Type: {1}", this.Name, TKey.class);

            Stopwatch stopwatch = Stopwatch.StartNew();

            PointMapping<TKey> pointMapping = _lsm.Lookup(key, false);

            stopwatch.Stop();

            getTracer().TraceInfo(TraceSourceConstants.ComponentNames.ListShardMap, "LookupPointMapping", "Complete; ShardMap name: {0}; Point Mapping Key Type: {1}; Duration: {2}", this.Name, TKey.class, stopwatch.Elapsed);

            return pointMapping;
        }
    }

    /**
     * Tries to looks up the key value and place the corresponding mapping in <paramref name="pointMapping"/>.
     *
     * @param key          Input key value.
     * @param pointMapping Mapping that contains the key value.
     * @return <c>true</c> if mapping is found, <c>false</c> otherwise.
     */
    public boolean TryGetMappingForKey(TKey key, ReferenceObjectHelper<PointMapping<TKey>> pointMapping) {
        try (ActivityIdScope activityIdScope = new ActivityIdScope(UUID.randomUUID())) {
            getTracer().TraceInfo(TraceSourceConstants.ComponentNames.ListShardMap, "TryLookupPointMapping", "Start; ShardMap name: {0}; Point Mapping Key Type: {1}", this.Name, TKey.class);

            Stopwatch stopwatch = Stopwatch.StartNew();

            boolean result = _lsm.TryLookup(key, false, pointMapping);

            stopwatch.Stop();

            getTracer().TraceInfo(TraceSourceConstants.ComponentNames.ListShardMap, "TryLookupPointMapping", "Complete; ShardMap name: {0}; Point Mapping Key Type: {1}; Duration: {2}", this.Name, TKey.class, stopwatch.Elapsed);

            return result;
        }
    }

    /**
     * Gets all the point mappings for the shard map.
     *
     * @return Read-only collection of all point mappings on the shard map.
     */
    public IReadOnlyList<PointMapping<TKey>> GetMappings() {
        try (ActivityIdScope activityIdScope = new ActivityIdScope(UUID.randomUUID())) {
            getTracer().TraceInfo(TraceSourceConstants.ComponentNames.ListShardMap, "GetPointMappings", "Start;");

            Stopwatch stopwatch = Stopwatch.StartNew();

            IReadOnlyList<PointMapping<TKey>> pointMappings = _lsm.GetMappingsForRange(null, null);

            stopwatch.Stop();

            getTracer().TraceInfo(TraceSourceConstants.ComponentNames.ListShardMap, "GetPointMappings", "Complete; Duration: {0}", stopwatch.Elapsed);

            return pointMappings;
        }
    }

    /**
     * Gets all the mappings that exist within given range.
     *
     * @param range Point value, any mapping overlapping with the range will be returned.
     * @return Read-only collection of mappings that satisfy the given range constraint.
     */
    public IReadOnlyList<PointMapping<TKey>> GetMappings(Range<TKey> range) {
        ExceptionUtils.DisallowNullArgument(range, "range");

        try (ActivityIdScope activityIdScope = new ActivityIdScope(UUID.randomUUID())) {
            getTracer().TraceInfo(TraceSourceConstants.ComponentNames.ListShardMap, "GetPointMappings", "Start; Range: {0}", range);

            Stopwatch stopwatch = Stopwatch.StartNew();

            IReadOnlyList<PointMapping<TKey>> pointMappings = _lsm.GetMappingsForRange(range, null);

            stopwatch.Stop();

            getTracer().TraceInfo(TraceSourceConstants.ComponentNames.ListShardMap, "GetPointMappings", "Complete; Range: {0}; Duration: {1}", range, stopwatch.Elapsed);

            return pointMappings;
        }
    }

    /**
     * Gets all the mappings that exist for the given shard.
     *
     * @param shard Shard for which the mappings will be returned.
     * @return Read-only collection of mappings that satisfy the given shard constraint.
     */
    public IReadOnlyList<PointMapping<TKey>> GetMappings(Shard shard) {
        ExceptionUtils.DisallowNullArgument(shard, "shard");

        try (ActivityIdScope activityIdScope = new ActivityIdScope(UUID.randomUUID())) {
            getTracer().TraceInfo(TraceSourceConstants.ComponentNames.ListShardMap, "GetPointMappings", "Start; Shard: {0}", shard.Location);

            Stopwatch stopwatch = Stopwatch.StartNew();

            IReadOnlyList<PointMapping<TKey>> pointMappings = _lsm.GetMappingsForRange(null, shard);

            stopwatch.Stop();

            getTracer().TraceInfo(TraceSourceConstants.ComponentNames.ListShardMap, "GetPointMappings", "Complete; Shard: {0}; Duration: {1}", shard.Location, stopwatch.Elapsed);

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
    public IReadOnlyList<PointMapping<TKey>> GetMappings(Range<TKey> range, Shard shard) {
        ExceptionUtils.DisallowNullArgument(range, "range");
        ExceptionUtils.DisallowNullArgument(shard, "shard");

        try (ActivityIdScope activityIdScope = new ActivityIdScope(UUID.randomUUID())) {
            getTracer().TraceInfo(TraceSourceConstants.ComponentNames.ListShardMap, "GetPointMappings", "Start; Shard: {0}; Range: {1}", shard.Location, range);

            Stopwatch stopwatch = Stopwatch.StartNew();

            IReadOnlyList<PointMapping<TKey>> pointMappings = _lsm.GetMappingsForRange(range, shard);

            stopwatch.Stop();

            getTracer().TraceInfo(TraceSourceConstants.ComponentNames.ListShardMap, "GetPointMappings", "Complete; Shard: {0}; Duration: {1}", shard.Location, stopwatch.Elapsed);

            return pointMappings;
        }
    }

    /**
     * Marks the specified mapping offline.
     *
     * @param mapping Input point mapping.
     * @return An offline mapping.
     */
    public PointMapping<TKey> MarkMappingOffline(PointMapping<TKey> mapping) {
        ExceptionUtils.DisallowNullArgument(mapping, "mapping");

        try (ActivityIdScope activityIdScope = new ActivityIdScope(UUID.randomUUID())) {
            getTracer().TraceInfo(TraceSourceConstants.ComponentNames.ListShardMap, "MarkMappingOffline", "Start; ");

            Stopwatch stopwatch = Stopwatch.StartNew();

            PointMapping<TKey> result = _lsm.MarkMappingOffline(mapping);

            stopwatch.Stop();

            getTracer().TraceInfo(TraceSourceConstants.ComponentNames.ListShardMap, "MarkMappingOffline", "Complete; Duration: {0}", stopwatch.Elapsed);

            return result;
        }
    }

    /**
     * Marks the specified mapping online.
     *
     * @param mapping Input point mapping.
     * @return An online mapping.
     */
    public PointMapping<TKey> MarkMappingOnline(PointMapping<TKey> mapping) {
        ExceptionUtils.DisallowNullArgument(mapping, "mapping");

        try (ActivityIdScope activityIdScope = new ActivityIdScope(UUID.randomUUID())) {
            getTracer().TraceInfo(TraceSourceConstants.ComponentNames.ListShardMap, "MarkMappingOnline", "Start; ");

            Stopwatch stopwatch = Stopwatch.StartNew();

            PointMapping<TKey> result = _lsm.MarkMappingOnline(mapping);

            stopwatch.Stop();

            getTracer().TraceInfo(TraceSourceConstants.ComponentNames.ListShardMap, "MarkMappingOnline", "Complete; Duration: {0}", stopwatch.Elapsed);

            return result;
        }
    }

    /**
     * Updates a <see cref="PointMapping{TKey}"/> with the updates provided in
     * the <paramref name="update"/> parameter.
     *
     * @param currentMapping Mapping being updated.
     * @param update         Updated properties of the mapping.
     * @return New instance of mapping with updated information.
     */
    public PointMapping<TKey> UpdateMapping(PointMapping<TKey> currentMapping, PointMappingUpdate update) {
        return this.UpdateMapping(currentMapping, update, MappingLockToken.NoLock);
    }

    /**
     * Updates a point mapping with the changes provided in
     * the <paramref name="update"/> parameter.
     *
     * @param currentMapping   Mapping being updated.
     * @param update           Updated properties of the Shard.
     * @param mappingLockToken An instance of <see cref="MappingLockToken"/>
     * @return New instance of mapping with updated information.
     */
    public PointMapping<TKey> UpdateMapping(PointMapping<TKey> currentMapping, PointMappingUpdate update, MappingLockToken mappingLockToken) {
        ExceptionUtils.DisallowNullArgument(currentMapping, "currentMapping");
        ExceptionUtils.DisallowNullArgument(update, "update");
        ExceptionUtils.DisallowNullArgument(mappingLockToken, "mappingLockToken");

        try (ActivityIdScope activityIdScope = new ActivityIdScope(UUID.randomUUID())) {
            String mappingKey = BitConverter.toString(currentMapping.Key.RawValue);
            getTracer().TraceInfo(TraceSourceConstants.ComponentNames.ListShardMap, "UpdatePointMapping", "Start; ShardMap name: {0}; Current Point Mapping: {1}", this.Name, mappingKey);

            Stopwatch stopwatch = Stopwatch.StartNew();

            PointMapping<TKey> pointMapping = _lsm.Update(currentMapping, update, mappingLockToken.LockOwnerId);

            stopwatch.Stop();

            getTracer().TraceInfo(TraceSourceConstants.ComponentNames.ListShardMap, "UpdatePointMapping", "Complete; ShardMap name: {0}; Current Point Mapping: {1}; Duration: {2}", this.Name, mappingKey, stopwatch.Elapsed);

            return pointMapping;
        }
    }

    /**
     * Gets the lock owner id of the specified mapping.
     *
     * @param mapping Input range mapping.
     * @return An instance of <see cref="MappingLockToken"/>
     */
    public MappingLockToken GetMappingLockOwner(PointMapping<TKey> mapping) {
        ExceptionUtils.DisallowNullArgument(mapping, "mapping");

        try (ActivityIdScope activityIdScope = new ActivityIdScope(UUID.randomUUID())) {
            getTracer().TraceInfo(TraceSourceConstants.ComponentNames.ListShardMap, "LookupLockOwner", "Start");

            Stopwatch stopwatch = Stopwatch.StartNew();

            UUID storeLockOwnerId = _lsm.GetLockOwnerForMapping(mapping);

            stopwatch.Stop();

            getTracer().TraceInfo(TraceSourceConstants.ComponentNames.ListShardMap, "LookupLockOwner", "Complete; Duration: {0}; StoreLockOwnerId: {1}", stopwatch.Elapsed, storeLockOwnerId);

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
    public void LockMapping(PointMapping<TKey> mapping, MappingLockToken mappingLockToken) {
        ExceptionUtils.DisallowNullArgument(mapping, "mapping");
        ExceptionUtils.DisallowNullArgument(mappingLockToken, "mappingLockToken");

        try (ActivityIdScope activityIdScope = new ActivityIdScope(UUID.randomUUID())) {
            // Generate a lock owner id
            UUID lockOwnerId = mappingLockToken.LockOwnerId;

            getTracer().TraceInfo(TraceSourceConstants.ComponentNames.ListShardMap, "Lock", "Start; LockOwnerId: {0}", lockOwnerId);

            Stopwatch stopwatch = Stopwatch.StartNew();

            _lsm.LockOrUnlockMappings(mapping, lockOwnerId, LockOwnerIdOpType.Lock);

            stopwatch.Stop();

            getTracer().TraceInfo(TraceSourceConstants.ComponentNames.ListShardMap, "Lock", "Complete; Duration: {0}; StoreLockOwnerId: {1}", stopwatch.Elapsed, lockOwnerId);
        }
    }

    /**
     * Unlocks the specified mapping
     *
     * @param mapping          Input range mapping.
     * @param mappingLockToken An instance of <see cref="MappingLockToken"/>
     */
    public void UnlockMapping(PointMapping<TKey> mapping, MappingLockToken mappingLockToken) {
        ExceptionUtils.DisallowNullArgument(mapping, "mapping");
        ExceptionUtils.DisallowNullArgument(mappingLockToken, "mappingLockToken");

        try (ActivityIdScope activityIdScope = new ActivityIdScope(UUID.randomUUID())) {
            UUID lockOwnerId = mappingLockToken.LockOwnerId;
            getTracer().TraceInfo(TraceSourceConstants.ComponentNames.ListShardMap, "Unlock", "Start; LockOwnerId: {0}", lockOwnerId);

            Stopwatch stopwatch = Stopwatch.StartNew();

            _lsm.LockOrUnlockMappings(mapping, lockOwnerId, LockOwnerIdOpType.UnlockMappingForId);

            stopwatch.Stop();

            getTracer().TraceInfo(TraceSourceConstants.ComponentNames.ListShardMap, "UnLock", "Complete; Duration: {0}; StoreLockOwnerId: {1}", stopwatch.Elapsed, lockOwnerId);
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
            getTracer().TraceInfo(TraceSourceConstants.ComponentNames.ListShardMap, "UnlockAllMappingsWithLockOwnerId", "Start; LockOwnerId: {0}", lockOwnerId);

            Stopwatch stopwatch = Stopwatch.StartNew();

            _lsm.LockOrUnlockMappings(null, lockOwnerId, LockOwnerIdOpType.UnlockAllMappingsForId);

            stopwatch.Stop();

            getTracer().TraceInfo(TraceSourceConstants.ComponentNames.ListShardMap, "UnlockAllMappingsWithLockOwnerId", "Complete; Duration: {0}", stopwatch.Elapsed);
        }
    }

    /**
     * Gets the mapper. This method is used by OpenConnection/Lookup of V.
     * <p>
     * <typeparam name="V">Shard provider type.</typeparam>
     *
     * @return ListShardMapper for given key type.
     */
    @Override
    public <V> IShardMapper<V> GetMapper() {
        return (IShardMapper<V>) ((_lsm instanceof IShardMapper<V>) ? _lsm : null);
    }

    /**
     * Clones the specified list shard map.
     *
     * @return A cloned instance of the list shard map.
     */
    public ListShardMap<TKey> Clone() {
        ShardMap tempVar = this.CloneCore();
        return (ListShardMap<TKey>) ((tempVar instanceof ListShardMap<TKey>) ? tempVar : null);
    }

    ///#region ICloneable<ShardMap>

    /**
     * Clones the specified shard map.
     *
     * @return A cloned instance of the shard map.
     */
    public ShardMap Clone() {
        return this.CloneCore();
    }

    /**
     * Clones the current shard map instance.
     *
     * @return Cloned shard map instance.
     */
    @Override
    protected ShardMap CloneCore() {
        return new ListShardMap<TKey>(this.Manager, this.StoreShardMap);
    }

    ///#endregion ICloneable<ShardMap>
}

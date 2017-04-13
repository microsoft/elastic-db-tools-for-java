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
import com.microsoft.azure.elasticdb.shard.mapper.ListShardMapper;
import com.microsoft.azure.elasticdb.shard.store.StoreShardMap;
import com.microsoft.azure.elasticdb.shard.utils.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

/**
 * Represents a shard map of points where points are of the specified key.
 * <p>
 * <typeparam name="TKey">Key type.</typeparam>
 */
public final class ListShardMap<TKey> extends ShardMap implements Cloneable {

    final static Logger log = LoggerFactory.getLogger(ListShardMap.class);

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
    public ListShardMap(ShardMapManager manager, StoreShardMap ssm) {
        super(manager, ssm);
        _lsm = new ListShardMapper<TKey>(manager, this);
    }

    ///#region Sync OpenConnection Methods

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
    public SQLServerConnection OpenConnectionForKey(TKey key, String connectionString) {
        return this.OpenConnectionForKey(key, connectionString, ConnectionOptions.Validate);
    }*/

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
    /*@Override
    public SQLServerConnection OpenConnectionForKey(TKey key, String connectionString, ConnectionOptions options) {
        ExceptionUtils.DisallowNullArgument(connectionString, "connectionString");

        try (ActivityIdScope activityIdScope = new ActivityIdScope(UUID.randomUUID())) {
            return _lsm.OpenConnectionForKey(key, connectionString, options);
        }
    }*/

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
     * All non-usage error related exceptions are reported via the returned Task.
     */
    /*@Override
    public Callable<SQLServerConnection> OpenConnectionForKeyAsync(TKey key, String connectionString, ConnectionOptions options) {
        ExceptionUtils.DisallowNullArgument(connectionString, "connectionString");

        try (ActivityIdScope activityIdScope = new ActivityIdScope(UUID.randomUUID())) {
            return _lsm.OpenConnectionForKeyAsync(key, connectionString, options);
        }
    }*/

    /**
     * Creates and adds a point mapping to ShardMap.
     *
     * @param creationInfo Information about mapping to be added.
     * @return Newly created mapping.
     */
    public PointMapping<TKey> CreatePointMapping(PointMappingCreationInfo<TKey> creationInfo) {
        ExceptionUtils.DisallowNullArgument(creationInfo, "args");

        try (ActivityIdScope activityIdScope = new ActivityIdScope(UUID.randomUUID())) {
            Stopwatch stopwatch = Stopwatch.createStarted();

            String mappingKey = creationInfo.getKey().getRawValue().toString();
            log.info("CreatePointMapping Start; ShardMap name: {}; Point Mapping: {} ", this.getName(), mappingKey);

            PointMapping<TKey> pointMapping = _lsm.Add(new PointMapping<TKey>(this.getShardMapManager(), creationInfo));

            stopwatch.stop();

            log.info("CreatePointMapping Complete; ShardMap name: {}; Point Mapping: {}; Duration: {}", this.getName(), mappingKey, stopwatch.elapsed(TimeUnit.MILLISECONDS));

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

            String mappingKey = args.getKey().getRawValue().toString();
            log.info("CreatePointMapping Start; ShardMap name: {}; Point Mapping: {}", this.getName(), mappingKey);

            Stopwatch stopwatch = Stopwatch.createStarted();

            PointMapping<TKey> pointMapping = _lsm.Add(new PointMapping<TKey>(this.getShardMapManager(), args));

            stopwatch.stop();

            log.info("CreatePointMapping Complete; ShardMap name: {}; Point Mapping: {}; Duration: {}", this.getName(), mappingKey, stopwatch.elapsed(TimeUnit.MILLISECONDS));

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
            String mappingKey = mapping.getKey().getRawValue().toString();
            log.info("DeletePointMapping Start; ShardMap name: {}; Point Mapping: {}", this.getName(), mappingKey);

            Stopwatch stopwatch = Stopwatch.createStarted();

            _lsm.Remove(mapping);

            stopwatch.stop();

            log.info("DeletePointMapping Completed; ShardMap name: {}; Point Mapping: {}; Duration: {}", this.getName(), mappingKey, stopwatch.elapsed(TimeUnit.MILLISECONDS));
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
            log.info("LookupPointMapping", "Start; ShardMap name: {0}; Point Mapping Key Type: {1}", this.getName(), key.getClass());

            Stopwatch stopwatch = Stopwatch.createStarted();

            PointMapping<TKey> pointMapping = _lsm.Lookup(key, false);

            stopwatch.stop();

            log.info("LookupPointMapping", "Complete; ShardMap name: {0}; Point Mapping Key Type: {1}; Duration: {2}", this.getName(), key.getClass(), stopwatch.elapsed(TimeUnit.MILLISECONDS));

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
            log.info("TryLookupPointMapping", "Start; ShardMap name: {0}; Point Mapping Key Type: {1}", this.getName(), key.getClass());

            Stopwatch stopwatch = Stopwatch.createStarted();

            boolean result = _lsm.TryLookup(key, false, pointMapping);

            stopwatch.stop();

            log.info("TryLookupPointMapping", "Complete; ShardMap name: {0}; Point Mapping Key Type: {1}; Duration: {2}", this.getName(), key.getClass(), stopwatch.elapsed(TimeUnit.MILLISECONDS));

            return result;
        }
    }

    /**
     * Gets all the point mappings for the shard map.
     *
     * @return Read-only collection of all point mappings on the shard map.
     */
    public List<PointMapping<TKey>> GetMappings() {
        try (ActivityIdScope activityIdScope = new ActivityIdScope(UUID.randomUUID())) {
            log.info("GetPointMappings", "Start;");

            Stopwatch stopwatch = Stopwatch.createStarted();

            List<PointMapping<TKey>> pointMappings = _lsm.GetMappingsForRange(null, null);

            stopwatch.stop();

            log.info("GetPointMappings", "Complete; Duration: {0}", stopwatch.elapsed(TimeUnit.MILLISECONDS));

            return pointMappings;
        }
    }

    /**
     * Gets all the mappings that exist within given range.
     *
     * @param range Point value, any mapping overlapping with the range will be returned.
     * @return Read-only collection of mappings that satisfy the given range constraint.
     */
    public List<PointMapping<TKey>> GetMappings(Range<TKey> range) {
        ExceptionUtils.DisallowNullArgument(range, "range");

        try (ActivityIdScope activityIdScope = new ActivityIdScope(UUID.randomUUID())) {
            log.info("GetPointMappings", "Start; Range: {0}", range);

            Stopwatch stopwatch = Stopwatch.createStarted();

            List<PointMapping<TKey>> pointMappings = _lsm.GetMappingsForRange(range, null);

            stopwatch.stop();

            log.info("GetPointMappings", "Complete; Range: {0}; Duration: {1}", range, stopwatch.elapsed(TimeUnit.MILLISECONDS));

            return pointMappings;
        }
    }

    /**
     * Gets all the mappings that exist for the given shard.
     *
     * @param shard Shard for which the mappings will be returned.
     * @return Read-only collection of mappings that satisfy the given shard constraint.
     */
    public List<PointMapping<TKey>> GetMappings(Shard shard) {
        ExceptionUtils.DisallowNullArgument(shard, "shard");

        try (ActivityIdScope activityIdScope = new ActivityIdScope(UUID.randomUUID())) {
            log.info("GetPointMappings", "Start; Shard: {0}", shard.getLocation());

            Stopwatch stopwatch = Stopwatch.createStarted();

            List<PointMapping<TKey>> pointMappings = _lsm.GetMappingsForRange(null, shard);

            stopwatch.stop();

            log.info("GetPointMappings", "Complete; Shard: {0}; Duration: {1}", shard.getLocation(), stopwatch.elapsed(TimeUnit.MILLISECONDS));

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
    public List<PointMapping<TKey>> GetMappings(Range<TKey> range, Shard shard) {
        ExceptionUtils.DisallowNullArgument(range, "range");
        ExceptionUtils.DisallowNullArgument(shard, "shard");

        try (ActivityIdScope activityIdScope = new ActivityIdScope(UUID.randomUUID())) {
            log.info("GetPointMappings", "Start; Shard: {0}; Range: {1}", shard.getLocation(), range);

            Stopwatch stopwatch = Stopwatch.createStarted();

            List<PointMapping<TKey>> pointMappings = _lsm.GetMappingsForRange(range, shard);

            stopwatch.stop();

            log.info("GetPointMappings", "Complete; Shard: {0}; Duration: {1}", shard.getLocation(), stopwatch.elapsed(TimeUnit.MILLISECONDS));

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
            log.info("MarkMappingOffline", "Start; ");

            Stopwatch stopwatch = Stopwatch.createStarted();

            PointMapping<TKey> result = _lsm.MarkMappingOffline(mapping);

            stopwatch.stop();

            log.info("MarkMappingOffline", "Complete; Duration: {0}", stopwatch.elapsed(TimeUnit.MILLISECONDS));

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
            log.info("MarkMappingOnline", "Start; ");

            Stopwatch stopwatch = Stopwatch.createStarted();

            PointMapping<TKey> result = _lsm.MarkMappingOnline(mapping);

            stopwatch.stop();

            log.info("MarkMappingOnline", "Complete; Duration: {0}", stopwatch.elapsed(TimeUnit.MILLISECONDS));

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
            String mappingKey = currentMapping.getKey().getRawValue().toString();
            log.info("UpdatePointMapping", "Start; ShardMap name: {0}; Current Point Mapping: {1}", this.getName(), mappingKey);

            Stopwatch stopwatch = Stopwatch.createStarted();

            PointMapping<TKey> pointMapping = _lsm.Update(currentMapping, update, mappingLockToken.getLockOwnerId());

            stopwatch.stop();

            log.info("UpdatePointMapping", "Complete; ShardMap name: {0}; Current Point Mapping: {1}; Duration: {2}", this.getName(), mappingKey, stopwatch.elapsed(TimeUnit.MILLISECONDS));

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
            log.info("LookupLockOwner", "Start");

            Stopwatch stopwatch = Stopwatch.createStarted();

            UUID storeLockOwnerId = _lsm.GetLockOwnerForMapping(mapping);

            stopwatch.stop();

            log.info("LookupLockOwner", "Complete; Duration: {0}; StoreLockOwnerId: {1}", stopwatch.elapsed(TimeUnit.MILLISECONDS), storeLockOwnerId);

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
            UUID lockOwnerId = mappingLockToken.getLockOwnerId();

            log.info("Lock", "Start; LockOwnerId: {0}", lockOwnerId);

            Stopwatch stopwatch = Stopwatch.createStarted();

            _lsm.LockOrUnlockMappings(mapping, lockOwnerId, LockOwnerIdOpType.Lock);

            stopwatch.stop();

            log.info("Lock", "Complete; Duration: {0}; StoreLockOwnerId: {1}", stopwatch.elapsed(TimeUnit.MILLISECONDS), lockOwnerId);
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
            UUID lockOwnerId = mappingLockToken.getLockOwnerId();
            log.info("Unlock", "Start; LockOwnerId: {0}", lockOwnerId);

            Stopwatch stopwatch = Stopwatch.createStarted();

            _lsm.LockOrUnlockMappings(mapping, lockOwnerId, LockOwnerIdOpType.UnlockMappingForId);

            stopwatch.stop();

            log.info("UnLock", "Complete; Duration: {0}; StoreLockOwnerId: {1}", stopwatch.elapsed(TimeUnit.MILLISECONDS), lockOwnerId);
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
            log.info("UnlockAllMappingsWithLockOwnerId", "Start; LockOwnerId: {0}", lockOwnerId);

            Stopwatch stopwatch = Stopwatch.createStarted();

            _lsm.LockOrUnlockMappings(null, lockOwnerId, LockOwnerIdOpType.UnlockAllMappingsForId);

            stopwatch.stop();

            log.info("UnlockAllMappingsWithLockOwnerId", "Complete; Duration: {0}", stopwatch.elapsed(TimeUnit.MILLISECONDS));
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
    public <V> IShardMapper1<V> GetMapper() {
        return (IShardMapper1<V>) ((_lsm instanceof IShardMapper) ? _lsm : null);
    }

    /**
     * Clones the specified list shard map.
     *
     * @return A cloned instance of the list shard map.
     */
    /*public ListShardMap<TKey> clone() {
        ShardMap tempVar = this.CloneCore();
        return (ListShardMap<TKey>) ((tempVar instanceof ListShardMap<TKey>) ? tempVar : null);
    }*/

    ///#region ICloneable<ShardMap>

    /**
     * Clones the specified shard map.
     *
     * @return A cloned instance of the shard map.
     */
    public ShardMap clone() {
        return this.CloneCore();
    }

    /**
     * Clones the current shard map instance.
     *
     * @return Cloned shard map instance.
     */
    @Override
    protected ShardMap CloneCore() {
        return new ListShardMap<TKey>(shardMapManager, storeShardMap);
    }

    ///#endregion ICloneable<ShardMap>
}

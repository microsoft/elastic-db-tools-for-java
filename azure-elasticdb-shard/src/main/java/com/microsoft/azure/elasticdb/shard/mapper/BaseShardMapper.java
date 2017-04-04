package com.microsoft.azure.elasticdb.shard.mapper;

// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

import com.microsoft.azure.elasticdb.core.commons.helpers.ActionGeneric1Param;
import com.microsoft.azure.elasticdb.core.commons.logging.ILogger;
import com.microsoft.azure.elasticdb.core.commons.logging.TraceHelper;
import com.microsoft.azure.elasticdb.shard.map.ShardMap;
import com.microsoft.azure.elasticdb.shard.mapmanager.ShardMapManager;

import java.util.UUID;

/**
 * Base class for keyed mappers.
 */
public abstract class BaseShardMapper {
    /**
     * Reference to ShardMapManager.
     */
    private ShardMapManager Manager;
    /**
     * Containing shard map.
     */
    private ShardMap _shardMap;

    /**
     * Base shard mapper, which is just a holder of some fields.
     *
     * @param manager Reference to ShardMapManager.
     * @param sm      Containing shard map.
     */
    protected BaseShardMapper(ShardMapManager manager, ShardMap sm) {
        assert manager != null;
        assert sm != null;

        this.setManager(manager);
        this.setShardMap(sm);
    }

    /**
     * The Tracer
     */
    private static ILogger getTracer() {
        return TraceHelper.Tracer;
    }

    /**
     * Sets the status of a shardmapping
     * <p>
     * <typeparam name="TMapping">Mapping type.</typeparam>
     * <typeparam name="TUpdate">Update type.</typeparam>
     * <typeparam name="TStatus">Status type.</typeparam>
     *
     * @param mapping      Mapping being added.
     * @param status       Status of <paramref name="mapping">mapping</paramref> being added.
     * @param getStatus    Delegate to construct new status from
     *                     <paramref name="status">input status</paramref>.
     * @param createUpdate Delegate to construct new update from new status returned by
     *                     <paramref name="getStatus">getStatus</paramref>.
     * @param runUpdate    Delegate to perform update from the <paramref name="mapping">input mapping</paramref> and
     *                     the update object returned by <paramref name="getStatus">createUpdate</paramref>.
     * @param lockOwnerId  Lock owner id of this mapping
     * @return
     */

    protected static <TMapping, TUpdate, TStatus> TMapping SetStatus(TMapping mapping, TStatus status, Func<TStatus, TStatus> getStatus, Func<TStatus, TUpdate> createUpdate, Func<TMapping, TUpdate, UUID, TMapping> runUpdate) {
        return SetStatus(mapping, status, getStatus, createUpdate, runUpdate, default (System.Guid));
    }

    protected static <TMapping, TUpdate, TStatus> TMapping SetStatus(TMapping mapping, TStatus status, ActionGeneric1Param<TStatus, TStatus> getStatus, ActionGeneric1Param<TStatus, TUpdate> createUpdate, ActionGeneric3Param<TMapping, TUpdate, UUID, TMapping> runUpdate, UUID lockOwnerId) {
        TStatus newStatus = getStatus.invoke(status);
        TUpdate update = createUpdate.invoke(newStatus);
        return runUpdate.invoke(mapping, update, lockOwnerId);
    }

    protected final ShardMapManager getManager() {
        return Manager;
    }

    private void setManager(ShardMapManager value) {
        Manager = value;
    }

    protected final ShardMap getShardMap() {
        return _shardMap;
    }

    private void setShardMap(ShardMap value) {
        _shardMap = value;
    }

    /**
     * Given a key value, obtains a SqlConnection to the shard in the mapping
     * that contains the key value.
     * <p>
     * <typeparam name="TMapping">Mapping type.</typeparam>
     * <typeparam name="TKey">Key type.</typeparam>
     *
     * @param key              Input key value.
     * @param constructMapping Delegate to construct a mapping object.
     * @param errorCategory    Error category.
     * @param connectionString Connection string with credential information, the DataSource and Database are
     *                         obtained from the results of the lookup operation for key.
     * @param options          Options for validation operations to perform on opened connection.
     * @return An opened SqlConnection.
     */

    protected final <TMapping extends IShardProvider, TKey> SqlConnection OpenConnectionForKey(TKey key, Func<ShardMapManager, ShardMap, IStoreMapping, TMapping> constructMapping, ShardManagementErrorCategory errorCategory, String connectionString) {
        return OpenConnectionForKey(key, constructMapping, errorCategory, connectionString, ConnectionOptions.Validate);
    }

    protected final <TMapping extends IShardProvider, TKey> SqlConnection OpenConnectionForKey(TKey key, ActionGeneric3Param<ShardMapManager, ShardMap, IStoreMapping, TMapping> constructMapping, ShardManagementErrorCategory errorCategory, String connectionString, ConnectionOptions options) {
        ShardKey sk = new ShardKey(ShardKey.ShardKeyTypeFromType(TKey.class), key);

        // Try to find the mapping within the cache.
        ICacheStoreMapping csm = this.getManager().Cache.LookupMappingByKey(this.getShardMap().StoreShardMap, sk);

        IStoreMapping sm;

        if (csm != null) {
            sm = csm.Mapping;
        } else {
            sm = this.LookupMappingForOpenConnectionForKey(sk, CacheStoreMappingUpdatePolicy.OverwriteExisting, errorCategory);
        }

        SqlConnection result;

        try {
            // Initially attempt to connect based on lookup results from either cache or GSM.
            result = this.getShardMap().OpenConnection(constructMapping.invoke(this.getManager(), this.getShardMap(), sm), connectionString, options);

            // Reset TTL on successful connection.
            if (csm != null && csm.TimeToLiveMilliseconds > 0) {
                csm.ResetTimeToLive();
            }

            this.getManager().Cache.IncrementPerformanceCounter(this.getShardMap().StoreShardMap, PerformanceCounterName.DdrOperationsPerSec);
            return result;
        } catch (ShardManagementException smme) {
            // If we hit a validation failure due to stale version of mapping, we will perform one more attempt.
            if (((options & ConnectionOptions.Validate) == ConnectionOptions.Validate) && smme.ErrorCategory == ShardManagementErrorCategory.Validation && smme.ErrorCode == ShardManagementErrorCode.MappingDoesNotExist) {
                // Assumption here is that this time the attempt should succeed since the cache entry
                // has already been either evicted, or updated based on latest data from the server.
                sm = this.LookupMappingForOpenConnectionForKey(sk, CacheStoreMappingUpdatePolicy.OverwriteExisting, errorCategory);

                result = this.getShardMap().OpenConnection(constructMapping.invoke(this.getManager(), this.getShardMap(), sm), connectionString, options);
                this.getManager().Cache.IncrementPerformanceCounter(this.getShardMap().StoreShardMap, PerformanceCounterName.DdrOperationsPerSec);
                return result;
            } else {
                // The error was not due to validation but something else e.g.
                // 1) Shard map does not exist
                // 2) Mapping could not be found.
                throw smme;
            }
        } catch (SqlException e) {
            // We failed to connect. If we were trying to connect from an entry in cache and mapping expired in cache.
            if (csm != null && TimerUtils.ElapsedMillisecondsSince(csm.CreationTime) >= csm.TimeToLiveMilliseconds) {
                try (IdLock _idLock = new IdLock(csm.Mapping.StoreShard.Id)) {
                    // Similar to DCL pattern, we need to refresh the mapping again to see if we still need to go to the store
                    // to lookup the mapping after acquiring the shard lock. It might be the case that a fresh version has already
                    // been obtained by some other thread.
                    csm = this.getManager().Cache.LookupMappingByKey(this.getShardMap().StoreShardMap, sk);

                    // Only go to store if the mapping is stale even after refresh.
                    if (csm == null || TimerUtils.ElapsedMillisecondsSince(csm.CreationTime) >= csm.TimeToLiveMilliseconds) {
                        // Refresh the mapping in cache. And try to open the connection after refresh.
                        sm = this.LookupMappingForOpenConnectionForKey(sk, CacheStoreMappingUpdatePolicy.UpdateTimeToLive, errorCategory);
                    } else {
                        sm = csm.Mapping;
                    }
                }

                result = this.getShardMap().OpenConnection(constructMapping.invoke(this.getManager(), this.getShardMap(), sm), connectionString, options);

                // Reset TTL on successful connection.
                if (csm != null && csm.TimeToLiveMilliseconds > 0) {
                    csm.ResetTimeToLive();
                }

                this.getManager().Cache.IncrementPerformanceCounter(this.getShardMap().StoreShardMap, PerformanceCounterName.DdrOperationsPerSec);
                return result;
            } else {
                // Either:
                // 1) The mapping is still within the TTL. No refresh.
                // 2) Mapping was not in cache, we originally did a lookup for mapping in GSM and even then could not connect.
                throw e;
            }
        }
    }

    /**
     * Given a key value, asynchronously obtains a SqlConnection to the shard in the mapping
     * that contains the key value.
     * <p>
     * <typeparam name="TMapping">Mapping type.</typeparam>
     * <typeparam name="TKey">Key type.</typeparam>
     *
     * @param key              Input key value.
     * @param constructMapping Delegate to construct a mapping object.
     * @param errorCategory    Error category.
     * @param connectionString Connection string with credential information, the DataSource and Database are
     *                         obtained from the results of the lookup operation for key.
     * @param options          Options for validation operations to perform on opened connection.
     * @return A task encapsulating an opened SqlConnection as the result.
     */

    protected final <TMapping extends IShardProvider, TKey> Task<SqlConnection> OpenConnectionForKeyAsync(TKey key, Func<ShardMapManager, ShardMap, IStoreMapping, TMapping> constructMapping, ShardManagementErrorCategory errorCategory, String connectionString) {
        return OpenConnectionForKeyAsync(key, constructMapping, errorCategory, connectionString, ConnectionOptions.Validate);
    }

    //TODO TASK: There is no equivalent in Java to the 'async' keyword:
//ORIGINAL LINE: protected async Task<SqlConnection> OpenConnectionForKeyAsync<TMapping, TKey>(TKey key, Func<ShardMapManager, ShardMap, IStoreMapping, TMapping> constructMapping, ShardManagementErrorCategory errorCategory, string connectionString, ConnectionOptions options = ConnectionOptions.Validate) where TMapping : class, IShardProvider
    protected final <TMapping extends IShardProvider, TKey> Task<SqlConnection> OpenConnectionForKeyAsync(TKey key, ActionGeneric3Param<ShardMapManager, ShardMap, IStoreMapping, TMapping> constructMapping, ShardManagementErrorCategory errorCategory, String connectionString, ConnectionOptions options) {
        ShardKey sk = new ShardKey(ShardKey.ShardKeyTypeFromType(TKey.class), key);

        // Try to find the mapping within the cache.
        ICacheStoreMapping csm = this.getManager().Cache.LookupMappingByKey(this.getShardMap().StoreShardMap, sk);

        IStoreMapping sm;

        if (csm != null) {
            sm = csm.Mapping;
        } else {
//TODO TASK: There is no equivalent to 'await' in Java:
            sm = await
            this.LookupMappingForOpenConnectionForKeyAsync(sk, CacheStoreMappingUpdatePolicy.OverwriteExisting, errorCategory).ConfigureAwait(false);
        }

        SqlConnection result;
        boolean lookupMappingOnEx = false;
        CacheStoreMappingUpdatePolicy cacheUpdatePolicyOnEx = CacheStoreMappingUpdatePolicy.OverwriteExisting;

        try {
            // Initially attempt to connect based on lookup results from either cache or GSM.
//TODO TASK: There is no equivalent to 'await' in Java:
            result = await
            this.getShardMap().OpenConnectionAsync(constructMapping.invoke(this.getManager(), this.getShardMap(), sm), connectionString, options).ConfigureAwait(false);

            csm.ResetTimeToLiveIfNecessary();

            return result;
        } catch (ShardManagementException smme) {
            // If we hit a validation failure due to stale version of mapping, we will perform one more attempt.
            if (((options & ConnectionOptions.Validate) == ConnectionOptions.Validate) && smme.ErrorCategory == ShardManagementErrorCategory.Validation && smme.ErrorCode == ShardManagementErrorCode.MappingDoesNotExist) {
                // Assumption here is that this time the attempt should succeed since the cache entry
                // has already been either evicted, or updated based on latest data from the server.
                lookupMappingOnEx = true;
                cacheUpdatePolicyOnEx = CacheStoreMappingUpdatePolicy.OverwriteExisting;
            } else {
                // The error was not due to validation but something else e.g.
                // 1) Shard map does not exist
                // 2) Mapping could not be found.
                throw smme;
            }
        } catch (SqlException e) {
            // We failed to connect. If we were trying to connect from an entry in cache and mapping expired in cache.
            if (csm != null && csm.HasTimeToLiveExpired()) {
                try (IdLock _idLock = new IdLock(csm.Mapping.StoreShard.Id)) {
                    // Similar to DCL pattern, we need to refresh the mapping again to see if we still need to go to the store
                    // to lookup the mapping after acquiring the shard lock. It might be the case that a fresh version has already
                    // been obtained by some other thread.
                    csm = this.getManager().Cache.LookupMappingByKey(this.getShardMap().StoreShardMap, sk);

                    // Only go to store if the mapping is stale even after refresh.
                    if (csm == null || csm.HasTimeToLiveExpired()) {
                        // Refresh the mapping in cache. And try to open the connection after refresh.
                        lookupMappingOnEx = true;
                        cacheUpdatePolicyOnEx = CacheStoreMappingUpdatePolicy.UpdateTimeToLive;
                    } else {
                        sm = csm.Mapping;
                    }
                }
            } else {
                // Either:
                // 1) The mapping is still within the TTL. No refresh.
                // 2) Mapping was not in cache, we originally did a lookup for mapping in GSM and even then could not connect.
                throw e;
            }
        }

        if (lookupMappingOnEx) {
//TODO TASK: There is no equivalent to 'await' in Java:
            sm = await
            this.LookupMappingForOpenConnectionForKeyAsync(sk, cacheUpdatePolicyOnEx, errorCategory).ConfigureAwait(false);
        }

        // One last attempt to open the connection after a cache refresh
//TODO TASK: There is no equivalent to 'await' in Java:
        result = await
        this.getShardMap().OpenConnectionAsync(constructMapping.invoke(this.getManager(), this.getShardMap(), sm), connectionString, options).ConfigureAwait(false);

        // Reset TTL on successful connection.
        csm.ResetTimeToLiveIfNecessary();

        return result;
    }

    /**
     * Adds a mapping to shard map.
     * <p>
     * <typeparam name="TMapping">Mapping type.</typeparam>
     *
     * @param mapping          Mapping being added.
     * @param constructMapping Delegate to construct a mapping object.
     * @return The added mapping object.
     */
    protected final <TMapping extends IShardProvider & IMappingInfoProvider> TMapping Add(TMapping mapping, ActionGeneric3Param<ShardMapManager, ShardMap, IStoreMapping, TMapping> constructMapping) {
        ExceptionUtils.EnsureShardBelongsToShardMap(this.getManager(), this.getShardMap(), mapping.ShardInfo, "CreateMapping", mapping.Kind == MappingKind.PointMapping ? "PointMapping" : "RangeMapping");

        this.EnsureMappingBelongsToShardMap(mapping, "Add", "mapping");

        TMapping newMapping = constructMapping.invoke(this.getManager(), this.getShardMap(), new DefaultStoreMapping(mapping.StoreMapping.Id, mapping.StoreMapping.ShardMapId, new DefaultStoreShard(mapping.ShardInfo.StoreShard.Id, UUID.randomUUID(), mapping.ShardInfo.StoreShard.ShardMapId, mapping.ShardInfo.StoreShard.Location, mapping.ShardInfo.StoreShard.Status), mapping.StoreMapping.MinValue, mapping.StoreMapping.MaxValue, mapping.StoreMapping.Status, mapping.StoreMapping.LockOwnerId));

        try (IStoreOperation op = this.getManager().StoreOperationFactory.CreateAddMappingOperation(this.getManager(), mapping.Kind == MappingKind.RangeMapping ? StoreOperationCode.AddRangeMapping : StoreOperationCode.AddPointMapping, this.getShardMap().StoreShardMap, newMapping.StoreMapping)) {
            op.Do();
        }

        return newMapping;
    }

    /**
     * Removes a mapping from shard map.
     * <p>
     * <typeparam name="TMapping">Mapping type.</typeparam>
     *
     * @param mapping          Mapping being removed.
     * @param constructMapping Delegate to construct a mapping object.
     * @param lockOwnerId      Lock owner id of this mapping
     */
    protected final <TMapping extends IShardProvider & IMappingInfoProvider> void Remove(TMapping mapping, ActionGeneric3Param<ShardMapManager, ShardMap, IStoreMapping, TMapping> constructMapping, UUID lockOwnerId) {
        this.<TMapping>EnsureMappingBelongsToShardMap(mapping, "Remove", "mapping");

        TMapping newMapping = constructMapping.invoke(this.getManager(), this.getShardMap(), new DefaultStoreMapping(mapping.StoreMapping.Id, mapping.StoreMapping.ShardMapId, new DefaultStoreShard(mapping.ShardInfo.Id, UUID.randomUUID(), mapping.ShardInfo.StoreShard.ShardMapId, mapping.ShardInfo.StoreShard.Location, mapping.ShardInfo.StoreShard.Status), mapping.StoreMapping.MinValue, mapping.StoreMapping.MaxValue, mapping.StoreMapping.Status, mapping.StoreMapping.LockOwnerId));

        try (IStoreOperation op = this.getManager().StoreOperationFactory.CreateRemoveMappingOperation(this.getManager(), mapping.Kind == MappingKind.RangeMapping ? StoreOperationCode.RemoveRangeMapping : StoreOperationCode.RemovePointMapping, this.getShardMap().StoreShardMap, newMapping.StoreMapping, lockOwnerId)) {
            op.Do();
        }
    }

    /**
     * Looks up the key value and returns the corresponding mapping.
     * <p>
     * <typeparam name="TMapping">Mapping type.</typeparam>
     * <typeparam name="TKey">Key type.</typeparam>
     *
     * @param key              Input key value.
     * @param useCache         Whether to use cache for lookups.
     * @param constructMapping Delegate to construct a mapping object.
     * @param errorCategory    Category under which errors must be thrown.
     * @return Mapping that contains the key value.
     */
    protected final <TMapping extends IShardProvider, TKey> TMapping Lookup(TKey key, boolean useCache, ActionGeneric3Param<ShardMapManager, ShardMap, IStoreMapping, TMapping> constructMapping, ShardManagementErrorCategory errorCategory) {
        ShardKey sk = new ShardKey(ShardKey.ShardKeyTypeFromType(TKey.class), key);

        if (useCache) {
            ICacheStoreMapping cachedMapping = this.getManager().Cache.LookupMappingByKey(this.getShardMap().StoreShardMap, sk);

            if (cachedMapping != null) {
                return constructMapping.invoke(this.getManager(), this.getShardMap(), cachedMapping.Mapping);
            }
        }

        // Cache-miss, find mapping for given key in GSM.
        TMapping m = null;

        IStoreResults gsmResult;

        Stopwatch stopwatch = Stopwatch.StartNew();

        try (IStoreOperationGlobal op = this.getManager().StoreOperationFactory.CreateFindMappingByKeyGlobalOperation(this.getManager(), "Lookup", this.getShardMap().StoreShardMap, sk, CacheStoreMappingUpdatePolicy.OverwriteExisting, errorCategory, true, false)) {
            gsmResult = op.Do();
        }

        stopwatch.Stop();

        getTracer().TraceVerbose(TraceSourceConstants.ComponentNames.BaseShardMapper, "Lookup", "Lookup key from GSM complete; Key type : {0}; Result: {1}; Duration: {2}", TKey.class, gsmResult.Result, stopwatch.Elapsed);

        // If we could not locate the mapping, we return null and do nothing here.
        if (gsmResult.Result != StoreResult.MappingNotFoundForKey) {
            return gsmResult.StoreMappings.Select(sm -> constructMapping.invoke(this.getManager(), this.getShardMap(), sm)).Single();
        }

        return m;
    }

    /**
     * Finds mapping in store for OpenConnectionForKey operation.
     *
     * @param sk            Key to find.
     * @param policy        Cache update policy.
     * @param errorCategory Error category.
     * @return Mapping corresponding to the given key if found.
     */
    private IStoreMapping LookupMappingForOpenConnectionForKey(ShardKey sk, CacheStoreMappingUpdatePolicy policy, ShardManagementErrorCategory errorCategory) {
        IStoreResults gsmResult;

        Stopwatch stopwatch = Stopwatch.StartNew();

        try (IStoreOperationGlobal op = this.getManager().StoreOperationFactory.CreateFindMappingByKeyGlobalOperation(this.getManager(), "Lookup", this.getShardMap().StoreShardMap, sk, policy, errorCategory, true, false)) {
            gsmResult = op.Do();
        }

        stopwatch.Stop();

        getTracer().TraceVerbose(TraceSourceConstants.ComponentNames.BaseShardMapper, "LookupMappingForOpenConnectionForKey", "Lookup key from GSM complete; Key type : {0}; Result: {1}; Duration: {2}", sk.DataType, gsmResult.Result, stopwatch.Elapsed);

        // If we could not locate the mapping, we throw.
        if (gsmResult.Result == StoreResult.MappingNotFoundForKey) {
            throw new ShardManagementException(errorCategory, ShardManagementErrorCode.MappingNotFoundForKey, Errors._Store_ShardMapper_MappingNotFoundForKeyGlobal, this.getShardMap().Name, StoreOperationRequestBuilder.SpFindShardMappingByKeyGlobal, "LookupMappingForOpenConnectionForKey");
        } else {
            return gsmResult.StoreMappings.Single();
        }
    }


    /**
     * Asynchronously finds the mapping in store for OpenConnectionForKey operation.
     *
     * @param sk            Key to find.
     * @param policy        Cache update policy.
     * @param errorCategory Error category.
     * @return Task with the Mapping corresponding to the given key if found as the result.
     */
//TODO TASK: There is no equivalent in Java to the 'async' keyword:
//ORIGINAL LINE: private async Task<IStoreMapping> LookupMappingForOpenConnectionForKeyAsync(ShardKey sk, CacheStoreMappingUpdatePolicy policy, ShardManagementErrorCategory errorCategory)
    private Task<IStoreMapping> LookupMappingForOpenConnectionForKeyAsync(ShardKey sk, CacheStoreMappingUpdatePolicy policy, ShardManagementErrorCategory errorCategory) {
        IStoreResults gsmResult;

        Stopwatch stopwatch = Stopwatch.StartNew();

        try (IStoreOperationGlobal op = this.getManager().StoreOperationFactory.CreateFindMappingByKeyGlobalOperation(this.getManager(), "Lookup", this.getShardMap().StoreShardMap, sk, policy, errorCategory, true, false)) {
//TODO TASK: There is no equivalent to 'await' in Java:
            gsmResult = await op.DoAsync().ConfigureAwait(false);
        }

        stopwatch.Stop();

        getTracer().TraceVerbose(TraceSourceConstants.ComponentNames.BaseShardMapper, "LookupMappingForOpenConnectionForKeyAsync", "Lookup key from GSM complete; Key type : {0}; Result: {1}; Duration: {2}", sk.DataType, gsmResult.Result, stopwatch.Elapsed);

        // If we could not locate the mapping, we throw.
        if (gsmResult.Result == StoreResult.MappingNotFoundForKey) {
            throw new ShardManagementException(errorCategory, ShardManagementErrorCode.MappingNotFoundForKey, Errors._Store_ShardMapper_MappingNotFoundForKeyGlobal, this.getShardMap().Name, StoreOperationRequestBuilder.SpFindShardMappingByKeyGlobal, "LookupMappingForOpenConnectionForKeyAsync");
        } else {
            return gsmResult.StoreMappings.Single();
        }
    }

    /**
     * Gets all the mappings that exist within given range.
     *
     * @param range            Optional range value, if null, we cover everything.
     * @param shard            Optional shard parameter, if null, we cover all shards.
     * @param constructMapping Delegate to construct a mapping object.
     * @param errorCategory    Category under which errors will be posted.
     * @param mappingType      Name of mapping type.
     * @return Read-only collection of mappings that overlap with given range.
     */
    protected final <TMapping, TKey> IReadOnlyList<TMapping> GetMappingsForRange(Range<TKey> range, Shard shard, ActionGeneric3Param<ShardMapManager, ShardMap, IStoreMapping, TMapping> constructMapping, ShardManagementErrorCategory errorCategory, String mappingType) {
        ShardRange sr = null;

        if (shard != null) {
            ExceptionUtils.EnsureShardBelongsToShardMap(this.getManager(), this.getShardMap(), shard, "GetMappings", mappingType);
        }

        if (range != null) {
            sr = new ShardRange(new ShardKey(ShardKey.ShardKeyTypeFromType(TKey.class), range.Low), new ShardKey(ShardKey.ShardKeyTypeFromType(TKey.class), range.HighIsMax ? null : (Object) range.High));
        }

        IStoreResults result;

        try (IStoreOperationGlobal op = this.getManager().StoreOperationFactory.CreateGetMappingsByRangeGlobalOperation(this.getManager(), "GetMappingsForRange", this.getShardMap().StoreShardMap, shard != null ? shard.StoreShard : null, sr, errorCategory, true, false)) {
            result = op.Do();
        }

        return result.StoreMappings.Select(sm -> constructMapping.invoke(this.getManager(), this.getShardMap(), sm)).ToList().AsReadOnly();
    }

    /**
     * Allows for update to a mapping with the updates provided in
     * the <paramref name="update"/> parameter.
     *
     * @param currentMapping   Mapping being updated.
     * @param update           Updated properties of the Shard.
     * @param constructMapping Delegate to construct a mapping object.
     * @param statusAsInt      Delegate to get the mapping status as an integer value.
     * @param intAsStatus      Delegate to get the mapping status from an integer value.
     * @param lockOwnerId      Lock owner id of this mapping
     * @return New instance of mapping with updated information.
     */

    protected final <TMapping extends IShardProvider & IMappingInfoProvider, TUpdate extends IMappingUpdate<TStatus>, TStatus> TMapping Update(TMapping currentMapping, TUpdate update, Func<ShardMapManager, ShardMap, IStoreMapping, TMapping> constructMapping, Func<TStatus, Integer> statusAsInt, Func<Integer, TStatus> intAsStatus) {
        return Update(currentMapping, update, constructMapping, statusAsInt, intAsStatus, default (System.Guid));
    }

    //TODO TASK: The C# 'struct' constraint has no equivalent in Java:
//ORIGINAL LINE: protected TMapping Update<TMapping, TUpdate, TStatus>(TMapping currentMapping, TUpdate update, Func<ShardMapManager, ShardMap, IStoreMapping, TMapping> constructMapping, Func<TStatus, int> statusAsInt, Func<int, TStatus> intAsStatus, Guid lockOwnerId = default(Guid)) where TUpdate : class, IMappingUpdate<TStatus> where TMapping : class, IShardProvider, IMappingInfoProvider where TStatus : struct
    protected final <TMapping extends IShardProvider & IMappingInfoProvider, TUpdate extends IMappingUpdate<TStatus>, TStatus> TMapping Update(TMapping currentMapping, TUpdate update, ActionGeneric3Param<ShardMapManager, ShardMap, IStoreMapping, TMapping> constructMapping, ActionGeneric1Param<TStatus, Integer> statusAsInt, ActionGeneric1Param<Integer, TStatus> intAsStatus, UUID lockOwnerId) {
        assert currentMapping != null;
        assert update != null;

        this.<TMapping>EnsureMappingBelongsToShardMap(currentMapping, "Update", "currentMapping");

        IMappingUpdate<TStatus> mu = (IMappingUpdate<TStatus>) ((update instanceof IMappingUpdate<TStatus>) ? update : null);

        // CONSIDER(wbasheer): Have refresh semantics for trivial case when nothing is modified.
        if (!mu.IsAnyPropertySet(MappingUpdatedProperties.All)) {
            return currentMapping;
        }

        boolean shardChanged = mu.IsAnyPropertySet(MappingUpdatedProperties.Shard) && !mu.Shard.equals(currentMapping.ShardInfo);

        // Ensure that shard belongs to current shard map.
        if (shardChanged) {
            ExceptionUtils.EnsureShardBelongsToShardMap(this.getManager(), this.getShardMap(), mu.Shard, "UpdateMapping", currentMapping.Kind == MappingKind.PointMapping ? "PointMapping" : "RangeMapping");
        }

        IStoreShard originalShard = new DefaultStoreShard(currentMapping.ShardInfo.Id, UUID.randomUUID(), currentMapping.ShardInfo.StoreShard.ShardMapId, currentMapping.ShardInfo.StoreShard.Location, currentMapping.ShardInfo.StoreShard.Status);

        IStoreMapping originalMapping = new DefaultStoreMapping(currentMapping.StoreMapping.Id, currentMapping.ShardMapId, originalShard, currentMapping.StoreMapping.MinValue, currentMapping.StoreMapping.MaxValue, currentMapping.StoreMapping.Status, lockOwnerId);

        IStoreShard updatedShard;

        if (shardChanged) {
            updatedShard = new DefaultStoreShard(update.Shard.ShardInfo.Id, UUID.randomUUID(), update.Shard.ShardInfo.StoreShard.ShardMapId, update.Shard.ShardInfo.StoreShard.Location, update.Shard.ShardInfo.StoreShard.Status);
        } else {
            updatedShard = originalShard;
        }

        IStoreMapping updatedMapping = new DefaultStoreMapping(UUID.randomUUID(), currentMapping.ShardMapId, updatedShard, currentMapping.StoreMapping.MinValue, currentMapping.StoreMapping.MaxValue, mu.IsAnyPropertySet(MappingUpdatedProperties.Status) ? statusAsInt.invoke(update.Status) : currentMapping.StoreMapping.Status, lockOwnerId);

        boolean fromOnlineToOffline = mu.IsMappingBeingTakenOffline(intAsStatus.invoke(currentMapping.StoreMapping.Status));

        StoreOperationCode opCode;

        if (fromOnlineToOffline) {
            opCode = currentMapping.Kind == MappingKind.PointMapping ? StoreOperationCode.UpdatePointMappingWithOffline : StoreOperationCode.UpdateRangeMappingWithOffline;
        } else {
            opCode = currentMapping.Kind == MappingKind.PointMapping ? StoreOperationCode.UpdatePointMapping : StoreOperationCode.UpdateRangeMapping;
        }

        try (IStoreOperation op = this.getManager().StoreOperationFactory.CreateUpdateMappingOperation(this.getManager(), opCode, this.getShardMap().StoreShardMap, originalMapping, updatedMapping, this.getShardMap().ApplicationNameSuffix, lockOwnerId)) {
            op.Do();
        }

        return constructMapping.invoke(this.getManager(), this.getShardMap(), updatedMapping);
    }

    /**
     * Gets the lock owner of a mapping.
     *
     * @param mapping       The mapping
     * @param errorCategory Error category to use for the store operation
     * @return Lock owner for the mapping.
     */
    public final <TMapping extends IShardProvider & IMappingInfoProvider> UUID GetLockOwnerForMapping(TMapping mapping, ShardManagementErrorCategory errorCategory) {
        this.<TMapping>EnsureMappingBelongsToShardMap(mapping, "LookupLockOwner", "mapping");

        IStoreResults result;

        try (IStoreOperationGlobal op = this.getManager().StoreOperationFactory.CreateFindMappingByIdGlobalOperation(this.getManager(), "LookupLockOwner", this.getShardMap().StoreShardMap, mapping.StoreMapping, errorCategory)) {
            result = op.Do();
        }

        return result.StoreMappings.Single().LockOwnerId;
    }

    /**
     * Locks or unlocks a given mapping or all mappings.
     *
     * @param mapping           Optional mapping
     * @param lockOwnerId       The lock onwer id
     * @param lockOwnerIdOpType Operation to perform on this mapping with the given lockOwnerId
     * @param errorCategory     Error category to use for the store operation
     */
    public final <TMapping extends IShardProvider & IMappingInfoProvider> void LockOrUnlockMappings(TMapping mapping, UUID lockOwnerId, LockOwnerIdOpType lockOwnerIdOpType, ShardManagementErrorCategory errorCategory) {
        String operationName = lockOwnerIdOpType == LockOwnerIdOpType.Lock ? "Lock" : "UnLock";

        if (lockOwnerIdOpType != LockOwnerIdOpType.UnlockAllMappingsForId && lockOwnerIdOpType != LockOwnerIdOpType.UnlockAllMappings) {
            this.<TMapping>EnsureMappingBelongsToShardMap(mapping, operationName, "mapping");

            if (lockOwnerIdOpType == LockOwnerIdOpType.Lock && UUID.OpEquality(lockOwnerId, MappingLockToken.ForceUnlock.LockOwnerId)) {
                throw new IllegalArgumentException(StringUtilsLocal.FormatInvariant(Errors._ShardMapping_LockIdNotSupported, mapping.ShardInfo.Location, this.getShardMap().Name, lockOwnerId), "lockOwnerId");
            }
        } else {
            assert mapping == null;
        }

        try (IStoreOperationGlobal op = this.getManager().StoreOperationFactory.CreateLockOrUnLockMappingsGlobalOperation(this.getManager(), operationName, this.getShardMap().StoreShardMap, mapping != null ? mapping.StoreMapping : null, lockOwnerId, lockOwnerIdOpType, errorCategory)) {
            op.Do();
        }
    }

    /**
     * Validates the input parameters and ensures that the mapping parameter belong to this shard map.
     *
     * @param mapping       Mapping to be validated.
     * @param operationName Operation being performed.
     * @param parameterName Parameter name for mapping parameter.
     */
    protected final <TMapping extends IMappingInfoProvider> void EnsureMappingBelongsToShardMap(TMapping mapping, String operationName, String parameterName) {
        assert mapping.Manager != null;

        // Ensure that shard belongs to current shard map.
        if (mapping.ShardMapId != this.getShardMap().Id) {
            throw new IllegalStateException(StringUtilsLocal.FormatInvariant(Errors._ShardMapping_DifferentShardMap, mapping.TypeName, operationName, this.getShardMap().Name, parameterName));
        }

        // Ensure that the mapping objects belong to same shard map.
        if (mapping.Manager != this.getManager()) {
            throw new IllegalStateException(StringUtilsLocal.FormatInvariant(Errors._ShardMapping_DifferentShardMapManager, mapping.TypeName, operationName, this.getManager().Credentials.ShardMapManagerLocation, this.getShardMap().Name, parameterName));
        }
    }
}

package com.microsoft.azure.elasticdb.shard.mapmanager;

// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

import com.google.common.base.Stopwatch;
import com.microsoft.azure.elasticdb.core.commons.helpers.EventHandler;
import com.microsoft.azure.elasticdb.core.commons.helpers.ReferenceObjectHelper;
import com.microsoft.azure.elasticdb.core.commons.logging.ActivityIdScope;
import com.microsoft.azure.elasticdb.core.commons.transientfaulthandling.RetryBehavior;
import com.microsoft.azure.elasticdb.core.commons.transientfaulthandling.RetryPolicy;
import com.microsoft.azure.elasticdb.core.commons.transientfaulthandling.RetryingEventArgs;
import com.microsoft.azure.elasticdb.shard.base.ShardKey;
import com.microsoft.azure.elasticdb.shard.base.ShardLocation;
import com.microsoft.azure.elasticdb.shard.cache.ICacheStore;
import com.microsoft.azure.elasticdb.shard.map.*;
import com.microsoft.azure.elasticdb.shard.recovery.RecoveryManager;
import com.microsoft.azure.elasticdb.shard.schema.SchemaInfoCollection;
import com.microsoft.azure.elasticdb.shard.sqlstore.SqlShardMapManagerCredentials;
import com.microsoft.azure.elasticdb.shard.store.IStoreConnectionFactory;
import com.microsoft.azure.elasticdb.shard.store.StoreResults;
import com.microsoft.azure.elasticdb.shard.store.StoreShardMap;
import com.microsoft.azure.elasticdb.shard.storeops.base.IStoreOperationFactory;
import com.microsoft.azure.elasticdb.shard.storeops.base.IStoreOperationGlobal;
import com.microsoft.azure.elasticdb.shard.storeops.base.IStoreOperationLocal;
import com.microsoft.azure.elasticdb.shard.utils.Errors;
import com.microsoft.azure.elasticdb.shard.utils.ExceptionUtils;
import com.microsoft.azure.elasticdb.shard.utils.GlobalConstants;
import com.microsoft.azure.elasticdb.shard.store.Version;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

/**
 * Serves as the entry point for creation, management and lookup operations over shard maps.
 */
public final class ShardMapManager {
    private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
    /**
     * Credentials for performing ShardMapManager operations.
     */
    private SqlShardMapManagerCredentials credentials;
    /**
     * Factory for store connections.
     */
    private IStoreConnectionFactory storeConnectionFactory;

    /**
     * Event to be raised on Shard Map Manager store retries.
     */
    //TODO public Event<EventHandler<RetryingEventArgs>> ShardMapManagerRetrying = new Event<EventHandler<RetryingEventArgs>>();
    /**
     * Factory for store operations.
     */
    private IStoreOperationFactory storeOperationFactory;
    /**
     * Policy for performing retries on connections to shard map manager database.
     */
    private RetryPolicy retryPolicy;
    /**
     * Local cache.
     */
    private ICacheStore cache;

    /**
     * Given the connection string, opens up the corresponding data source and obtains the ShardMapManager.
     *
     * @param credentials            credentials for performing ShardMapManager operations.
     * @param storeConnectionFactory Factory for store connections.
     * @param storeOperationFactory  Factory for store operations.
     * @param cacheStore             Cache store.
     * @param loadPolicy             Initialization policy.
     * @param retryPolicy            Policy for performing retries on connections to shard map manager database.
     * @param retryBehavior          Policy for detecting transient errors.
     */
    public ShardMapManager(SqlShardMapManagerCredentials credentials, IStoreConnectionFactory storeConnectionFactory, IStoreOperationFactory storeOperationFactory, ICacheStore cacheStore, ShardMapManagerLoadPolicy loadPolicy, RetryPolicy retryPolicy, RetryBehavior retryBehavior) {
        this(credentials, storeConnectionFactory, storeOperationFactory, cacheStore, loadPolicy, retryPolicy, retryBehavior, null);
    }

    /**
     * Given the connection string, opens up the corresponding data source and obtains the ShardMapManager.
     *
     * @param credentials            credentials for performing ShardMapManager operations.
     * @param storeConnectionFactory Factory for store connections.
     * @param storeOperationFactory  Factory for store operations.
     * @param cacheStore             Cache store.
     * @param loadPolicy             Initialization policy.
     * @param retryPolicy            Policy for performing retries on connections to shard map manager database.
     * @param retryBehavior          Policy for detecting transient errors.
     * @param retryEventHandler      Event handler for store operation retry events.
     */
    public ShardMapManager(SqlShardMapManagerCredentials credentials, IStoreConnectionFactory storeConnectionFactory, IStoreOperationFactory storeOperationFactory, ICacheStore cacheStore, ShardMapManagerLoadPolicy loadPolicy, RetryPolicy retryPolicy, RetryBehavior retryBehavior, EventHandler<RetryingEventArgs> retryEventHandler) {
        assert credentials != null;

        this.setCredentials(credentials);
        this.setStoreConnectionFactory(storeConnectionFactory);
        this.setStoreOperationFactory(storeOperationFactory);
        this.setCache(cacheStore);

        this.setRetryPolicy(new RetryPolicy(new ShardManagementTransientErrorDetectionStrategy(retryBehavior), retryPolicy.GetRetryStrategy()));

        // Register for TfhImpl.RetryPolicy.Retrying event.
//TODO TASK: Java has no equivalent to C#-style event wireups:
        //this.RetryPolicy.Retrying += this.ShardMapManagerRetryingEventHandler;

        // Add user specified event handler.
        if (retryEventHandler != null) {
            //TODO this.ShardMapManagerRetrying.addListener("retryEventHandler", (Object sender, RetryingEventArgs e) -> retryEventHandler(sender, e));
        }

        if (loadPolicy == ShardMapManagerLoadPolicy.Eager) {
            // We eagerly load everything from ShardMapManager. In case of lazy
            // loading policy, we will add things to local caches based on cache
            // misses on lookups.
            this.LoadFromStore();
        }
    }

    /**
     * Ensures that the given shard map name is valid.
     *
     * @param shardMapName Input shard map name.
     */
    private static void ValidateShardMapName(String shardMapName) {
        ExceptionUtils.DisallowNullOrEmptyStringArgument(shardMapName, "shardMapName");

        // Disallow non-alpha-numeric characters.
        if (!StringUtils.isAlphanumeric(shardMapName)) {
            throw new IllegalArgumentException(String.format(Errors._ShardMapManager_UnsupportedShardMapName,
                    shardMapName));
        }

        // Ensure that length is within bounds.
        if (shardMapName.length() > GlobalConstants.MaximumShardMapNameLength) {
            throw new IllegalArgumentException(String.format(
                    Errors._ShardMapManager_UnsupportedShardMapNameLength,
                    shardMapName,
                    GlobalConstants.MaximumShardMapNameLength));
        }
    }

    public SqlShardMapManagerCredentials getCredentials() {
        return credentials;
    }

    private void setCredentials(SqlShardMapManagerCredentials value) {
        credentials = value;
    }

    public IStoreConnectionFactory getStoreConnectionFactory() {
        return storeConnectionFactory;
    }

    private void setStoreConnectionFactory(IStoreConnectionFactory value) {
        storeConnectionFactory = value;
    }

    public IStoreOperationFactory getStoreOperationFactory() {
        return storeOperationFactory;
    }

    private void setStoreOperationFactory(IStoreOperationFactory value) {
        storeOperationFactory = value;
    }

    public RetryPolicy getRetryPolicy() {
        return retryPolicy;
    }

    private void setRetryPolicy(RetryPolicy value) {
        retryPolicy = value;
    }

    public ICacheStore getCache() {
        return cache;
    }

    private void setCache(ICacheStore value) {
        cache = value;
    }

    /**
     * Creates a list based <see cref="ListShardMap{TKey}"/>.
     * <p>
     * <typeparam name="TKey">Type of keys.</typeparam>
     *
     * @param shardMapName Name of shard map.
     * @return List shard map with the specified name.
     */
    public <TKey> ListShardMap<TKey> CreateListShardMap(String shardMapName) {
        ShardMapManager.ValidateShardMapName(shardMapName);

        try (ActivityIdScope activityIdScope = new ActivityIdScope(UUID.randomUUID())) {
            //TODO: Replace this.getClass() with Tkey.class
            StoreShardMap dssm = new StoreShardMap(UUID.randomUUID(), shardMapName, ShardMapType.List, ShardKey.ShardKeyTypeFromType(this.getClass()));

            ListShardMap<TKey> listShardMap = new ListShardMap<TKey>(this, dssm);

            //getTracer().TraceInfo(TraceSourceConstants.ComponentNames.ShardMapManager, "CreateListShardMap", "Start; ShardMap: {0}", shardMapName);

            Stopwatch stopwatch = Stopwatch.createStarted();

            this.AddShardMapToStore("CreateListShardMap", dssm);

            stopwatch.stop();

            //getTracer().TraceInfo(TraceSourceConstants.ComponentNames.ShardMapManager, "CreateListShardMap", "Added ShardMap to Store; ShardMap: {0} Duration: {1}", shardMapName, stopwatch.Elapsed);

            //getTracer().TraceInfo(TraceSourceConstants.ComponentNames.ShardMapManager, "CreateListShardMap", "Complete; ShardMap: {0} Duration: {1}", shardMapName, stopwatch.Elapsed);

            return listShardMap;
        }
    }

    /**
     * Create a range based <see cref="RangeShardMap{TKey}"/>.
     * <p>
     * <typeparam name="TKey">Type of keys.</typeparam>
     *
     * @param shardMapName Name of shard map.
     * @return Range shard map with the specified name.
     */
    public <TKey> RangeShardMap<TKey> CreateRangeShardMap(String shardMapName) {
        ShardMapManager.ValidateShardMapName(shardMapName);

        try (ActivityIdScope activityIdScope = new ActivityIdScope(UUID.randomUUID())) {
            //TODO: Replace this.getClass() with Tkey.class
            StoreShardMap dssm = new StoreShardMap(UUID.randomUUID(), shardMapName, ShardMapType.Range, ShardKey.ShardKeyTypeFromType(this.getClass()));

            RangeShardMap<TKey> rangeShardMap = new RangeShardMap<TKey>(this, dssm);

            //getTracer().TraceInfo(TraceSourceConstants.ComponentNames.ShardMapManager, "CreateRangeShardMap", "Start; ShardMap: {0}", shardMapName);

            Stopwatch stopwatch = Stopwatch.createStarted();

            this.AddShardMapToStore("CreateRangeShardMap", dssm);

            stopwatch.stop();

            //getTracer().TraceInfo(TraceSourceConstants.ComponentNames.ShardMapManager, "CreateRangeShardMap", "Added ShardMap to Store; ShardMap: {0} Duration: {1}", shardMapName, stopwatch.Elapsed);

            //getTracer().TraceInfo(TraceSourceConstants.ComponentNames.ShardMapManager, "CreateRangeShardMap", "Complete; ShardMap: {0} Duration: {1}", shardMapName, stopwatch.Elapsed);

            return rangeShardMap;
        }
    }

    /**
     * Removes the specified shard map.
     *
     * @param shardMap Shardmap to be removed.
     */
    public void DeleteShardMap(ShardMap shardMap) {
        this.ValidateShardMap(shardMap);

        try (ActivityIdScope activityIdScope = new ActivityIdScope(UUID.randomUUID())) {
            //getTracer().TraceInfo(TraceSourceConstants.ComponentNames.ShardMapManager, "DeleteShardMap", "Start; ShardMap: {0}", shardMap.Name);

            Stopwatch stopwatch = Stopwatch.createStarted();

            this.RemoveShardMapFromStore(shardMap.getStoreShardMap());

            stopwatch.stop();

            //getTracer().TraceInfo(TraceSourceConstants.ComponentNames.ShardMapManager, "DeleteShardMap", "Complete; ShardMap: {0}; Duration: {1}", shardMap.Name, stopwatch.Elapsed);
        }
    }

    /**
     * Obtains all shard maps associated with the shard map manager.
     *
     * @return Collection of shard maps associated with the shard map manager.
     */
    public List<ShardMap> GetShardMaps() {
        try (ActivityIdScope activityIdScope = new ActivityIdScope(UUID.randomUUID())) {
            //getTracer().TraceInfo(TraceSourceConstants.ComponentNames.ShardMapManager, "GetShardMaps", "Start; ");

            Stopwatch stopwatch = Stopwatch.createStarted();

            List<ShardMap> result = this.GetShardMapsFromStore();

            stopwatch.stop();

            //getTracer().TraceInfo(TraceSourceConstants.ComponentNames.ShardMapManager, "GetShardMaps", "Complete; Duration: {0}", stopwatch.Elapsed);

            return result;
        }
    }

    /**
     * Obtains a <see cref="ShardMap"/> given the name.
     *
     * @param shardMapName Name of shard map.
     * @return Shardmap with the specificed name.
     */
    public ShardMap GetShardMap(String shardMapName) {
        ShardMapManager.ValidateShardMapName(shardMapName);

        try (ActivityIdScope activityIdScope = new ActivityIdScope(UUID.randomUUID())) {
            //getTracer().TraceVerbose(TraceSourceConstants.ComponentNames.ShardMapManager, "GetShardMap", "Start; ShardMap: {0}", shardMapName);

            ShardMap shardMap = this.<ShardMap>LookupAndConvertShardMapHelper("GetShardMap", shardMapName, true);

            assert shardMap != null;

            //getTracer().TraceInfo(TraceSourceConstants.ComponentNames.ShardMapManager, "GetShardMap", "Complete; ShardMap: {0}", shardMapName);

            return shardMap;
        }
    }

    /**
     * Tries to obtains a <see cref="ShardMap"/> given the name.
     *
     * @param shardMapName Name of shard map.
     * @param shardMap     Shard map with the specified name.
     * @return <c>true</c> if shard map with the specified name was found, <c>false</c> otherwise.
     */
    public boolean TryGetShardMap(String shardMapName, ReferenceObjectHelper<ShardMap> shardMap) {
        ShardMapManager.ValidateShardMapName(shardMapName);

        try (ActivityIdScope activityIdScope = new ActivityIdScope(UUID.randomUUID())) {
            //getTracer().TraceVerbose(TraceSourceConstants.ComponentNames.ShardMapManager, "TryGetShardMap", "Start; ShardMap: {0}", shardMapName);

            shardMap.argValue = this.<ShardMap>LookupAndConvertShardMapHelper("TryGetShardMap", shardMapName, false);

            //getTracer().TraceInfo(TraceSourceConstants.ComponentNames.ShardMapManager, "TryGetShardMap", "Complete; ShardMap: {0}", shardMapName);

            return shardMap.argValue != null;
        }
    }

    /**
     * Obtains a <see cref="ListShardMap{TKey}"/> given the name.
     * <p>
     * <typeparam name="TKey">Key type.</typeparam>
     *
     * @param shardMapName Name of shard map.
     * @return Resulting ShardMap.
     */
    public <TKey> ListShardMap<TKey> GetListShardMap(String shardMapName) {
        ShardMapManager.ValidateShardMapName(shardMapName);

        try (ActivityIdScope activityIdScope = new ActivityIdScope(UUID.randomUUID())) {
            //getTracer().TraceVerbose(TraceSourceConstants.ComponentNames.ShardMapManager, "GetListShardMap", "Start; ShardMap: {0}", shardMapName);

            ListShardMap<TKey> shardMap = null; //TODO: this.<ListShardMap<TKey>>LookupAndConvertShardMapHelper("GetListShardMap", shardMapName, ShardMapExtensions.AsListShardMap < TKey >, true);

            assert shardMap != null;

            //getTracer().TraceInfo(TraceSourceConstants.ComponentNames.ShardMapManager, "GetListShardMap", "Complete; ShardMap: {0}", shardMapName);

            return shardMap;
        }
    }

    /**
     * Tries to obtains a <see cref="ListShardMap{TKey}"/> given the name.
     * <p>
     * <typeparam name="TKey">Key type.</typeparam>
     *
     * @param shardMapName Name of shard map.
     * @param shardMap     Shard map with the specified name.
     * @return <c>true</c> if shard map with the specified name was found, <c>false</c> otherwise.
     */
    public <TKey> boolean TryGetListShardMap(String shardMapName, ReferenceObjectHelper<ListShardMap<TKey>> shardMap) {
        ShardMapManager.ValidateShardMapName(shardMapName);

        try (ActivityIdScope activityIdScope = new ActivityIdScope(UUID.randomUUID())) {
            //getTracer().TraceVerbose(TraceSourceConstants.ComponentNames.ShardMapManager, "TryGetListShardMap", "Start; ShardMap: {0}", shardMapName);

            shardMap.argValue = null; //TODO: this.<ListShardMap<TKey>>LookupAndConvertShardMapHelper("TryGetListShardMap", shardMapName, ShardMapExtensions.AsListShardMap < TKey >, false);

            //getTracer().TraceInfo(TraceSourceConstants.ComponentNames.ShardMapManager, "TryGetListShardMap", "Complete; ShardMap: {0}", shardMapName);

            return shardMap.argValue != null;
        }
    }

    /**
     * Obtains a <see cref="RangeShardMap{TKey}"/> given the name.
     * <p>
     * <typeparam name="TKey">Key type.</typeparam>
     *
     * @param shardMapName Name of shard map.
     * @return Resulting ShardMap.
     */
    public <TKey> RangeShardMap<TKey> GetRangeShardMap(String shardMapName) {
        ShardMapManager.ValidateShardMapName(shardMapName);

        try (ActivityIdScope activityIdScope = new ActivityIdScope(UUID.randomUUID())) {
            //getTracer().TraceVerbose(TraceSourceConstants.ComponentNames.ShardMapManager, "GetRangeShardMap", "Start; ShardMap: {0}", shardMapName);

            RangeShardMap<TKey> shardMap = null; //TODO: this.<RangeShardMap<TKey>>LookupAndConvertShardMapHelper("GetRangeShardMap", shardMapName, ShardMapExtensions.AsRangeShardMap < TKey >, true);

            assert shardMap != null;

            //getTracer().TraceInfo(TraceSourceConstants.ComponentNames.ShardMapManager, "GetRangeShardMap", "Complete; ShardMap: {0}", shardMapName);

            return shardMap;
        }
    }

    /**
     * Tries to obtains a <see cref="RangeShardMap{TKey}"/> given the name.
     *
     * @param shardMapName Name of shard map.
     * @return RangeShardMap
     */
    public <TKey> RangeShardMap<TKey> TryGetRangeShardMap(String shardMapName) {
        ValidateShardMapName(shardMapName);

        try (ActivityIdScope activityIdScope = new ActivityIdScope(UUID.randomUUID())) {
            log.debug("Start; ShardMap: {}", shardMapName);

            ShardMap shardMap = this.LookupAndConvertShardMapHelper(
                    "TryGetRangeShardMap",
                    shardMapName, false);
            log.info("Complete; ShardMap: {}", shardMapName);
            return ShardMapExtensions.AsRangeShardMap(shardMap, false);
        }
    }

    /**
     * Obtains distinct shard locations from the shard map manager.
     *
     * @return Collection of shard locations associated with the shard map manager.
     */
    public List<ShardLocation> GetDistinctShardLocations() {
        try (ActivityIdScope activityIdScope = new ActivityIdScope(UUID.randomUUID())) {
            //getTracer().TraceInfo(TraceSourceConstants.ComponentNames.ShardMapManager, "GetDistinctShardLocations", "Start; ");

            Stopwatch stopwatch = Stopwatch.createStarted();

            List<ShardLocation> result = this.GetDistinctShardLocationsFromStore();

            stopwatch.stop();

            //getTracer().TraceInfo(TraceSourceConstants.ComponentNames.ShardMapManager, "GetDistinctShardLocations", "Complete; Duration: {0}", stopwatch.Elapsed);

            return result;
        }
    }

    /**
     * Upgrades store location to the latest version supported by library.
     *
     * @param location Shard location to upgrade.
     */
    public void UpgradeLocalStore(ShardLocation location) {
        try (ActivityIdScope activityIdScope = new ActivityIdScope(UUID.randomUUID())) {
            //getTracer().TraceInfo(TraceSourceConstants.ComponentNames.ShardMapManager, "UpgradeGlobalShardMapManager", "Start; ");

            Stopwatch stopwatch = Stopwatch.createStarted();

            this.UpgradeStoreLocal(location, GlobalConstants.LsmVersionClient);

            stopwatch.stop();

            //getTracer().TraceInfo(TraceSourceConstants.ComponentNames.ShardMapManager, "UpgradeGlobalShardMapManager", "Complete; Duration: {0}", stopwatch.Elapsed);
        }
    }

    /**
     * Obtains the recovery manager for the current shard map manager instance.
     *
     * @return Recovery manager for the shard map manager.
     */
    public RecoveryManager GetRecoveryManager() {
        return new RecoveryManager(this);
    }

    ///#region Internal Lookup functions

    /**
     * Obtains the schema info collection object for the current shard map manager instance.
     *
     * @return schema info collection for shard map manager.
     */
    public SchemaInfoCollection GetSchemaInfoCollection() {
        return new SchemaInfoCollection(this);
    }
    ///#endregion Internal Lookup functions

    /**
     * Finds a shard map from cache if requested and if necessary from global shard map.
     *
     * @param operationName    Operation name, useful for diagnostics.
     * @param shardMapName     Name of shard map.
     * @param lookInCacheFirst Whether to skip first lookup in cache.
     * @return Shard map object corresponding to one being searched.
     */
    private ShardMap LookupShardMapByName(String operationName, String shardMapName, boolean lookInCacheFirst) {
        StoreShardMap ssm = null;

        if (lookInCacheFirst) {
            // Typical scenario will result in immediate lookup succeeding.
            ssm = this.getCache().LookupShardMapByName(shardMapName);
        }

        ShardMap shardMap;

        // Cache miss. Go to store and add entry to cache.
        if (ssm == null) {
            Stopwatch stopwatch = Stopwatch.createStarted();

            shardMap = this.LookupShardMapByNameInStore(operationName, shardMapName);

            stopwatch.stop();

            log.info("Lookup ShardMap: {} in store complete; Duration: {}",
                    shardMapName, stopwatch.elapsed(TimeUnit.MILLISECONDS));
        } else {
            shardMap = ShardMapUtils.CreateShardMapFromStoreShardMap(this, ssm);
        }

        return shardMap;
    }

    /**
     * Subscriber function to RetryPolicy.Retrying event.
     *
     * @param sender Sender object (RetryPolicy)
     * @param arg    Event argument.
     */
    public void ShardMapManagerRetryingEventHandler(Object sender, RetryingEventArgs arg) {
        // Trace out retry event.
        //getTracer().TraceInfo(TraceSourceConstants.ComponentNames.ShardMapManager, "ShardMapManagerRetryingEvent", "Retry Count: {0}; Delay: {1}", arg.CurrentRetryCount, arg.Delay);

        this.OnShardMapManagerRetryingEvent(new RetryingEventArgs(arg));
    }

    /**
     * Publisher for ShardMapManagerRetryingEvent event.
     *
     * @param arg Event argument.
     */
    public void OnShardMapManagerRetryingEvent(RetryingEventArgs arg) {
        /*EventHandler<RetryingEventArgs> handler = (Object sender, RetryingEventArgs e) -> ShardMapManagerRetrying.invoke(sender, e);
        if (handler != null) {
            handler.invoke(this, arg);
        }*/
        //TODO
    }

    /**
     * Upgrades store hosting global shard map to specified version. This will be used for upgrade testing.
     *
     * @param targetVersion Target store version to deploy.
     */
    public void UpgradeGlobalStore(Version targetVersion) {
        try (ActivityIdScope activityIdScope = new ActivityIdScope(UUID.randomUUID())) {
            //getTracer().TraceInfo(TraceSourceConstants.ComponentNames.ShardMapManager, "UpgradeGlobalShardMapManager", "Start; ");

            Stopwatch stopwatch = Stopwatch.createStarted();

            this.UpgradeStoreGlobal(targetVersion);

            stopwatch.stop();

            //getTracer().TraceInfo(TraceSourceConstants.ComponentNames.ShardMapManager, "UpgradeGlobalShardMapManager", "Complete; Duration: {0}", stopwatch.Elapsed);
        }
    }

    /**
     * Upgrades store location to the specified version. This will be used for upgrade testing.
     *
     * @param location      Shard location to upgrade.
     * @param targetVersion Target store version to deploy.
     */
    public void UpgradeLocalStore(ShardLocation location, Version targetVersion) {
        try (ActivityIdScope activityIdScope = new ActivityIdScope(UUID.randomUUID())) {
            //getTracer().TraceInfo(TraceSourceConstants.ComponentNames.ShardMapManager, "UpgradeGlobalShardMapManager", "Start; ");

            Stopwatch stopwatch = Stopwatch.createStarted();

            this.UpgradeStoreLocal(location, targetVersion);

            stopwatch.stop();

            //getTracer().TraceInfo(TraceSourceConstants.ComponentNames.ShardMapManager, "UpgradeGlobalShardMapManager", "Complete; Duration: {0}", stopwatch.Elapsed);
        }
    }

    /**
     * Performs lookup and conversion operation for shard map with given name.
     * <p>
     * <typeparam name="TShardMap">Type to convert shard map to.</typeparam>
     *
     * @param operationName  Operation name, useful for diagnostics.
     * @param shardMapName   Shard map name.
     * @param throwOnFailure Whether to throw exception or return null on failure.
     * @return The converted shard map.
     */
    private ShardMap LookupAndConvertShardMapHelper(String operationName
            , String shardMapName
            , boolean throwOnFailure) {
        ShardMap sm = this.LookupShardMapByName(operationName, shardMapName, true);

        if (sm == null && throwOnFailure) {
            throw new ShardManagementException(
                    ShardManagementErrorCategory.ShardMapManager,
                    ShardManagementErrorCode.ShardMapLookupFailure,
                    Errors._ShardMapManager_ShardMapLookupFailed,
                    shardMapName,
                    this.credentials.getShardMapManagerLocation());
        }
        return sm;
    }

    /**
     * Loads the shard map manager and shards from Store.
     */
    private void LoadFromStore() {
        this.getCache().Clear();

        try (IStoreOperationGlobal op = this.getStoreOperationFactory().CreateLoadShardMapManagerGlobalOperation(this, "GetShardMapManager")) {
            op.Do();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * Adds a shard to global shard map.
     *
     * @param operationName Operation name, useful for diagnostics.
     * @param ssm           Storage representation of shard map object.
     */
    private void AddShardMapToStore(String operationName, StoreShardMap ssm) {
        try (IStoreOperationGlobal op = this.getStoreOperationFactory().CreateAddShardMapGlobalOperation(this, operationName, ssm)) {
            op.Do();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * Removes a shard map from global shard map.
     *
     * @param ssm Shard map to remove.
     */
    private void RemoveShardMapFromStore(StoreShardMap ssm) {
        try (IStoreOperationGlobal op = this.getStoreOperationFactory().CreateRemoveShardMapGlobalOperation(this, "DeleteShardMap", ssm)) {
            op.Do();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * Obtains all ShardMaps associated with the shard map manager.
     *
     * @return Collection of shard maps associated with the shard map manager.
     */
    private List<ShardMap> GetShardMapsFromStore() {
        StoreResults result = null;

        try (IStoreOperationGlobal op = this.getStoreOperationFactory().CreateGetShardMapsGlobalOperation(this, "GetShardMaps")) {
            result = op.Do();
        } catch (Exception e) {
            e.printStackTrace();
        }

        return null; //TODO: result.getStoreShardMaps().Select(ssm -> ShardMapUtils.CreateShardMapFromStoreShardMap(this, ssm));
    }

    /**
     * Get distinct locations for the shard map manager from store.
     *
     * @return Distinct locations from shard map manager.
     */
    private List<ShardLocation> GetDistinctShardLocationsFromStore() {
        StoreResults result = null;

        try (IStoreOperationGlobal op = this.getStoreOperationFactory().CreateGetDistinctShardLocationsGlobalOperation(this, "GetDistinctShardLocations")) {
            result = op.Do();
        } catch (Exception e) {
            e.printStackTrace();
        }

        return null; //TODO: result.getStoreLocations().Select(sl -> sl.Location);
    }

    /**
     * Upgrades store hosting GSM.
     *
     * @param targetVersion Target version for store to upgrade to.
     */
    private void UpgradeStoreGlobal(Version targetVersion) {
        try (IStoreOperationGlobal op = this.getStoreOperationFactory().CreateUpgradeStoreGlobalOperation(this, "UpgradeStoreGlobal", targetVersion)) {
            op.Do();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * Upgrades store at specified location.
     *
     * @param location      Store location to upgrade.
     * @param targetVersion Target version for store to upgrade to.
     * @return
     */
    private void UpgradeStoreLocal(ShardLocation location, Version targetVersion) {
        try (IStoreOperationLocal op = this.getStoreOperationFactory().CreateUpgradeStoreLocalOperation(this, location, "UpgradeStoreLocal", targetVersion)) {
            op.Do();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * Finds shard map with given name in global shard map.
     *
     * @param operationName Operation name, useful for diagnostics.
     * @param shardMapName  Name of shard map to search.
     * @return Shard map corresponding to given Id.
     */
    private ShardMap LookupShardMapByNameInStore(String operationName, String shardMapName) {
        StoreResults result;

        try (IStoreOperationGlobal op = this.getStoreOperationFactory().CreateFindShardMapByNameGlobalOperation(this, operationName, shardMapName)) {
            result = op.Do();
            return result.getStoreShardMaps()
                    .stream().map(ssm -> ShardMapUtils.CreateShardMapFromStoreShardMap(this, ssm))
                    .findFirst().get();
        } catch (Exception e) {
            log.error("", e);
        }
        return null;
    }

    /**
     * Validates the input shard map. This includes:
     * Ensuring that shard map belongs to this instance of shard map manager.
     *
     * @param shardMap Input shard map.
     */
    private void ValidateShardMap(ShardMap shardMap) {
        ExceptionUtils.DisallowNullArgument(shardMap, "shardMap");

        if (shardMap.getShardMapManager() != this) {
            throw new IllegalStateException(String.format(Errors._ShardMapManager_DifferentShardMapManager, shardMap.getName(), this.getCredentials().getShardMapManagerLocation()));
        }
    }
}

package com.microsoft.azure.elasticdb.shard.mapmanager;

/* Copyright (c) Microsoft. All rights reserved.
Licensed under the MIT license. See LICENSE file in the project root for full license information.*/

import com.google.common.base.Stopwatch;
import com.microsoft.azure.elasticdb.core.commons.helpers.EventHandler;
import com.microsoft.azure.elasticdb.core.commons.helpers.ReferenceObjectHelper;
import com.microsoft.azure.elasticdb.core.commons.logging.ActivityIdScope;
import com.microsoft.azure.elasticdb.core.commons.transientfaulthandling.RetryBehavior;
import com.microsoft.azure.elasticdb.core.commons.transientfaulthandling.RetryPolicy;
import com.microsoft.azure.elasticdb.core.commons.transientfaulthandling.RetryingEventArgs;
import com.microsoft.azure.elasticdb.shard.base.ShardKeyType;
import com.microsoft.azure.elasticdb.shard.base.ShardLocation;
import com.microsoft.azure.elasticdb.shard.cache.ICacheStore;
import com.microsoft.azure.elasticdb.shard.map.ListShardMap;
import com.microsoft.azure.elasticdb.shard.map.RangeShardMap;
import com.microsoft.azure.elasticdb.shard.map.ShardMap;
import com.microsoft.azure.elasticdb.shard.map.ShardMapExtensions;
import com.microsoft.azure.elasticdb.shard.map.ShardMapType;
import com.microsoft.azure.elasticdb.shard.map.ShardMapUtils;
import com.microsoft.azure.elasticdb.shard.recovery.RecoveryManager;
import com.microsoft.azure.elasticdb.shard.schema.SchemaInfoCollection;
import com.microsoft.azure.elasticdb.shard.sqlstore.SqlShardMapManagerCredentials;
import com.microsoft.azure.elasticdb.shard.store.IStoreConnectionFactory;
import com.microsoft.azure.elasticdb.shard.store.StoreResults;
import com.microsoft.azure.elasticdb.shard.store.StoreShardMap;
import com.microsoft.azure.elasticdb.shard.store.Version;
import com.microsoft.azure.elasticdb.shard.storeops.base.IStoreOperationFactory;
import com.microsoft.azure.elasticdb.shard.storeops.base.IStoreOperationGlobal;
import com.microsoft.azure.elasticdb.shard.storeops.base.IStoreOperationLocal;
import com.microsoft.azure.elasticdb.shard.utils.Errors;
import com.microsoft.azure.elasticdb.shard.utils.ExceptionUtils;
import com.microsoft.azure.elasticdb.shard.utils.GlobalConstants;
import com.microsoft.azure.elasticdb.shard.utils.StringUtilsLocal;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
  //TODO: public Event<EventHandler<RetryingEventArgs>> ShardMapManagerRetrying = new Event();

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
   * Given the connection string, opens up the corresponding data source and obtains the
   * ShardMapManager.
   *
   * @param credentials credentials for performing ShardMapManager operations.
   * @param storeConnectionFactory Factory for store connections.
   * @param storeOperationFactory Factory for store operations.
   * @param cacheStore Cache store.
   * @param loadPolicy Initialization policy.
   * @param retryPolicy Policy for performing retries on connections to shard map manager database.
   * @param retryBehavior Policy for detecting transient errors.
   */
  public ShardMapManager(SqlShardMapManagerCredentials credentials,
      IStoreConnectionFactory storeConnectionFactory, IStoreOperationFactory storeOperationFactory,
      ICacheStore cacheStore, ShardMapManagerLoadPolicy loadPolicy, RetryPolicy retryPolicy,
      RetryBehavior retryBehavior) {
    this(credentials, storeConnectionFactory, storeOperationFactory, cacheStore, loadPolicy,
        retryPolicy, retryBehavior, null);
  }

  /**
   * Given the connection string, opens up the corresponding data source and obtains the
   * ShardMapManager.
   *
   * @param credentials credentials for performing ShardMapManager operations.
   * @param storeConnectionFactory Factory for store connections.
   * @param storeOperationFactory Factory for store operations.
   * @param cacheStore Cache store.
   * @param loadPolicy Initialization policy.
   * @param retryPolicy Policy for performing retries on connections to shard map manager database.
   * @param retryBehavior Policy for detecting transient errors.
   * @param retryEventHandler Event handler for store operation retry events.
   */
  public ShardMapManager(SqlShardMapManagerCredentials credentials,
      IStoreConnectionFactory storeConnectionFactory, IStoreOperationFactory storeOperationFactory,
      ICacheStore cacheStore, ShardMapManagerLoadPolicy loadPolicy, RetryPolicy retryPolicy,
      RetryBehavior retryBehavior, EventHandler<RetryingEventArgs> retryEventHandler) {
    assert credentials != null;

    this.setCredentials(credentials);
    this.setStoreConnectionFactory(storeConnectionFactory);
    this.setStoreOperationFactory(storeOperationFactory);
    this.setCache(cacheStore);

    this.setRetryPolicy(
        new RetryPolicy(new ShardManagementTransientErrorDetectionStrategy(retryBehavior),
            retryPolicy.getExponentialRetryStrategy()));

    // Register for TfhImpl.RetryPolicy.retrying event.
    // TODO: this.RetryPolicy.retrying += this.ShardMapManagerRetryingEventHandler;

    // Add user specified event handler.
    if (retryEventHandler != null) {
      //TODO
      // this.ShardMapManagerRetrying.addListener("retryEventHandler",
      // (Object sender, RetryingEventArgs e) -> retryEventHandler(sender, e));
    }

    if (loadPolicy == ShardMapManagerLoadPolicy.Eager) {
      // We eagerly load everything from ShardMapManager. In case of lazy
      // loading policy, we will add things to local caches based on cache
      // misses on lookups.
      this.loadFromStore();
    }
  }

  /**
   * Ensures that the given shard map name is valid.
   *
   * @param shardMapName Input shard map name.
   */
  private static void validateShardMapName(String shardMapName) {
    ExceptionUtils.disallowNullOrEmptyStringArgument(shardMapName, "shardMapName");

    // Disallow non-alpha-numeric characters.
    if (!StringUtilsLocal.isAlphanumericPunctuated(shardMapName)) {
      throw new IllegalArgumentException(String.format(
          Errors._ShardMapManager_UnsupportedShardMapName, shardMapName));
    }

    // Ensure that length is within bounds.
    if (shardMapName.length() > GlobalConstants.MaximumShardMapNameLength) {
      throw new IllegalArgumentException(String.format(
          Errors._ShardMapManager_UnsupportedShardMapNameLength, shardMapName,
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
   * Creates a list based <see cref="ListShardMap{KeyT}"/>.
   * <typeparam name="KeyT">Type of keys.</typeparam>
   *
   * @param shardMapName Name of shard map.
   * @return List shard map with the specified name.
   */
  public <KeyT> ListShardMap<KeyT> createListShardMap(String shardMapName, ShardKeyType keyType) {
    ShardMapManager.validateShardMapName(shardMapName);

    try (ActivityIdScope activityIdScope = new ActivityIdScope(UUID.randomUUID())) {
      StoreShardMap dssm = new StoreShardMap(UUID.randomUUID(), shardMapName, ShardMapType.List,
          keyType);

      log.info("ShardMapManager CreateListShardMap Start; ShardMap: {}", shardMapName);
      Stopwatch stopwatch = Stopwatch.createStarted();

      this.addShardMapToStore("CreateListShardMap", dssm);

      stopwatch.stop();
      log.info(
          "ShardMapManager CreateListShardMap Added ShardMap to Store; ShardMap: {} Duration: {}",
          shardMapName, stopwatch.elapsed(TimeUnit.MILLISECONDS));
      log.info("ShardMapManager CreateListShardMap Complete; ShardMap: {} Duration: {}",
          shardMapName, stopwatch.elapsed(TimeUnit.MILLISECONDS));

      return new ListShardMap<>(this, dssm);
    }
  }

  /**
   * Create a range based <see cref="RangeShardMap{KeyT}"/>.
   * <typeparam name="KeyT">Type of keys.</typeparam>
   *
   * @param shardMapName Name of shard map.
   * @return Range shard map with the specified name.
   */
  public <KeyT> RangeShardMap<KeyT> createRangeShardMap(String shardMapName, ShardKeyType keyType) {
    ShardMapManager.validateShardMapName(shardMapName);

    try (ActivityIdScope activityIdScope = new ActivityIdScope(UUID.randomUUID())) {
      StoreShardMap dssm = new StoreShardMap(UUID.randomUUID(), shardMapName, ShardMapType.Range,
          keyType);

      log.info("ShardMapManager CreateRangeShardMap Start; ShardMap: {}", shardMapName);
      Stopwatch stopwatch = Stopwatch.createStarted();

      this.addShardMapToStore("CreateRangeShardMap", dssm);

      stopwatch.stop();
      log.info(
          "ShardMapManager CreateRangeShardMap Added ShardMap to Store; ShardMap: {}; Duration: {}",
          shardMapName, stopwatch.elapsed(TimeUnit.MILLISECONDS));
      log.info("ShardMapManager CreateRangeShardMap Complete; ShardMap: {} Duration: {}",
          shardMapName, stopwatch.elapsed(TimeUnit.MILLISECONDS));

      return new RangeShardMap<>(this, dssm);
    }
  }

  /**
   * Removes the specified shard map.
   *
   * @param shardMap ShardMap to be removed.
   */
  public void deleteShardMap(ShardMap shardMap) {
    this.validateShardMap(shardMap);

    try (ActivityIdScope activityIdScope = new ActivityIdScope(UUID.randomUUID())) {
      log.info("ShardMapManager DeleteShardMap Start; ShardMap: {}", shardMap.getName());
      Stopwatch stopwatch = Stopwatch.createStarted();

      this.removeShardMapFromStore(shardMap.getStoreShardMap());

      stopwatch.stop();
      log.info("ShardMapManager DeleteShardMap Complete; ShardMap: {}; Duration: {}",
          shardMap.getName(), stopwatch.elapsed(TimeUnit.MILLISECONDS));
    }
  }

  /**
   * Obtains all shard maps associated with the shard map manager.
   *
   * @return Collection of shard maps associated with the shard map manager.
   */
  public List<ShardMap> getShardMaps() {
    try (ActivityIdScope activityIdScope = new ActivityIdScope(UUID.randomUUID())) {
      log.info("ShardMapManager GetShardMaps Start; ");

      Stopwatch stopwatch = Stopwatch.createStarted();

      List<ShardMap> result = this.getShardMapsFromStore();

      stopwatch.stop();

      log.info("ShardMapManager GetShardMaps Complete; Duration: {}",
          stopwatch.elapsed(TimeUnit.MILLISECONDS));

      return result;
    }
  }

  /**
   * Obtains a <see cref="ShardMap"/> given the name.
   *
   * @param shardMapName Name of shard map.
   * @return ShardMap with the specificed name.
   */
  public ShardMap getShardMap(String shardMapName) {
    ShardMapManager.validateShardMapName(shardMapName);

    try (ActivityIdScope activityIdScope = new ActivityIdScope(UUID.randomUUID())) {
      log.info("ShardMapManager GetShardMap Start; ShardMap: {}", shardMapName);

      ShardMap shardMap = this.<ShardMap>lookupAndConvertShardMapHelper("GetShardMap", shardMapName,
          true);

      assert shardMap != null;

      log.info("ShardMapManager GetShardMap Complete; ShardMap: {}", shardMapName);

      return shardMap;
    }
  }

  /**
   * Tries to obtains a <see cref="ShardMap"/> given the name.
   *
   * @param shardMapName Name of shard map.
   * @param shardMap Shard map with the specified name.
   * @return <c>true</c> if shard map with the specified name was found, <c>false</c> otherwise.
   */
  public boolean tryGetShardMap(String shardMapName, ReferenceObjectHelper<ShardMap> shardMap) {
    ShardMapManager.validateShardMapName(shardMapName);

    try (ActivityIdScope activityIdScope = new ActivityIdScope(UUID.randomUUID())) {
      log.info("ShardMapManager TryGetShardMap Start; ShardMap: {}", shardMapName);

      shardMap.argValue = this.<ShardMap>lookupAndConvertShardMapHelper("TryGetShardMap",
          shardMapName, false);

      log.info("ShardMapManager TryGetShardMap Complete; ShardMap: {}", shardMapName);

      return shardMap.argValue != null;
    }
  }

  /**
   * Obtains a <see cref="ListShardMap{KeyT}"/> given the name.
   * <typeparam name="KeyT">Key type.</typeparam>
   *
   * @param shardMapName Name of shard map.
   * @return Resulting ShardMap.
   */
  public <KeyT> ListShardMap<KeyT> getListShardMap(String shardMapName) {
    ShardMapManager.validateShardMapName(shardMapName);

    try (ActivityIdScope activityIdScope = new ActivityIdScope(UUID.randomUUID())) {
      log.info("ShardMapManager GetListShardMap Start; ShardMap: {}", shardMapName);

      ListShardMap<KeyT> shardMap = ShardMapExtensions.asListShardMap(
          this.<ListShardMap<KeyT>>lookupAndConvertShardMapHelper("GetListShardMap", shardMapName,
              true));

      assert shardMap != null;

      log.info("ShardMapManager GetListShardMap Complete; ShardMap: {}", shardMapName);

      return shardMap;
    }
  }

  /**
   * Tries to obtains a <see cref="ListShardMap{KeyT}"/> given the name.
   * <typeparam name="KeyT">Key type.</typeparam>
   *
   * @param shardMapName Name of shard map.
   * @return ListShardMap
   */
  public <KeyT> boolean tryGetListShardMap(String shardMapName,
      ReferenceObjectHelper<ListShardMap<KeyT>> shardMap) {
    ShardMapManager.validateShardMapName(shardMapName);

    try (ActivityIdScope activityIdScope = new ActivityIdScope(UUID.randomUUID())) {
      log.info("ShardMapManager TryGetListShardMap Start; ShardMap: {}", shardMapName);

      shardMap.argValue
          = (ListShardMap<KeyT>) this.<ListShardMap<KeyT>>lookupAndConvertShardMapHelper(
          "TryGetListShardMap",
          shardMapName, false);

      log.info("Complete; ShardMap: {}", shardMapName);

      return shardMap.argValue != null;
    }
  }

  /**
   * Obtains a <see cref="RangeShardMap{KeyT}"/> given the name.
   * <typeparam name="KeyT">Key type.</typeparam>
   *
   * @param shardMapName Name of shard map.
   * @return Resulting ShardMap.
   */
  public <KeyT> RangeShardMap<KeyT> getRangeShardMap(String shardMapName) {
    ShardMapManager.validateShardMapName(shardMapName);

    try (ActivityIdScope activityIdScope = new ActivityIdScope(UUID.randomUUID())) {
      log.info("ShardMapManager GetRangeShardMap Start; ShardMap: {}", shardMapName);

      RangeShardMap<KeyT> shardMap = ShardMapExtensions.asRangeShardMap(
          this.lookupAndConvertShardMapHelper("GetRangeShardMap", shardMapName, true));

      assert shardMap != null;

      log.info("ShardMapManager GetRangeShardMap Complete; ShardMap: {}", shardMapName);

      return shardMap;
    }
  }

  /**
   * Tries to obtains a <see cref="RangeShardMap{KeyT}"/> given the name.
   *
   * @param shardMapName Name of shard map.
   * @return RangeShardMap
   */
  public <KeyT> boolean tryGetRangeShardMap(String shardMapName,
      ReferenceObjectHelper<RangeShardMap<KeyT>> shardMap) {
    validateShardMapName(shardMapName);

    try (ActivityIdScope activityIdScope = new ActivityIdScope(UUID.randomUUID())) {
      log.info("ShardMapManager TryGetRangeShardMap Start; ShardMap: {}", shardMapName);

      shardMap.argValue
          = (RangeShardMap<KeyT>) this.<RangeShardMap<KeyT>>lookupAndConvertShardMapHelper(
          "TryGetRangeShardMap",
          shardMapName, false);

      log.info("Complete; ShardMap: {}", shardMapName);
      return shardMap.argValue != null;
    }
  }

  /**
   * Obtains distinct shard locations from the shard map manager.
   *
   * @return Collection of shard locations associated with the shard map manager.
   */
  public List<ShardLocation> getDistinctShardLocations() {
    try (ActivityIdScope activityIdScope = new ActivityIdScope(UUID.randomUUID())) {
      log.info("ShardMapManager GetDistinctShardLocations Start; ");

      Stopwatch stopwatch = Stopwatch.createStarted();

      List<ShardLocation> result = this.getDistinctShardLocationsFromStore();

      stopwatch.stop();

      log.info("ShardMapManager GetDistinctShardLocations Complete; Duration: {}",
          stopwatch.elapsed(TimeUnit.MILLISECONDS));

      return result;
    }
  }

  /**
   * Upgrades store hosting global shard map to the latest version supported by library.
   */
  public void upgradeGlobalStore() {
    try (ActivityIdScope activityIdScope = new ActivityIdScope(UUID.randomUUID())) {
      log.info("ShardMapManager UpgradeGlobalShardMapManager Latest Version Start; ");

      Stopwatch stopwatch = Stopwatch.createStarted();

      this.upgradeStoreGlobal(GlobalConstants.GsmVersionClient);

      stopwatch.stop();

      log.info("ShardMapManager UpgradeGlobalShardMapManager Complete; Duration: {}",
          stopwatch.elapsed(TimeUnit.MILLISECONDS));
    }
  }

  /**
   * Upgrades store hosting global shard map to specified version. This will be used for upgrade
   * testing.
   *
   * @param targetVersion Target store version to deploy.
   */
  public void upgradeGlobalStore(Version targetVersion) {
    try (ActivityIdScope activityIdScope = new ActivityIdScope(UUID.randomUUID())) {
      log.info("ShardMapManager UpgradeGlobalShardMapManager Start; Version : {}", targetVersion);

      Stopwatch stopwatch = Stopwatch.createStarted();

      this.upgradeStoreGlobal(targetVersion);

      stopwatch.stop();

      log.info("ShardMapManager UpgradeGlobalShardMapManager Complete; Duration: {}",
          stopwatch.elapsed(TimeUnit.MILLISECONDS));
    }
  }

  /**
   * Upgrades store location to the specified version. This will be used for upgrade testing.
   *
   * @param location Shard location to upgrade.
   * @param targetVersion Target store version to deploy.
   */
  public void upgradeLocalStore(ShardLocation location, Version targetVersion) {
    try (ActivityIdScope activityIdScope = new ActivityIdScope(UUID.randomUUID())) {
      log.info("ShardMapManager UpgradeGlobalShardMapManager Start; ");

      Stopwatch stopwatch = Stopwatch.createStarted();

      this.upgradeStoreLocal(location, targetVersion);

      stopwatch.stop();

      log.info("ShardMapManager UpgradeGlobalShardMapManager Complete; Duration: {}",
          stopwatch.elapsed(TimeUnit.MILLISECONDS));
    }
  }

  /**
   * Upgrades store location to the latest version supported by library.
   *
   * @param location Shard location to upgrade.
   */
  public void upgradeLocalStore(ShardLocation location) {
    try (ActivityIdScope activityIdScope = new ActivityIdScope(UUID.randomUUID())) {
      log.info("ShardMapManager UpgradeGlobalShardMapManager Start; ");

      Stopwatch stopwatch = Stopwatch.createStarted();

      this.upgradeStoreLocal(location, GlobalConstants.LsmVersionClient);

      stopwatch.stop();

      log.info("ShardMapManager UpgradeGlobalShardMapManager Complete; Duration: {}",
          stopwatch.elapsed(TimeUnit.MILLISECONDS));
    }
  }

  /**
   * Obtains the recovery manager for the current shard map manager instance.
   *
   * @return Recovery manager for the shard map manager.
   */
  public RecoveryManager getRecoveryManager() {
    return new RecoveryManager(this);
  }

  /**
   * Obtains the schema info collection object for the current shard map manager instance.
   *
   * @return schema info collection for shard map manager.
   */
  public SchemaInfoCollection getSchemaInfoCollection() {
    return new SchemaInfoCollection(this);
  }

  /**
   * Finds a shard map from cache if requested and if necessary from global shard map.
   *
   * @param operationName Operation name, useful for diagnostics.
   * @param shardMapName Name of shard map.
   * @param lookInCacheFirst Whether to skip first lookup in cache.
   * @return Shard map object corresponding to one being searched.
   */
  public ShardMap lookupShardMapByName(String operationName, String shardMapName,
      boolean lookInCacheFirst) {
    StoreShardMap ssm = null;

    if (lookInCacheFirst) {
      // Typical scenario will result in immediate lookup succeeding.
      ssm = this.getCache().lookupShardMapByName(shardMapName);
    }

    ShardMap shardMap;

    // Cache miss. Go to store and add entry to cache.
    if (ssm == null) {
      Stopwatch stopwatch = Stopwatch.createStarted();

      shardMap = this.lookupShardMapByNameInStore(operationName, shardMapName);

      stopwatch.stop();

      log.info("Lookup ShardMap: {} in store complete; Duration: {}",
          shardMapName, stopwatch.elapsed(TimeUnit.MILLISECONDS));
    } else {
      shardMap = ShardMapUtils.createShardMapFromStoreShardMap(this, ssm);
    }

    return shardMap;
  }

  /**
   * Subscriber function to RetryPolicy.retrying event.
   *
   * @param sender Sender object (RetryPolicy)
   * @param arg Event argument.
   */
  public void shardMapManagerRetryingEventHandler(Object sender, RetryingEventArgs arg) {
    // Trace out retry event.
    log.info("ShardMapManager ShardMapManagerRetryingEvent Retry Count: {}; Delay: {}",
        arg.getCurrentRetryCount(), arg.getDelay());

    this.onShardMapManagerRetryingEvent(new RetryingEventArgs(arg));
  }

  /**
   * Publisher for ShardMapManagerRetryingEvent event.
   *
   * @param arg Event argument.
   */
  public void onShardMapManagerRetryingEvent(RetryingEventArgs arg) {
    //TODO
    /*EventHandler<RetryingEventArgs> handler = (Object sender, RetryingEventArgs e) ->
        ShardMapManagerRetrying.invoke(sender, e);
    if (handler != null) {
      handler.invoke(this, arg);
    }*/
  }

  /**
   * Performs lookup and conversion operation for shard map with given name.
   * <typeparam name="TShardMap">Type to convert shard map to.</typeparam>
   *
   * @param operationName Operation name, useful for diagnostics.
   * @param shardMapName Shard map name.
   * @param throwOnFailure Whether to throw exception or return null on failure.
   * @return The converted shard map.
   */
  private ShardMap lookupAndConvertShardMapHelper(String operationName, String shardMapName,
      boolean throwOnFailure) {
    ShardMap sm = this.lookupShardMapByName(operationName, shardMapName, true);

    if (sm == null && throwOnFailure) {
      throw new ShardManagementException(ShardManagementErrorCategory.ShardMapManager,
          ShardManagementErrorCode.ShardMapLookupFailure,
          Errors._ShardMapManager_ShardMapLookupFailed, shardMapName,
          this.credentials.getShardMapManagerLocation());
    }
    return sm;
  }

  /**
   * Loads the shard map manager and shards from Store.
   */
  private void loadFromStore() {
    this.getCache().clear();

    try (IStoreOperationGlobal op = this.getStoreOperationFactory()
        .createLoadShardMapManagerGlobalOperation(this, "GetShardMapManager")) {
      op.doGlobal();
    } catch (Exception e) {
      e.printStackTrace();
      throw (ShardManagementException) e.getCause();
    }
  }

  /**
   * Adds a shard to global shard map.
   *
   * @param operationName Operation name, useful for diagnostics.
   * @param ssm Storage representation of shard map object.
   */
  private void addShardMapToStore(String operationName, StoreShardMap ssm)
      throws ShardManagementException {
    try (IStoreOperationGlobal op = this.getStoreOperationFactory()
        .createAddShardMapGlobalOperation(this, operationName, ssm)) {
      op.doGlobal();
    } catch (Exception e) {
      e.printStackTrace();
      ExceptionUtils.throwShardManagementOrStoreException(e);
    }
  }

  /**
   * Removes a shard map from global shard map.
   *
   * @param ssm Shard map to remove.
   */
  private void removeShardMapFromStore(StoreShardMap ssm) {
    try (IStoreOperationGlobal op = this.getStoreOperationFactory()
        .createRemoveShardMapGlobalOperation(this, "DeleteShardMap", ssm)) {
      op.doGlobal();
    } catch (Exception e) {
      e.printStackTrace();
      ExceptionUtils.throwShardManagementOrStoreException(e);
    }
  }

  /**
   * Obtains all ShardMaps associated with the shard map manager.
   *
   * @return Collection of shard maps associated with the shard map manager.
   */
  private List<ShardMap> getShardMapsFromStore() {
    StoreResults result = null;

    try (IStoreOperationGlobal op = this.getStoreOperationFactory()
        .createGetShardMapsGlobalOperation(this, "GetShardMaps")) {
      result = op.doGlobal();
    } catch (Exception e) {
      e.printStackTrace();
      throw (ShardManagementException) e.getCause();
    }

    assert result != null;
    return result.getStoreShardMaps().stream()
        .map(ssm -> ShardMapUtils.createShardMapFromStoreShardMap(this, ssm))
        .collect(Collectors.toList());
  }

  /**
   * Get distinct locations for the shard map manager from store.
   *
   * @return Distinct locations from shard map manager.
   */
  private List<ShardLocation> getDistinctShardLocationsFromStore() {
    StoreResults result = null;

    try (IStoreOperationGlobal op = this.getStoreOperationFactory()
        .createGetDistinctShardLocationsGlobalOperation(this, "GetDistinctShardLocations")) {
      result = op.doGlobal();
    } catch (Exception e) {
      e.printStackTrace();
      throw (ShardManagementException) e.getCause();
    }

    assert result != null;
    return result.getStoreLocations();
  }

  /**
   * Upgrades store hosting GSM.
   *
   * @param targetVersion Target version for store to upgrade to.
   */
  private void upgradeStoreGlobal(Version targetVersion) {
    try (IStoreOperationGlobal op = this.getStoreOperationFactory()
        .createUpgradeStoreGlobalOperation(this, "UpgradeStoreGlobal", targetVersion)) {
      op.doGlobal();
    } catch (Exception e) {
      e.printStackTrace();
      ExceptionUtils.throwShardManagementOrStoreException(e);
    }
  }

  /**
   * Upgrades store at specified location.
   *
   * @param location Store location to upgrade.
   * @param targetVersion Target version for store to upgrade to.
   */
  private void upgradeStoreLocal(ShardLocation location, Version targetVersion) {
    try (IStoreOperationLocal op = this.getStoreOperationFactory()
        .createUpgradeStoreLocalOperation(this, location, "UpgradeStoreLocal", targetVersion)) {
      op.doLocal();
    } catch (IOException e) {
      e.printStackTrace();
      throw (ShardManagementException) e.getCause();
    }
  }

  /**
   * Finds shard map with given name in global shard map.
   *
   * @param operationName Operation name, useful for diagnostics.
   * @param shardMapName Name of shard map to search.
   * @return Shard map corresponding to given Id.
   */
  private ShardMap lookupShardMapByNameInStore(String operationName, String shardMapName) {
    StoreResults result;

    try (IStoreOperationGlobal op = this.getStoreOperationFactory()
        .createFindShardMapByNameGlobalOperation(this, operationName, shardMapName)) {
      result = op.doGlobal();
      return result.getStoreShardMaps()
          .stream().map(ssm -> ShardMapUtils.createShardMapFromStoreShardMap(this, ssm))
          .findFirst().orElse(null);
    } catch (Exception e) {
      e.printStackTrace();
      throw (ShardManagementException) e.getCause();
    }
  }

  /**
   * Validates the input shard map. This includes:
   * Ensuring that shard map belongs to this instance of shard map manager.
   *
   * @param shardMap Input shard map.
   */
  private void validateShardMap(ShardMap shardMap) {
    ExceptionUtils.disallowNullArgument(shardMap, "shardMap");

    if (shardMap.getShardMapManager() != this) {
      throw new IllegalStateException(String.format(
          Errors._ShardMapManager_DifferentShardMapManager, shardMap.getName(),
          this.getCredentials().getShardMapManagerLocation()));
    }
  }
}

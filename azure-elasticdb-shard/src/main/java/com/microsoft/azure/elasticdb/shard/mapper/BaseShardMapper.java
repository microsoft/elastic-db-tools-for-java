package com.microsoft.azure.elasticdb.shard.mapper;

/* Copyright (c) Microsoft. All rights reserved.
Licensed under the MIT license. See LICENSE file in the project root for full license information.*/

import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import com.microsoft.azure.elasticdb.core.commons.helpers.ActionGeneric3Param;
import com.microsoft.azure.elasticdb.shard.base.IMappingInfoProvider;
import com.microsoft.azure.elasticdb.shard.base.IMappingUpdate;
import com.microsoft.azure.elasticdb.shard.base.IShardProvider;
import com.microsoft.azure.elasticdb.shard.base.LockOwnerIdOpType;
import com.microsoft.azure.elasticdb.shard.base.MappingKind;
import com.microsoft.azure.elasticdb.shard.base.MappingLockToken;
import com.microsoft.azure.elasticdb.shard.base.Range;
import com.microsoft.azure.elasticdb.shard.base.Shard;
import com.microsoft.azure.elasticdb.shard.base.ShardKey;
import com.microsoft.azure.elasticdb.shard.base.ShardRange;
import com.microsoft.azure.elasticdb.shard.cache.CacheStoreMappingUpdatePolicy;
import com.microsoft.azure.elasticdb.shard.cache.ICacheStoreMapping;
import com.microsoft.azure.elasticdb.shard.cache.PerformanceCounterName;
import com.microsoft.azure.elasticdb.shard.map.ShardMap;
import com.microsoft.azure.elasticdb.shard.mapmanager.ShardManagementErrorCategory;
import com.microsoft.azure.elasticdb.shard.mapmanager.ShardManagementErrorCode;
import com.microsoft.azure.elasticdb.shard.mapmanager.ShardManagementException;
import com.microsoft.azure.elasticdb.shard.mapmanager.ShardMapManager;
import com.microsoft.azure.elasticdb.shard.store.StoreMapping;
import com.microsoft.azure.elasticdb.shard.store.StoreResult;
import com.microsoft.azure.elasticdb.shard.store.StoreResults;
import com.microsoft.azure.elasticdb.shard.store.StoreShard;
import com.microsoft.azure.elasticdb.shard.storeops.base.IStoreOperation;
import com.microsoft.azure.elasticdb.shard.storeops.base.IStoreOperationGlobal;
import com.microsoft.azure.elasticdb.shard.storeops.base.StoreOperationCode;
import com.microsoft.azure.elasticdb.shard.storeops.base.StoreOperationRequestBuilder;
import com.microsoft.azure.elasticdb.shard.utils.Errors;
import com.microsoft.azure.elasticdb.shard.utils.ExceptionUtils;
import com.microsoft.azure.elasticdb.shard.utils.IdLock;
import com.microsoft.azure.elasticdb.shard.utils.StringUtilsLocal;
import com.microsoft.sqlserver.jdbc.SQLServerConnection;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Base class for keyed mappers.
 */
public abstract class BaseShardMapper {

  protected static final UUID DEFAULT_OWNER = UUID.randomUUID();

  private final static Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  /**
   * Reference to ShardMapManager.
   */
  protected ShardMapManager shardMapManager;

  /**
   * Containing shard map.
   */
  protected ShardMap shardMap;

  /**
   * Base shard mapper, which is just a holder of some fields.
   *
   * @param shardMapManager Reference to ShardMapManager.
   * @param sm Containing shard map.
   */
  protected BaseShardMapper(ShardMapManager shardMapManager, ShardMap sm) {
    this.shardMapManager = Preconditions.checkNotNull(shardMapManager);
    this.shardMap = Preconditions.checkNotNull(sm);
  }

  /**
   * Sets the status of a shardmapping
   * <p>
   * <typeparam name="TMapping">Mapping type.</typeparam>
   * <typeparam name="TUpdate">Update type.</typeparam>
   * <typeparam name="TStatus">Status type.</typeparam>
   *
   * @param mapping Mapping being added.
   * @param status Status of <paramref name="mapping">mapping</paramref> being added.
   * @param getStatus Delegate to construct new status from <paramref name="status">input
   * status</paramref>.
   * @param createUpdate Delegate to construct new update from new status returned by <paramref
   * name="getStatus">getStatus</paramref>.
   * @param runUpdate Delegate to perform update from the <paramref name="mapping">input
   * mapping</paramref> and the update object returned by <paramref name="getStatus">createUpdate</paramref>.
   */
  protected static <TMapping, TUpdate, TStatus> TMapping SetStatus(TMapping mapping, TStatus status,
      Function<TStatus, TStatus> getStatus, Function<TStatus, TUpdate> createUpdate,
      ActionGeneric3Param<TMapping, TUpdate, UUID, TMapping> runUpdate) {
    return SetStatus(mapping, status, getStatus, createUpdate, runUpdate, DEFAULT_OWNER);
  }

  /**
   * Sets the status of a shardmapping
   * <p>
   * <typeparam name="TMapping">Mapping type.</typeparam>
   * <typeparam name="TUpdate">Update type.</typeparam>
   * <typeparam name="TStatus">Status type.</typeparam>
   *
   * @param mapping Mapping being added.
   * @param status Status of <paramref name="mapping">mapping</paramref> being added.
   * @param getStatus Delegate to construct new status from <paramref name="status">input
   * status</paramref>.
   * @param createUpdate Delegate to construct new update from new status returned by <paramref
   * name="getStatus">getStatus</paramref>.
   * @param runUpdate Delegate to perform update from the <paramref name="mapping">input
   * mapping</paramref> and the update object returned by <paramref name="getStatus">createUpdate</paramref>.
   * @param lockOwnerId Lock owner id of this mapping
   */
  protected static <TMapping, TUpdate, TStatus> TMapping SetStatus(TMapping mapping, TStatus status,
      Function<TStatus, TStatus> getStatus, Function<TStatus, TUpdate> createUpdate,
      ActionGeneric3Param<TMapping, TUpdate, UUID, TMapping> runUpdate, UUID lockOwnerId) {
    TStatus newStatus = getStatus.apply(status);
    TUpdate update = createUpdate.apply(newStatus);
    return runUpdate.invoke(mapping, update, lockOwnerId);
  }

  protected final ShardMapManager getShardMapManager() {
    return shardMapManager;
  }

  protected final ShardMap getShardMap() {
    return shardMap;
  }

  /**
   * Given a key value, obtains a SqlConnection to the shard in the mapping
   * that contains the key value.
   * <p>
   * <typeparam name="TMapping">Mapping type.</typeparam>
   * <typeparam name="TKey">Key type.</typeparam>
   *
   * @param key Input key value.
   * @param constructMapping Delegate to construct a mapping object.
   * @param errorCategory Error category.
   * @param connectionString Connection string with credential information, the DataSource and
   * Database are obtained from the results of the lookup operation for key.
   * @return An opened SqlConnection.
   */

  protected final <TMapping extends IShardProvider, TKey> SQLServerConnection OpenConnectionForKey(
      TKey key,
      ActionGeneric3Param<ShardMapManager, ShardMap, StoreMapping, TMapping> constructMapping,
      ShardManagementErrorCategory errorCategory, String connectionString) {
    return OpenConnectionForKey(key, constructMapping, errorCategory, connectionString,
        ConnectionOptions.Validate);
  }

  /**
   * Given a key value, obtains a SqlConnection to the shard in the mapping
   * that contains the key value.
   * <p>
   * <typeparam name="TMapping">Mapping type.</typeparam>
   * <typeparam name="TKey">Key type.</typeparam>
   *
   * @param key Input key value.
   * @param constructMapping Delegate to construct a mapping object.
   * @param errorCategory Error category.
   * @param connectionString Connection string with credential information, the DataSource and
   * Database are obtained from the results of the lookup operation for key.
   * @param options Options for validation operations to perform on opened connection.
   * @return An opened SqlConnection.
   */
  protected final <TMapping extends IShardProvider, TKey> SQLServerConnection OpenConnectionForKey(
      TKey key,
      ActionGeneric3Param<ShardMapManager, ShardMap, StoreMapping, TMapping> constructMapping,
      ShardManagementErrorCategory errorCategory, String connectionString,
      ConnectionOptions options) {
    ShardKey sk = new ShardKey(ShardKey.shardKeyTypeFromType(key.getClass()), key);

    // Try to find the mapping within the cache.
    ICacheStoreMapping csm = shardMapManager.getCache()
        .lookupMappingByKey(shardMap.getStoreShardMap(), sk);

    StoreMapping sm;

    if (csm != null) {
      sm = csm.getMapping();
    } else {
      sm = this
          .LookupMappingForOpenConnectionForKey(sk, CacheStoreMappingUpdatePolicy.OverwriteExisting,
              errorCategory);
    }

    SQLServerConnection result;

    try {
      // Initially attempt to connect based on lookup results from either cache or GSM.
      result = shardMap.OpenConnection(constructMapping.invoke(this.getShardMapManager(),
          this.getShardMap(), sm), connectionString, options);

      // Reset TTL on successful connection.
      if (csm != null && csm.getTimeToLiveMilliseconds() > 0) {
        csm.ResetTimeToLive();
      }

      shardMapManager.getCache().incrementPerformanceCounter(shardMap.getStoreShardMap(),
          PerformanceCounterName.DdrOperationsPerSec);
      return result;
    } catch (ShardManagementException smme) {
      // If we hit a validation failure due to stale version of mapping, we will perform one more attempt.
      if (((options.getValue() & ConnectionOptions.Validate.getValue())
          == ConnectionOptions.Validate.getValue())
          && smme.getErrorCategory() == ShardManagementErrorCategory.Validation
          && smme.getErrorCode() == ShardManagementErrorCode.MappingDoesNotExist) {
        // Assumption here is that this time the attempt should succeed since the cache entry
        // has already been either evicted, or updated based on latest data from the server.
        sm = this.LookupMappingForOpenConnectionForKey(sk,
            CacheStoreMappingUpdatePolicy.OverwriteExisting, errorCategory);

        result = shardMap.OpenConnection(
            constructMapping.invoke(this.getShardMapManager(), this.getShardMap(), sm),
            connectionString, options);
        shardMapManager.getCache().incrementPerformanceCounter(shardMap.getStoreShardMap(),
            PerformanceCounterName.DdrOperationsPerSec);
        return result;
      } else {
        // The error was not due to validation but something else e.g.
        // 1) Shard map does not exist
        // 2) Mapping could not be found.
        throw smme;
      }
    } catch (Exception e) {
      //TODO: Change this catch block back to SQLException once all inner methods are implemented
      // We failed to connect. If we were trying to connect from an entry in cache and mapping expired in cache.
      if (csm != null && (System.nanoTime() - csm.getCreationTime()) >= csm
          .getTimeToLiveMilliseconds()) {
        try (IdLock _idLock = new IdLock(csm.getMapping().getStoreShard().getId())) {
          // Similar to DCL pattern, we need to refresh the mapping again to see if we still need to go to the store
          // to lookup the mapping after acquiring the shard lock. It might be the case that a fresh version has already
          // been obtained by some other thread.
          csm = shardMapManager.getCache().lookupMappingByKey(shardMap.getStoreShardMap(), sk);

          // Only go to store if the mapping is stale even after refresh.
          if (csm == null || (System.nanoTime() - csm.getCreationTime()) >= csm
              .getTimeToLiveMilliseconds()) {
            // Refresh the mapping in cache. And try to open the connection after refresh.
            sm = this.LookupMappingForOpenConnectionForKey(sk,
                CacheStoreMappingUpdatePolicy.UpdateTimeToLive, errorCategory);
          } else {
            sm = csm.getMapping();
          }
        } catch (IOException e1) {
          e1.printStackTrace();
        }

        result = shardMap.OpenConnection(
            constructMapping.invoke(this.getShardMapManager(), this.getShardMap(), sm),
            connectionString, options);

        // Reset TTL on successful connection.
        if (csm != null && csm.getTimeToLiveMilliseconds() > 0) {
          csm.ResetTimeToLive();
        }

        shardMapManager.getCache().incrementPerformanceCounter(shardMap.getStoreShardMap(),
            PerformanceCounterName.DdrOperationsPerSec);
        return result;
      } else {
        // Either:
        // 1) The mapping is still within the TTL. No refresh.
        // 2) Mapping was not in cache, we originally did a lookup for mapping in GSM
        // and even then could not connect.
        throw e;
      }
    }
  }

  protected final <TMapping extends IShardProvider, TKey> Callable<SQLServerConnection> OpenConnectionForKeyAsync(
      TKey key,
      ActionGeneric3Param<ShardMapManager, ShardMap, StoreMapping, TMapping> constructMapping,
      ShardManagementErrorCategory errorCategory, String connectionString) {
    return OpenConnectionForKeyAsync(key, constructMapping, errorCategory, connectionString,
        ConnectionOptions.Validate);
  }

  /**
   * Given a key value, asynchronously obtains a SqlConnection to the shard in the mapping
   * that contains the key value.
   * <p>
   * <typeparam name="TMapping">Mapping type.</typeparam>
   * <typeparam name="TKey">Key type.</typeparam>
   *
   * @param key Input key value.
   * @param constructMapping Delegate to construct a mapping object.
   * @param errorCategory Error category.
   * @param connectionString Connection string with credential information, the DataSource and
   * Database are obtained from the results of the lookup operation for key.
   * @param options Options for validation operations to perform on opened connection.
   * @return A task encapsulating an opened SqlConnection as the result.
   */

  protected final <TMapping extends IShardProvider, TKey> Callable<SQLServerConnection> OpenConnectionForKeyAsync(
      TKey key,
      ActionGeneric3Param<ShardMapManager, ShardMap, StoreMapping, TMapping> constructMapping,
      ShardManagementErrorCategory errorCategory, String connectionString,
      ConnectionOptions options) {
    return () -> {
      return OpenConnectionForKey(key, constructMapping, errorCategory, connectionString, options);
    };
  }

  /**
   * Adds a mapping to shard map.
   * <p>
   * <typeparam name="TMapping">Mapping type.</typeparam>
   *
   * @param mapping Mapping being added.
   * @param constructMapping Delegate to construct a mapping object.
   * @return The added mapping object.
   */
  protected final <TMapping extends IShardProvider & IMappingInfoProvider> TMapping Add(
      TMapping mapping,
      ActionGeneric3Param<ShardMapManager, ShardMap, StoreMapping, TMapping> constructMapping) {
    ExceptionUtils.EnsureShardBelongsToShardMap(this.getShardMapManager(), this.getShardMap(),
        mapping.getShardInfo(), "CreateMapping",
        mapping.getKind() == MappingKind.PointMapping ? "PointMapping" : "RangeMapping");

    this.EnsureMappingBelongsToShardMap(mapping, "Add", "mapping");

    TMapping newMapping = constructMapping.invoke(this.getShardMapManager(),
        this.getShardMap(),
        new StoreMapping(mapping.getStoreMapping().getId(),
            mapping.getStoreMapping().getShardMapId(),
            mapping.getStoreMapping().getMinValue(),
            mapping.getStoreMapping().getMaxValue(),
            mapping.getStoreMapping().getStatus(),
            mapping.getStoreMapping().getLockOwnerId(),
            new StoreShard(mapping.getShardInfo().getStoreShard().getId(), UUID.randomUUID(),
                mapping.getShardInfo().getStoreShard().getShardMapId(),
                mapping.getShardInfo().getStoreShard().getLocation(),
                mapping.getShardInfo().getStoreShard().getStatus())
        ));

    try (IStoreOperation op = shardMapManager.getStoreOperationFactory()
        .CreateAddMappingOperation(this.getShardMapManager(),
            mapping.getKind() == MappingKind.RangeMapping ? StoreOperationCode.AddRangeMapping
                : StoreOperationCode.AddPointMapping, shardMap.getStoreShardMap(),
            newMapping.getStoreMapping())) {
      op.Do();
    } catch (Exception e) {
      e.printStackTrace();
    }

    return newMapping;
  }

  /**
   * Removes a mapping from shard map.
   * <p>
   * <typeparam name="TMapping">Mapping type.</typeparam>
   *
   * @param mapping Mapping being removed.
   * @param constructMapping Delegate to construct a mapping object.
   * @param lockOwnerId Lock owner id of this mapping
   */
  protected final <TMapping extends IShardProvider & IMappingInfoProvider> void Remove(
      TMapping mapping,
      ActionGeneric3Param<ShardMapManager, ShardMap, StoreMapping, TMapping> constructMapping,
      UUID lockOwnerId) {
    this.<TMapping>EnsureMappingBelongsToShardMap(mapping, "Remove", "mapping");

    TMapping newMapping = constructMapping.invoke(this.getShardMapManager(), this.getShardMap(),
        new StoreMapping(mapping.getStoreMapping().getId(),
            mapping.getStoreMapping().getShardMapId(),
            mapping.getStoreMapping().getMinValue(),
            mapping.getStoreMapping().getMaxValue(),
            mapping.getStoreMapping().getStatus(),
            mapping.getStoreMapping().getLockOwnerId(),
            new StoreShard(mapping.getShardInfo().getId(), UUID.randomUUID(),
                mapping.getShardInfo().getStoreShard().getShardMapId(),
                mapping.getShardInfo().getStoreShard().getLocation(),
                mapping.getShardInfo().getStoreShard().getStatus()))
    );

    try (IStoreOperation op = shardMapManager.getStoreOperationFactory()
        .CreateRemoveMappingOperation(this.getShardMapManager(),
            mapping.getKind() == MappingKind.RangeMapping ? StoreOperationCode.RemoveRangeMapping
                : StoreOperationCode.RemovePointMapping, shardMap.getStoreShardMap(),
            newMapping.getStoreMapping(), lockOwnerId)) {
      op.Do();
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  /**
   * Looks up the key value and returns the corresponding mapping.
   * <p>
   * <typeparam name="TMapping">Mapping type.</typeparam>
   * <typeparam name="TKey">Key type.</typeparam>
   *
   * @param key Input key value.
   * @param useCache Whether to use cache for lookups.
   * @param constructMapping Delegate to construct a mapping object.
   * @param errorCategory Category under which errors must be thrown.
   * @return Mapping that contains the key value.
   */
  protected final <TMapping extends IShardProvider, TKey> TMapping Lookup(TKey key,
      boolean useCache,
      ActionGeneric3Param<ShardMapManager, ShardMap, StoreMapping, TMapping> constructMapping,
      ShardManagementErrorCategory errorCategory) {
        /*ShardKey sk = new ShardKey(ShardKey.shardKeyTypeFromType(TKey.class), key);

        if (useCache) {
            ICacheStoreMapping cachedMapping = shardMapManager.getCache().lookupMappingByKey(shardMap.getStoreShardMap(), sk);

            if (cachedMapping != null) {
                return constructMapping.invoke(this.getShardMapManager(), this.getShardMap(), cachedMapping.getMapping());
            }
        }

        // Cache-miss, find mapping for given key in GSM.
        TMapping m = null;

        StoreResults gsmResult;

        Stopwatch stopwatch = Stopwatch.createStarted();

        try (IStoreOperationGlobal op = shardMapManager.getStoreOperationFactory().CreateFindMappingByKeyGlobalOperation(this.getShardMapManager(), "Lookup", shardMap.getStoreShardMap(), sk, CacheStoreMappingUpdatePolicy.OverwriteExisting, errorCategory, true, false)) {
            gsmResult = op.Do();
        }

        stopwatch.stop();

        log.info("Lookup", "Lookup key from GSM complete; Key type : {} Result: {}; Duration: {}", TKey.class, gsmResult.getResult(), stopwatch.elapsed(TimeUnit.MILLISECONDS));

        // If we could not locate the mapping, we return null and do nothing here.
        if (gsmResult.getResult() != StoreResult.MappingNotFoundForKey) {
            return gsmResult.getStoreMapping()
            s.Select(sm -> constructMapping.invoke(this.getShardMapManager(), this.getShardMap(), sm)).Single();
        }

        return m;*/
    return null; //TODO
  }

  /**
   * Finds mapping in store for OpenConnectionForKey operation.
   *
   * @param sk Key to find.
   * @param policy Cache update policy.
   * @param errorCategory Error category.
   * @return Mapping corresponding to the given key if found.
   */
  private StoreMapping LookupMappingForOpenConnectionForKey(ShardKey sk,
      CacheStoreMappingUpdatePolicy policy, ShardManagementErrorCategory errorCategory) {
    StoreResults gsmResult = null;

    Stopwatch stopwatch = Stopwatch.createStarted();

    try (IStoreOperationGlobal op = shardMapManager.getStoreOperationFactory()
        .CreateFindMappingByKeyGlobalOperation(this.getShardMapManager(), "Lookup",
            shardMap.getStoreShardMap(), sk, policy, errorCategory, true, false)) {
      gsmResult = op.Do();
    } catch (Exception e) {
      e.printStackTrace();
      return null; //TODO Handle Exception
    }

    stopwatch.stop();

    log.info("LookupMappingForOpenConnectionForKey",
        "Lookup key from GSM complete; Key type : {} Result: {}; Duration: {}", sk.getDataType(),
        gsmResult.getResult(), stopwatch.elapsed(TimeUnit.MILLISECONDS));

    // If we could not locate the mapping, we throw.
    if (gsmResult.getResult() == StoreResult.MappingNotFoundForKey) {
      throw new ShardManagementException(errorCategory,
          ShardManagementErrorCode.MappingNotFoundForKey,
          Errors._Store_ShardMapper_MappingNotFoundForKeyGlobal, shardMap.getName(),
          StoreOperationRequestBuilder.SP_FIND_SHARD_MAPPING_BY_KEY_GLOBAL,
          "LookupMappingForOpenConnectionForKey");
    } else {
      return gsmResult.getStoreMappings().get(0);
    }
  }


  /**
   * Asynchronously finds the mapping in store for OpenConnectionForKey operation.
   *
   * @param sk Key to find.
   * @param policy Cache update policy.
   * @param errorCategory Error category.
   * @return Task with the Mapping corresponding to the given key if found as the result.
   */
  private Callable<StoreMapping> LookupMappingForOpenConnectionForKeyAsync(ShardKey sk,
      CacheStoreMappingUpdatePolicy policy, ShardManagementErrorCategory errorCategory) {
        /*StoreResults gsmResult;

        Stopwatch stopwatch = Stopwatch.createStarted();

        try (IStoreOperationGlobal op = shardMapManager.getStoreOperationFactory().CreateFindMappingByKeyGlobalOperation(this.getShardMapManager(), "Lookup", shardMap.getStoreShardMap(), sk, policy, errorCategory, true, false)) {
//TODO TASK: There is no equivalent to 'await' in Java:
            gsmResult = await op.DoAsync().ConfigureAwait(false);
        }

        stopwatch.stop();

        log.info("LookupMappingForOpenConnectionForKeyAsync", "Lookup key from GSM complete; Key type : {} Result: {}; Duration: {}", sk.getDataType(), gsmResult.getResult(), stopwatch.elapsed(TimeUnit.MILLISECONDS));

        // If we could not locate the mapping, we throw.
        if (gsmResult.getResult() == StoreResult.MappingNotFoundForKey) {
            throw new ShardManagementException(errorCategory, ShardManagementErrorCode.MappingNotFoundForKey, Errors._Store_ShardMapper_MappingNotFoundForKeyGlobal, shardMap.getName(), StoreOperationRequestBuilder.SP_FIND_SHARD_MAPPING_BY_KEY_GLOBAL, "LookupMappingForOpenConnectionForKeyAsync");
        } else {
            return Iterables.getOnlyElement(gsmResult.getStoreMappings());
        }*/
    return null; //TODO
  }

  /**
   * Gets all the mappings that exist within given range.
   *
   * @param range Optional range value, if null, we cover everything.
   * @param shard Optional shard parameter, if null, we cover all shards.
   * @param constructMapping Delegate to construct a mapping object.
   * @param errorCategory Category under which errors will be posted.
   * @param mappingType Name of mapping type.
   * @return Read-only collection of mappings that overlap with given range.
   */
  protected final <TMapping> List<TMapping> GetMappingsForRange(Range range
      , Shard shard
      , ActionGeneric3Param<ShardMapManager, ShardMap, StoreMapping, TMapping> constructMapping
      , ShardManagementErrorCategory errorCategory
      , String mappingType) {
    ShardRange sr = null;

    if (shard != null) {
      ExceptionUtils
          .EnsureShardBelongsToShardMap(this.getShardMapManager(), this.getShardMap(), shard,
              "GetMappings", mappingType);
    }

    if (range != null) {
      sr = range.getShardRange();
    }

    StoreResults result;

    try (IStoreOperationGlobal op = shardMapManager.getStoreOperationFactory()
        .CreateGetMappingsByRangeGlobalOperation(shardMapManager,
            "GetMappingsForRange",
            shardMap.getStoreShardMap(),
            shard != null ? shard.getStoreShard() : null,
            sr,
            errorCategory,
            true, // cacheResults
            false // ignoreFailure
        )
    ) {
      result = op.Do();
    } catch (Exception e) {
      e.printStackTrace();
      return null; //TODO
    }

    return Collections.unmodifiableList(
        result.getStoreMappings()
            .stream()
            .map(sm -> constructMapping.invoke(this.getShardMapManager(), this.getShardMap(), sm))
            .collect(Collectors.toList()));
  }

  /**
   * Allows for update to a mapping with the updates provided in
   * the <paramref name="update"/> parameter.
   *
   * @param currentMapping Mapping being updated.
   * @param update Updated properties of the Shard.
   * @param constructMapping Delegate to construct a mapping object.
   * @param statusAsInt Delegate to get the mapping status as an integer value.
   * @param intAsStatus Delegate to get the mapping status from an integer value.
   * @return New instance of mapping with updated information.
   */
  protected final <TMapping extends IShardProvider & IMappingInfoProvider, TUpdate extends IMappingUpdate<TStatus>, TStatus> TMapping Update(
      TMapping currentMapping, TUpdate update,
      ActionGeneric3Param<ShardMapManager, ShardMap, StoreMapping, TMapping> constructMapping,
      Function<TStatus, Integer> statusAsInt, Function<Integer, TStatus> intAsStatus) {
    return Update(currentMapping, update, constructMapping, statusAsInt, intAsStatus,
        new UUID(0L, 0L));
  }

  //TODO TASK: The C# 'struct' constraint has no equivalent in Java:
//ORIGINAL LINE: protected TMapping Update<TMapping, TUpdate, TStatus>(TMapping currentMapping, TUpdate update, Func<ShardMapManager, ShardMap, StoreMapping, TMapping> constructMapping, Func<TStatus, int> statusAsInt, Func<int, TStatus> intAsStatus, Guid lockOwnerId = default(Guid)) where TUpdate : class, IMappingUpdate<TStatus> where TMapping : class, IShardProvider, IMappingInfoProvider where TStatus : struct

  /**
   * Allows for update to a mapping with the updates provided in
   * the <paramref name="update"/> parameter.
   *
   * @param currentMapping Mapping being updated.
   * @param update Updated properties of the Shard.
   * @param constructMapping Delegate to construct a mapping object.
   * @param statusAsInt Delegate to get the mapping status as an integer value.
   * @param intAsStatus Delegate to get the mapping status from an integer value.
   * @param lockOwnerId Lock owner id of this mapping
   * @return New instance of mapping with updated information.
   */
  protected final <TMapping extends IShardProvider & IMappingInfoProvider, TUpdate extends IMappingUpdate<TStatus>, TStatus> TMapping Update(
      TMapping currentMapping, TUpdate update,
      ActionGeneric3Param<ShardMapManager, ShardMap, StoreMapping, TMapping> constructMapping,
      Function<TStatus, Integer> statusAsInt, Function<Integer, TStatus> intAsStatus,
      UUID lockOwnerId) {
    assert currentMapping != null;
    assert update != null;

    this.<TMapping>EnsureMappingBelongsToShardMap(currentMapping, "Update", "currentMapping");

        /*IMappingUpdate<TStatus> mu = (IMappingUpdate<TStatus>) ((update instanceof IMappingUpdate<TStatus>) ? update : null);

        // CONSIDER(wbasheer): Have refresh semantics for trivial case when nothing is modified.
        if (!mu.IsAnyPropertySet(MappingUpdatedProperties.All)) {
            return currentMapping;
        }

        boolean shardChanged = mu.IsAnyPropertySet(MappingUpdatedProperties.Shard) && !mu.getShard().equals(currentMapping.getShardInfo());

        // Ensure that shard belongs to current shard map.
        if (shardChanged) {
            ExceptionUtils.EnsureShardBelongsToShardMap(this.getShardMapManager(), this.getShardMap(), mu.getShard(), "UpdateMapping", currentMapping.getKind() == MappingKind.PointMapping ? "PointMapping" : "RangeMapping");
        }

        StoreShard originalShard = new DefaultStoreShard(currentMapping.getShardInfo().getId(), UUID.randomUUID(), currentMapping.getShardInfo().getStoreShard().getShardMapId(), currentMapping.getShardInfo().getStoreShard().getLocation(), currentMapping.getShardInfo().getStoreShard().getStatus());

        StoreMapping originalMapping = new StoreMapping(currentMapping.getStoreMapping().getId(), currentMapping.getShardMapId(), originalShard, currentMapping.getStoreMapping().getMinValue(), currentMapping.getStoreMapping().getMaxValue(), currentMapping.getStoreMapping().getStatus(), lockOwnerId);

        StoreShard updatedShard;

        if (shardChanged) {
            updatedShard = new DefaultStoreShard(update.getShard().getShardInfo().getId(), UUID.randomUUID(), update.getShard().getShardInfo().getStoreShard().getShardMapId(), update.getShard().getShardInfo().getStoreShard().getLocation(), update.getShard().getShardInfo().getStoreShard().getStatus());
        } else {
            updatedShard = originalShard;
        }

        StoreMapping updatedMapping = new StoreMapping(UUID.randomUUID(), currentMapping.getShardMapId(), updatedShard, currentMapping.getStoreMapping().getMinValue(), currentMapping.getStoreMapping().getMaxValue(), mu.IsAnyPropertySet(MappingUpdatedProperties.Status) ? statusAsInt.invoke(update.getStatus()) : currentMapping.getStoreMapping().getStatus(), lockOwnerId);

        boolean fromOnlineToOffline = mu.IsMappingBeingTakenOffline(intAsStatus.invoke(currentMapping.getStoreMapping().getStatus()));

        StoreOperationCode opCode;

        if (fromOnlineToOffline) {
            opCode = currentMapping.getKind() == MappingKind.PointMapping ? StoreOperationCode.UpdatePointMappingWithOffline : StoreOperationCode.UpdateRangeMappingWithOffline;
        } else {
            opCode = currentMapping.getKind() == MappingKind.PointMapping ? StoreOperationCode.UpdatePointMapping : StoreOperationCode.UpdateRangeMapping;
        }

        try (IStoreOperation op = shardMapManager.getStoreOperationFactory().CreateUpdateMappingOperation(this.getShardMapManager(), opCode, shardMap.getStoreShardMap(), originalMapping, updatedMapping, shardMap.getApplicationNameSuffix(), lockOwnerId)) {
            op.Do();
        } catch (Exception e) {
            e.printStackTrace();
        }

        return constructMapping.invoke(this.getShardMapManager(), this.getShardMap(), updatedMapping);*/
    return null; //TODO
  }

  /**
   * Gets the lock owner of a mapping.
   *
   * @param mapping The mapping
   * @param errorCategory Error category to use for the store operation
   * @return Lock owner for the mapping.
   */
  public final <TMapping extends IShardProvider & IMappingInfoProvider> UUID GetLockOwnerForMapping(
      TMapping mapping, ShardManagementErrorCategory errorCategory) {
    this.<TMapping>EnsureMappingBelongsToShardMap(mapping, "LookupLockOwner", "mapping");

    StoreResults result;

    try (IStoreOperationGlobal op = shardMapManager.getStoreOperationFactory()
        .CreateFindMappingByIdGlobalOperation(this.getShardMapManager(), "LookupLockOwner",
            shardMap.getStoreShardMap(), mapping.getStoreMapping(), errorCategory)) {
      result = op.Do();
    } catch (Exception e) {
      e.printStackTrace();
      return null; //TODO
    }

    return result.getStoreMappings().get(0).getLockOwnerId();
  }

  /**
   * Locks or unlocks a given mapping or all mappings.
   *
   * @param mapping Optional mapping
   * @param lockOwnerId The lock onwer id
   * @param lockOwnerIdOpType Operation to perform on this mapping with the given lockOwnerId
   * @param errorCategory Error category to use for the store operation
   */
  public final <TMapping extends IShardProvider & IMappingInfoProvider> void LockOrUnlockMappings(
      TMapping mapping, UUID lockOwnerId, LockOwnerIdOpType lockOwnerIdOpType,
      ShardManagementErrorCategory errorCategory) {
    String operationName = lockOwnerIdOpType == LockOwnerIdOpType.Lock ? "Lock" : "UnLock";

    if (lockOwnerIdOpType != LockOwnerIdOpType.UnlockAllMappingsForId
        && lockOwnerIdOpType != LockOwnerIdOpType.UnlockAllMappings) {
      this.<TMapping>EnsureMappingBelongsToShardMap(mapping, operationName, "mapping");

      if (lockOwnerIdOpType == LockOwnerIdOpType.Lock && lockOwnerId
          .equals(MappingLockToken.ForceUnlock.getLockOwnerId())) {
        throw new IllegalArgumentException(StringUtilsLocal
            .FormatInvariant(Errors._ShardMapping_LockIdNotSupported,
                mapping.getShardInfo().getLocation(), shardMap.getName(), lockOwnerId),
            new Throwable("lockOwnerId"));
      }
    } else {
      assert mapping == null;
    }

    try (IStoreOperationGlobal op = shardMapManager.getStoreOperationFactory()
        .CreateLockOrUnLockMappingsGlobalOperation(this.getShardMapManager(), operationName,
            shardMap.getStoreShardMap(), mapping != null ? mapping.getStoreMapping() : null,
            lockOwnerId, lockOwnerIdOpType, errorCategory)) {
      op.Do();
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  /**
   * Validates the input parameters and ensures that the mapping parameter belong to this shard map.
   *
   * @param mapping Mapping to be validated.
   * @param operationName Operation being performed.
   * @param parameterName Parameter name for mapping parameter.
   */
  protected final <TMapping extends IMappingInfoProvider> void EnsureMappingBelongsToShardMap(
      TMapping mapping, String operationName, String parameterName) {
    assert mapping.getManager() != null;

    // Ensure that shard belongs to current shard map.
    if (!mapping.getShardMapId().equals(shardMap.getId())) {
      throw new IllegalStateException(StringUtilsLocal
          .FormatInvariant(Errors._ShardMapping_DifferentShardMap, mapping.getTypeName(),
              operationName, shardMap.getName(), parameterName));
    }

    // Ensure that the mapping objects belong to same shard map.
    if (!Objects.equals(mapping.getManager(), shardMapManager)) {
      throw new IllegalStateException(StringUtilsLocal
          .FormatInvariant(Errors._ShardMapping_DifferentShardMapManager, mapping.getTypeName(),
              operationName, shardMapManager.getCredentials().getShardMapManagerLocation(),
              shardMap.getName(), parameterName));
    }
  }
}

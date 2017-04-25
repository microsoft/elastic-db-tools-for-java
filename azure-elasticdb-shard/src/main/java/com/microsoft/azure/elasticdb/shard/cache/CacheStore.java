package com.microsoft.azure.elasticdb.shard.cache;

/* Copyright (c) Microsoft. All rights reserved.
Licensed under the MIT license. See LICENSE file in the project root for full license information.*/

import com.microsoft.azure.elasticdb.core.commons.helpers.ReferenceObjectHelper;
import com.microsoft.azure.elasticdb.shard.base.ShardKey;
import com.microsoft.azure.elasticdb.shard.store.StoreMapping;
import com.microsoft.azure.elasticdb.shard.store.StoreShardMap;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Client side cache store.
 */
public class CacheStore implements ICacheStore {

  private final static Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  /**
   * Root of the cache tree.
   */
  private CacheRoot cacheRoot;

  /**
   * Constructs an instance of client side cache object.
   */
  public CacheStore() {
    cacheRoot = new CacheRoot();
  }

  /**
   * Invoked for refreshing shard map in cache from store.
   *
   * @param shardMap Storage representation of shard map.
   */
  public void addOrUpdateShardMap(StoreShardMap shardMap) {
    try (WriteLockScope wls = cacheRoot.GetWriteLockScope()) {
      cacheRoot.AddOrUpdate(shardMap);
      log.info("Cache Add/Update complete. ShardMap: {}", shardMap.getName());
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  /**
   * Invoked for deleting shard map in cache becase it no longer exists in store.
   *
   * @param shardMap Storage representation of shard map.
   */
  public void deleteShardMap(StoreShardMap shardMap) {
    try (WriteLockScope wls = cacheRoot.GetWriteLockScope()) {
      cacheRoot.Remove(shardMap);
      log.info("Cache delete complete. ShardMap: {}", shardMap.getName());
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  /**
   * Looks up a given shard map in cache based on it's name.
   *
   * @param shardMapName Name of shard map.
   * @return The shard being searched.
   */
  public StoreShardMap lookupShardMapByName(String shardMapName) {
    StoreShardMap shardMap = null;

    try (ReadLockScope rls = cacheRoot.GetReadLockScope(false)) {
      // Typical scenario will result in immediate lookup succeeding.
      shardMap = cacheRoot.LookupByName(shardMapName);
    } catch (IOException e) {
      e.printStackTrace();
    }

    log.info("Cache {}; ShardMap: {}", shardMap == null ? "miss" : "hit", shardMapName);

    return shardMap;
  }

  /**
   * Invoked for refreshing mapping in cache from store.
   *
   * @param mapping Storage representation of mapping.
   * @param policy Policy to use for preexisting cache entries during update.
   */
  public void addOrUpdateMapping(StoreMapping mapping, CacheStoreMappingUpdatePolicy policy) {
    try (ReadLockScope rls = cacheRoot.GetReadLockScope(false)) {
      CacheShardMap csm = cacheRoot.LookupById(mapping.getShardMapId());

      if (csm != null) {
        try (WriteLockScope wlscsm = csm.GetWriteLockScope()) {
          csm.getMapper().addOrUpdate(mapping, policy);

          // Update perf counters for add or update operation and mappings count.
          csm.IncrementPerformanceCounter(PerformanceCounterName.MappingsAddOrUpdatePerSec);
          csm.SetPerformanceCounter(PerformanceCounterName.MappingsCount,
              csm.getMapper().getMappingsCount());

          log.info("Cache Add/Update mapping complete. Mapping Id: {}", mapping.getId());
        } catch (IOException e) {
          e.printStackTrace();
        }
      }
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  /**
   * Invoked for deleting mapping in cache becase it no longer exists in store.
   *
   * @param mapping Storage representation of mapping.
   */
  public void deleteMapping(StoreMapping mapping) {
    try (ReadLockScope rls = cacheRoot.GetReadLockScope(false)) {
      CacheShardMap csm = cacheRoot.LookupById(mapping.getShardMapId());

      if (csm != null) {
        try (WriteLockScope wlscsm = csm.GetWriteLockScope()) {
          csm.getMapper().remove(mapping);

          // Update perf counters for remove mapping operation and mappings count.
          csm.IncrementPerformanceCounter(PerformanceCounterName.MappingsRemovePerSec);
          csm.SetPerformanceCounter(PerformanceCounterName.MappingsCount,
              csm.getMapper().getMappingsCount());

          log.info("Cache delete mapping complete. Mapping Id: {}", mapping.getId());
        } catch (IOException e) {
          e.printStackTrace();
        }
      }
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  /**
   * Looks up a given key in given shard map.
   *
   * @param shardMap Storage representation of shard map.
   * @param key Key value.
   * @return Mapping corresponding to <paramref name="key"/> or null.
   */
  public ICacheStoreMapping lookupMappingByKey(StoreShardMap shardMap, ShardKey key) {
    ICacheStoreMapping sm = null;

    try (ReadLockScope rls = cacheRoot.GetReadLockScope(false)) {
      CacheShardMap csm = cacheRoot.LookupById(shardMap.getId());

      if (csm != null) {
        try (ReadLockScope rlsShardMap = csm.GetReadLockScope(false)) {
          StoreMapping smDummy = null;
          ReferenceObjectHelper<StoreMapping> tempRef_smDummy = new ReferenceObjectHelper<StoreMapping>(
              smDummy);
          sm = csm.getMapper().lookupByKey(key, tempRef_smDummy);
          smDummy = tempRef_smDummy.argValue;

          // perf counter can not be updated in csm.Mapper.lookupByKey() as this function is also
          // called from csm.Mapper.addOrUpdate() so updating perf counter value here instead.
          csm.IncrementPerformanceCounter(
              sm == null ? PerformanceCounterName.MappingsLookupFailedPerSec
                  : PerformanceCounterName.MappingsLookupSucceededPerSec);
        }
      }
    } catch (IOException e) {
      e.printStackTrace();
    }

    return sm;
  }

  /**
   * Invoked for updating specified performance counter for a cached shard map object.
   *
   * @param shardMap Storage representation of a shard map.
   * @param name Performance counter to increment.
   */
  public final void incrementPerformanceCounter(StoreShardMap shardMap,
      PerformanceCounterName name) {
    try (ReadLockScope rls = cacheRoot.GetReadLockScope(false)) {
      CacheShardMap csm = cacheRoot.LookupById(shardMap.getId());

      if (csm != null) {
        try (ReadLockScope rlsShardMap = csm.GetReadLockScope(false)) {
          csm.IncrementPerformanceCounter(name);
        } catch (IOException e) {
          e.printStackTrace();
        }
      }
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  /**
   * Clears the cache.
   */
  public void clear() {
    try (WriteLockScope wls = cacheRoot.GetWriteLockScope()) {
      cacheRoot.Clear();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  ///#region IDisposable

  /**
   * Public dispose method.
   */
  public final void Dispose() {
    Dispose(true);
    //TODO: GC.SuppressFinalize(this);
  }

  /**
   * Protected vitual member of the dispose pattern.
   *
   * @param disposing Call came from Dispose.
   */
  protected void Dispose(boolean disposing) {
    if (disposing) {
      //TODO: cacheRoot.close();
    }
  }

  @Override
  public void close() throws IOException {

  }

  ///#endregion IDisposable
}
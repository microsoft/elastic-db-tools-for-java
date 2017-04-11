package com.microsoft.azure.elasticdb.shard.cache;

// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

import com.microsoft.azure.elasticdb.core.commons.helpers.ReferenceObjectHelper;
import com.microsoft.azure.elasticdb.shard.base.ShardKey;
import com.microsoft.azure.elasticdb.shard.store.IStoreMapping;
import com.microsoft.azure.elasticdb.shard.store.StoreShardMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.invoke.MethodHandles;

/**
 * Client side cache store.
 */
public class CacheStore implements ICacheStore {
    private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    /**
     * Root of the cache tree.
     */
    private CacheRoot _cacheRoot;

    /**
     * Constructs an instance of client side cache object.
     */
    public CacheStore() {
        _cacheRoot = new CacheRoot();
    }

    /**
     * Invoked for refreshing shard map in cache from store.
     *
     * @param shardMap Storage representation of shard map.
     */
    public void AddOrUpdateShardMap(StoreShardMap shardMap) {
        try (WriteLockScope wls = _cacheRoot.GetWriteLockScope()) {
            _cacheRoot.AddOrUpdate(shardMap);
            log.debug("Cache Add/Update complete. ShardMap: {}", shardMap.getName());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * Invoked for deleting shard map in cache becase it no longer exists in store.
     *
     * @param shardMap Storage representation of shard map.
     */
    public void DeleteShardMap(StoreShardMap shardMap) {
        try (WriteLockScope wls = _cacheRoot.GetWriteLockScope()) {
            _cacheRoot.Remove(shardMap);
            log.debug("Cache delete complete. ShardMap: {}", shardMap.getName());
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
    public StoreShardMap LookupShardMapByName(String shardMapName) {
        StoreShardMap shardMap = null;

        try (ReadLockScope rls = _cacheRoot.GetReadLockScope(false)) {
            // Typical scenario will result in immediate lookup succeeding.
            shardMap = _cacheRoot.LookupByName(shardMapName);
        } catch (IOException e) {
            e.printStackTrace();
        }

        log.debug("Cache {}; ShardMap: {}", shardMap == null ? "miss" : "hit", shardMapName);

        return shardMap;
    }

    /**
     * Invoked for refreshing mapping in cache from store.
     *
     * @param mapping Storage representation of mapping.
     * @param policy  Policy to use for preexisting cache entries during update.
     */
    public void AddOrUpdateMapping(IStoreMapping mapping, CacheStoreMappingUpdatePolicy policy) {
        try (ReadLockScope rls = _cacheRoot.GetReadLockScope(false)) {
            CacheShardMap csm = _cacheRoot.LookupById(mapping.getShardMapId());

            if (csm != null) {
                try (WriteLockScope wlscsm = csm.GetWriteLockScope()) {
                    csm.getMapper().AddOrUpdate(mapping, policy);

                    // Update perf counters for add or update operation and mappings count.
                    csm.IncrementPerformanceCounter(PerformanceCounterName.MappingsAddOrUpdatePerSec);
                    csm.SetPerformanceCounter(PerformanceCounterName.MappingsCount, csm.getMapper().GetMappingsCount());

                    log.debug("Cache Add/Update mapping complete. Mapping Id: {}", mapping.getId());
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
    public void DeleteMapping(IStoreMapping mapping) {
        try (ReadLockScope rls = _cacheRoot.GetReadLockScope(false)) {
            CacheShardMap csm = _cacheRoot.LookupById(mapping.getShardMapId());

            if (csm != null) {
                try (WriteLockScope wlscsm = csm.GetWriteLockScope()) {
                    csm.getMapper().Remove(mapping);

                    // Update perf counters for remove mapping operation and mappings count.
                    csm.IncrementPerformanceCounter(PerformanceCounterName.MappingsRemovePerSec);
                    csm.SetPerformanceCounter(PerformanceCounterName.MappingsCount, csm.getMapper().GetMappingsCount());

                    log.debug("Cache delete mapping complete. Mapping Id: {}", mapping.getId());
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
     * @param key      Key value.
     * @return Mapping corresponding to <paramref name="key"/> or null.
     */
    public ICacheStoreMapping LookupMappingByKey(StoreShardMap shardMap, ShardKey key) {
        ICacheStoreMapping sm = null;

        try (ReadLockScope rls = _cacheRoot.GetReadLockScope(false)) {
            CacheShardMap csm = _cacheRoot.LookupById(shardMap.getId());

            if (csm != null) {
                try (ReadLockScope rlsShardMap = csm.GetReadLockScope(false)) {
                    IStoreMapping smDummy = null;
                    ReferenceObjectHelper<IStoreMapping> tempRef_smDummy = new ReferenceObjectHelper<IStoreMapping>(smDummy);
                    sm = csm.getMapper().LookupByKey(key, tempRef_smDummy);
                    smDummy = tempRef_smDummy.argValue;

                    // perf counter can not be updated in csm.Mapper.LookupByKey() as this function is also called from csm.Mapper.AddOrUpdate()
                    // so updating perf counter value here instead.
                    csm.IncrementPerformanceCounter(sm == null ? PerformanceCounterName.MappingsLookupFailedPerSec : PerformanceCounterName.MappingsLookupSucceededPerSec);
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
     * @param name     Performance counter to increment.
     */
    public final void IncrementPerformanceCounter(StoreShardMap shardMap, PerformanceCounterName name) {
        try (ReadLockScope rls = _cacheRoot.GetReadLockScope(false)) {
            CacheShardMap csm = _cacheRoot.LookupById(shardMap.getId());

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
    public void Clear() {
        try (WriteLockScope wls = _cacheRoot.GetWriteLockScope()) {
            _cacheRoot.Clear();
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
            //TODO: _cacheRoot.close();
        }
    }

    @Override
    public void close() throws IOException {

    }

    ///#endregion IDisposable
}
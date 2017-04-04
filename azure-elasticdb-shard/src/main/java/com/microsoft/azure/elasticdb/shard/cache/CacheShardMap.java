package com.microsoft.azure.elasticdb.shard.cache;

// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

import com.microsoft.azure.elasticdb.shard.store.IStoreShardMap;

/**
 * Cached representation of shard map.
 */
public class CacheShardMap extends CacheObject {
    /**
     * Storage representation of shard map.
     */
    private IStoreShardMap StoreShardMap;
    /**
     * Mapper object. Exists only for List/Range/Hash shard maps.
     */
    private CacheMapper Mapper;
    /**
     * Performance counter instance for this shard map.
     */
    private PerfCounterInstance _perfCounters;

    /**
     * Constructs the cached shard map.
     *
     * @param ssm Storage representation of shard map.
     */
    public CacheShardMap(IStoreShardMap ssm) {
        super();
        this.setStoreShardMap(ssm);

        switch (ssm.getMapType()) {
            case List:
                this.setMapper(new CacheListMapper(ssm.getKeyType()));
                break;
            case Range:
                this.setMapper(new CacheRangeMapper(ssm.getKeyType()));
                break;
        }

        this._perfCounters = new PerfCounterInstance(ssm.getName());
    }

    public final IStoreShardMap getStoreShardMap() {
        return StoreShardMap;
    }

    public final void setStoreShardMap(IStoreShardMap value) {
        StoreShardMap = value;
    }

    public final CacheMapper getMapper() {
        return Mapper;
    }

    public final void setMapper(CacheMapper value) {
        Mapper = value;
    }

    /**
     * Transfers the child cache objects to current instance from the source instance.
     * Useful for mantaining the cache even in case of refreshes to shard map objects.
     *
     * @param source Source cached shard map to copy child objects from.
     */
    public final void TransferStateFrom(CacheShardMap source) {
        this.setMapper(source.getMapper());
    }

    /**
     * Increment value of performance counter by 1.
     *
     * @param name Name of performance counter to increment.
     */
    public final void IncrementPerformanceCounter(PerformanceCounterName name) {
        this._perfCounters.IncrementCounter(name);
    }

    /**
     * Set raw value of performance counter.
     *
     * @param name  Performance counter to update.
     * @param value Raw value for the counter.
     *              This method is always called from CacheStore inside csm.GetWriteLockScope() so we do not have to
     *              worry about multithreaded access here.
     */
    public final void SetPerformanceCounter(PerformanceCounterName name, long value) {
        this._perfCounters.SetCounter(name, value);
    }

    /**
     * Protected vitual member of the dispose pattern.
     *
     * @param disposing Call came from Dispose.
     */
    @Override
    protected void Dispose(boolean disposing) {
        //TODO: this._perfCounters.Dispose();
        super.Dispose(disposing);
    }

    protected void finalize() throws Throwable {
        Dispose(false);
    }
}
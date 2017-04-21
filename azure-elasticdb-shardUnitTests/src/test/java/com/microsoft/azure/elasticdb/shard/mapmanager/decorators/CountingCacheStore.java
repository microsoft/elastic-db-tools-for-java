package com.microsoft.azure.elasticdb.shard.mapmanager.decorators;

import com.microsoft.azure.elasticdb.shard.base.ShardKey;
import com.microsoft.azure.elasticdb.shard.cache.CacheStoreMappingUpdatePolicy;
import com.microsoft.azure.elasticdb.shard.cache.ICacheStore;
import com.microsoft.azure.elasticdb.shard.cache.ICacheStoreMapping;
import com.microsoft.azure.elasticdb.shard.store.StoreMapping;
import com.microsoft.azure.elasticdb.shard.store.StoreShardMap;

public class CountingCacheStore extends CacheStoreDecorator {
	private int AddShardMapCount;

	public void setAddShardMapCount(int AddShardMapCount) {
		this.AddShardMapCount = AddShardMapCount;
	}

	public int getAddShardMapCount() {
		return this.AddShardMapCount;
	}

	private int DeleteShardMapCount;

	public void setDeleteShardMapCount(int DeleteShardMapCount) {
		this.DeleteShardMapCount = DeleteShardMapCount;
	}

	public int getDeleteShardMapCount() {
		return this.DeleteShardMapCount;
	}

	private int LookupShardMapCount;

	public void setLookupShardMapCount(int LookupShardMapCount) {
		this.LookupShardMapCount = LookupShardMapCount;
	}

	public int getLookupShardMapCount() {
		return this.LookupShardMapCount;
	}

	private int LookupShardMapHitCount;

	public void setLookupShardMapHitCount(int LookupShardMapHitCount) {
		this.LookupShardMapHitCount = LookupShardMapHitCount;
	}

	public int getLookupShardMapHitCount() {
		return this.LookupShardMapHitCount;
	}

	private int LookupShardMapMissCount;

	public void setLookupShardMapMissCount(int LookupShardMapMissCount) {
		this.LookupShardMapMissCount = LookupShardMapMissCount;
	}

	public int getLookupShardMapMissCount() {
		return this.LookupShardMapMissCount;
	}

	private int AddMappingCount;

	public void setAddMappingCount(int AddMappingCount) {
		this.AddMappingCount = AddMappingCount;
	}

	public int getAddMappingCount() {
		return this.AddMappingCount;
	}

	private int DeleteMappingCount;

	public void setDeleteMappingCount(int DeleteMappingCount) {
		this.DeleteMappingCount = DeleteMappingCount;
	}

	public int getDeleteMappingCount() {
		return this.DeleteMappingCount;
	}

	private int LookupMappingCount;

	public void setLookupMappingCount(int LookupMappingCount) {
		this.LookupMappingCount = LookupMappingCount;
	}

	public int getLookupMappingCount() {
		return this.LookupMappingCount;
	}

	private int LookupMappingHitCount;

	public void setLookupMappingHitCount(int LookupMappingHitCount) {
		this.LookupMappingHitCount = LookupMappingHitCount;
	}

	public int getLookupMappingHitCount() {
		return this.LookupMappingHitCount;
	}

	private int LookupMappingMissCount;

	public void setLookupMappingMissCount(int LookupMappingMissCount) {
		this.LookupMappingMissCount = LookupMappingMissCount;
	}

	public int getLookupMappingMissCount() {
		return this.LookupMappingMissCount;
	}

	public CountingCacheStore(ICacheStore inner) {
		super(inner);
		this.ResetCounters();
	}

	private void ResetCounters() {
		this.setAddShardMapCount(0);
		this.setDeleteShardMapCount(0);
		this.setLookupShardMapCount(0);
		this.setLookupShardMapHitCount(0);
		this.setLookupShardMapMissCount(0);

		this.setAddMappingCount(0);
		this.setDeleteMappingCount(0);
		this.setLookupMappingCount(0);
		this.setLookupMappingHitCount(0);
		this.setLookupMappingMissCount(0);

	}

	@Override
	public void AddOrUpdateShardMap(StoreShardMap shardMap) {
		this.setAddMappingCount(this.getAddMappingCount() + 1);
		super.AddOrUpdateShardMap(shardMap);
	}

	@Override
	public void DeleteShardMap(StoreShardMap shardMap) {
		this.setDeleteShardMapCount(this.getDeleteShardMapCount() + 1);
		super.DeleteShardMap(shardMap);
	}

	@Override
	public StoreShardMap LookupShardMapByName(String shardMapName) {
		this.setLookupShardMapCount(this.getLookupShardMapCount() + 1);
		StoreShardMap result = super.LookupShardMapByName(shardMapName);
		if (result == null) {
			this.setLookupShardMapMissCount(this.getLookupShardMapMissCount() + 1);
		} else {
			this.setLookupShardMapHitCount(this.getLookupShardMapHitCount() + 1);
		}
		return result;

	}

	@Override
	public void AddOrUpdateMapping(StoreMapping mapping, CacheStoreMappingUpdatePolicy policy) {
		this.setAddMappingCount(this.getAddMappingCount() + 1);
		super.AddOrUpdateMapping(mapping, policy);
	}

	@Override
	public void DeleteMapping(StoreMapping mapping) {
		this.setDeleteMappingCount(this.getDeleteMappingCount() + 1);
		super.DeleteMapping(mapping);
	}

	@Override
	public ICacheStoreMapping LookupMappingByKey(StoreShardMap shardMap, ShardKey key) {
		this.setLookupMappingCount(this.getLookupMappingCount() + 1);
		ICacheStoreMapping result = super.LookupMappingByKey(shardMap, key);
		if (result == null) {
			this.setLookupMappingMissCount(this.getLookupMappingMissCount() + 1);
		} else {
			this.setLookupMappingHitCount(this.getLookupMappingHitCount() + 1);
		}
		return result;
	}

}

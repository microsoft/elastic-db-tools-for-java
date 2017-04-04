package com.microsoft.azure.elasticdb.shard.mapper;

// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

import com.microsoft.azure.elasticdb.shard.base.Shard;
import com.microsoft.azure.elasticdb.shard.base.ShardLocation;
import com.microsoft.azure.elasticdb.shard.store.IStoreResults;
import com.microsoft.azure.elasticdb.shard.storeops.base.IStoreOperationGlobal;

import java.util.List;
import java.util.UUID;

/**
 * Default shard mapper, that basically is a container of shards with no keys.
 */
public final class DefaultShardMapper extends BaseShardMapper implements IShardMapper<Shard, ShardLocation, Shard> {
    /**
     * Default shard mapper, which just manages Shards.
     *
     * @param manager Reference to ShardMapManager.
     * @param sm      Containing shard map.
     */
    public DefaultShardMapper(ShardMapManager manager, ShardMap sm) {
        super(manager, sm);
    }

    /**
     * Given a shard, obtains a SqlConnection to the shard. The shard must exist in the mapper.
     *
     * @param key              Input shard.
     * @param connectionString Connection string with credential information, the DataSource and Database are
     *                         obtained from the results of the lookup operation.
     * @param options          Options for validation operations to perform on opened connection.
     * @return An opened SqlConnection.
     */

    public SqlConnection OpenConnectionForKey(Shard key, String connectionString) {
        return OpenConnectionForKey(key, connectionString, ConnectionOptions.Validate);
    }

    public SqlConnection OpenConnectionForKey(Shard key, String connectionString, ConnectionOptions options) {
        assert key != null;
        assert connectionString != null;

        return this.ShardMap.OpenConnection(this.Lookup(key, true), connectionString, options);
    }

    /**
     * Given a shard, asynchronously obtains a SqlConnection to the shard. The shard must exist in the mapper.
     *
     * @param key              Input shard.
     * @param connectionString Connection string with credential information, the DataSource and Database are
     *                         obtained from the results of the lookup operation.
     * @param options          Options for validation operations to perform on opened connection.
     * @return An opened SqlConnection.
     */

    public Task<SqlConnection> OpenConnectionForKeyAsync(Shard key, String connectionString) {
        return OpenConnectionForKeyAsync(key, connectionString, ConnectionOptions.Validate);
    }

    public Task<SqlConnection> OpenConnectionForKeyAsync(Shard key, String connectionString, ConnectionOptions options) {
        assert key != null;
        assert connectionString != null;

        return await
        this.ShardMap.OpenConnectionAsync(this.Lookup(key, true), connectionString, options).ConfigureAwait(false);
    }

    /**
     * Adds a shard.
     *
     * @param shard Shard being added.
     * @return The added shard object.
     */
    public Shard Add(Shard shard) {
        assert shard != null;

        ExceptionUtils.EnsureShardBelongsToShardMap(this.Manager, this.ShardMap, shard, "CreateShard", "Shard");

        try (IStoreOperation op = this.Manager.StoreOperationFactory.CreateAddShardOperation(this.Manager, this.ShardMap.StoreShardMap, shard.StoreShard)) {
            op.Do();
        }

        return shard;
    }

    /**
     * Removes a shard.
     *
     * @param shard       Shard being removed.
     * @param lockOwnerId Lock owner id of this mapping
     */

    public void Remove(Shard shard) {
        Remove(shard, default (System.Guid));
    }

    public void Remove(Shard shard, UUID lockOwnerId) {
        assert shard != null;

        ExceptionUtils.EnsureShardBelongsToShardMap(this.Manager, this.ShardMap, shard, "DeleteShard", "Shard");

        try (IStoreOperation op = this.Manager.StoreOperationFactory.CreateRemoveShardOperation(this.Manager, this.ShardMap.StoreShardMap, shard.StoreShard)) {
            op.Do();
        }
    }

    /**
     * Looks up the given shard in the mapper.
     *
     * @param shard    Input shard.
     * @param useCache Whether to use cache for lookups.
     * @return Returns the shard after verifying that it is present in mapper.
     */
    public Shard Lookup(Shard shard, boolean useCache) {
        assert shard != null;

        return shard;
    }

    /**
     * Tries to looks up the key value and returns the corresponding mapping.
     *
     * @param key      Input shard.
     * @param useCache Whether to use cache for lookups.
     * @param shard    Shard that contains the key value.
     * @return <c>true</c> if shard is found, <c>false</c> otherwise.
     */
    public boolean TryLookup(Shard key, boolean useCache, ReferenceObjectHelper<Shard> shard) {
        assert key != null;

        shard.argValue = key;

        return true;
    }

    /**
     * Gets all shards for a shard map.
     *
     * @return All the shards belonging to the shard map.
     */
    public List<Shard> GetShards() {
        IStoreResults result;

        try (IStoreOperationGlobal op = this.Manager.StoreOperationFactory.CreateGetShardsGlobalOperation("GetShards", this.Manager, this.ShardMap.StoreShardMap)) {
            result = op.Do();
        }

        return result.StoreShards.Select(ss -> new Shard(this.Manager, this.ShardMap, ss));
    }

    /**
     * Gets shard object based on given location.
     *
     * @param location Input location.
     * @return Shard belonging to ShardMap.
     */
    public Shard GetShardByLocation(ShardLocation location) {
        assert location != null;

        IStoreResults result;

        try (IStoreOperationGlobal op = this.getManager().getStoreOperationFactory().CreateFindShardByLocationGlobalOperation(this.getManager(), "GetShardByLocation", this.getShardMap().getStoreShardMap(), location)) {
            result = op.Do();
        }

        return result.getStoreShards().Select(ss -> new Shard(this.Manager, this.ShardMap, ss)).SingleOrDefault();
    }

    /**
     * Allows for update to a shard with the updates provided in the <paramref name="update"/> parameter.
     *
     * @param currentShard Shard to be updated.
     * @param update       Updated properties of the Shard.
     * @return New Shard instance with updated information.
     */
    public Shard UpdateShard(Shard currentShard, ShardUpdate update) {
        assert currentShard != null;
        assert update != null;

        ExceptionUtils.EnsureShardBelongsToShardMap(this.Manager, this.ShardMap, currentShard, "UpdateShard", "Shard");

        // CONSIDER(wbasheer): Have refresh semantics for trivial case when nothing is modified.
        if (!update.IsAnyPropertySet(ShardUpdatedProperties.All)) {
            return currentShard;
        }

        DefaultStoreShard sNew = new DefaultStoreShard(currentShard.Id, UUID.randomUUID(), currentShard.ShardMapId, currentShard.Location, update.IsAnyPropertySet(ShardUpdatedProperties.Status) ? (int) update.Status : currentShard.StoreShard.Status);

        try (IStoreOperation op = this.Manager.StoreOperationFactory.CreateUpdateShardOperation(this.Manager, this.ShardMap.StoreShardMap, currentShard.StoreShard, sNew)) {
            op.Do();
        }

        return new Shard(this.Manager, this.ShardMap, sNew);
    }
}
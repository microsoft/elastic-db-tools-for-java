package com.microsoft.azure.elasticdb.shard.mapper;

// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import com.microsoft.azure.elasticdb.core.commons.helpers.ReferenceObjectHelper;
import com.microsoft.azure.elasticdb.shard.base.Shard;
import com.microsoft.azure.elasticdb.shard.base.ShardLocation;
import com.microsoft.azure.elasticdb.shard.base.ShardUpdate;
import com.microsoft.azure.elasticdb.shard.base.ShardUpdatedProperties;
import com.microsoft.azure.elasticdb.shard.map.ShardMap;
import com.microsoft.azure.elasticdb.shard.mapmanager.ShardMapManager;
import com.microsoft.azure.elasticdb.shard.store.StoreResults;
import com.microsoft.azure.elasticdb.shard.store.StoreShard;
import com.microsoft.azure.elasticdb.shard.storeops.base.IStoreOperation;
import com.microsoft.azure.elasticdb.shard.storeops.base.IStoreOperationGlobal;
import com.microsoft.azure.elasticdb.shard.utils.ExceptionUtils;
import com.microsoft.sqlserver.jdbc.SQLServerConnection;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.stream.Collectors;

/**
 * Default shard mapper, that basically is a container of shards with no keys.
 */
public final class DefaultShardMapper extends BaseShardMapper implements IShardMapper2<Shard, ShardLocation, Shard> {
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
     * @return An opened SqlConnection.
     */
    public SQLServerConnection OpenConnectionForKey(Shard key, String connectionString) {
        return OpenConnectionForKey(key, connectionString, ConnectionOptions.Validate);
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
    public SQLServerConnection OpenConnectionForKey(Shard key, String connectionString, ConnectionOptions options) {
        Preconditions.checkNotNull(key);
        Preconditions.checkNotNull(connectionString);

        return shardMap.OpenConnection(this.Lookup(key, true), connectionString, options);
    }

    /**
     * Given a shard, asynchronously obtains a SqlConnection to the shard. The shard must exist in the mapper.
     *
     * @param key              Input shard.
     * @param connectionString Connection string with credential information, the DataSource and Database are
     *                         obtained from the results of the lookup operation.
     * @return An opened SqlConnection.
     */
    public Callable<SQLServerConnection> OpenConnectionForKeyAsync(Shard key, String connectionString) {
        return OpenConnectionForKeyAsync(key, connectionString, ConnectionOptions.Validate);
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
    public Callable<SQLServerConnection> OpenConnectionForKeyAsync(Shard key, String connectionString, ConnectionOptions options) {
        Preconditions.checkNotNull(key);
        Preconditions.checkNotNull(connectionString);
        return shardMap.OpenConnectionAsync(this.Lookup(key, true), connectionString, options);
    }

    /**
     * Adds a shard.
     *
     * @param shard Shard being added.
     * @return The added shard object.
     */
    public Shard Add(Shard shard) {
        assert shard != null;

        ExceptionUtils.EnsureShardBelongsToShardMap(this.shardMapManager, shardMap, shard, "CreateShard", "Shard");

        try (IStoreOperation op = this.shardMapManager.getStoreOperationFactory().CreateAddShardOperation(this.shardMapManager, shardMap.getStoreShardMap(), shard.getStoreShard())) {
            op.Do();
            return shard;
        } catch (Exception e) {
            e.printStackTrace();
            return null; //TODO
        }
    }

    /**
     * Removes a shard.
     *
     * @param shard Shard being removed.
     */
    public void Remove(Shard shard) {
        Remove(shard, new UUID(0L, 0L));
    }

    /**
     * Removes a shard.
     *
     * @param shard       Shard being removed.
     * @param lockOwnerId Lock owner id of this mapping
     */
    public void Remove(Shard shard, UUID lockOwnerId) {
        assert shard != null;

        ExceptionUtils.EnsureShardBelongsToShardMap(this.shardMapManager, shardMap, shard, "DeleteShard", "Shard");

        try (IStoreOperation op = this.shardMapManager.getStoreOperationFactory().CreateRemoveShardOperation(this.shardMapManager, shardMap.getStoreShardMap(), shard.getStoreShard())) {
            op.Do();
        } catch (Exception e) {
            e.printStackTrace();
            //TODO
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
        StoreResults result;

        try (IStoreOperationGlobal op = shardMapManager.getStoreOperationFactory().CreateGetShardsGlobalOperation("GetShards", this.shardMapManager, shardMap.getStoreShardMap())) {
            result = op.Do();
        } catch (Exception e) {
            e.printStackTrace();
            return null; //TODO
        }

        return result.getStoreShards().stream().map(ss -> new Shard(shardMapManager, shardMap, ss)).collect(Collectors.toList());
    }

    /**
     * Gets shard object based on given location.
     *
     * @param location Input location.
     * @return Shard belonging to ShardMap.
     */
    public Shard GetShardByLocation(ShardLocation location) {
        assert location != null;

        StoreResults result;

        try (IStoreOperationGlobal op = this.getShardMapManager().getStoreOperationFactory().CreateFindShardByLocationGlobalOperation(this.getShardMapManager(), "GetShardByLocation", this.getShardMap().getStoreShardMap(), location)) {
            result = op.Do();
        } catch (Exception e) {
            e.printStackTrace();
            return null; //TODO
        }
        StoreShard onlyElement = Iterables.getOnlyElement(result.getStoreShards());
        return new Shard(shardMapManager, shardMap, onlyElement);
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

        ExceptionUtils.EnsureShardBelongsToShardMap(this.shardMapManager, shardMap, currentShard, "UpdateShard", "Shard");

        // CONSIDER(wbasheer): Have refresh semantics for trivial case when nothing is modified.
        if (!update.IsAnyPropertySet(ShardUpdatedProperties.All)) {
            return currentShard;
        }

        StoreShard sNew = new StoreShard(currentShard.getId(), UUID.randomUUID(), currentShard.getShardMapId(), currentShard.getLocation(), update.IsAnyPropertySet(ShardUpdatedProperties.Status) ? update.getStatus().getValue() : currentShard.getStoreShard().getStatus());

        try (IStoreOperation op = this.shardMapManager.getStoreOperationFactory().CreateUpdateShardOperation(this.shardMapManager, shardMap.getStoreShardMap(), currentShard.getStoreShard(), sNew)) {
            op.Do();
        } catch (Exception e) {
            e.printStackTrace();
        }

        return new Shard(this.shardMapManager, shardMap, sNew);
    }
}
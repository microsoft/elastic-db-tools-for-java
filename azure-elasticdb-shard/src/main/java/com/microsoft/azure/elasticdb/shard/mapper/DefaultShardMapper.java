package com.microsoft.azure.elasticdb.shard.mapper;

/* Copyright (c) Microsoft. All rights reserved.
Licensed under the MIT license. See LICENSE file in the project root for full license information.*/

import com.google.common.base.Preconditions;
import com.microsoft.azure.elasticdb.core.commons.helpers.ReferenceObjectHelper;
import com.microsoft.azure.elasticdb.shard.base.Shard;
import com.microsoft.azure.elasticdb.shard.base.ShardLocation;
import com.microsoft.azure.elasticdb.shard.base.ShardUpdate;
import com.microsoft.azure.elasticdb.shard.base.ShardUpdatedProperties;
import com.microsoft.azure.elasticdb.shard.map.ShardMap;
import com.microsoft.azure.elasticdb.shard.mapmanager.ShardManagementException;
import com.microsoft.azure.elasticdb.shard.mapmanager.ShardMapManager;
import com.microsoft.azure.elasticdb.shard.store.StoreResults;
import com.microsoft.azure.elasticdb.shard.store.StoreShard;
import com.microsoft.azure.elasticdb.shard.storeops.base.IStoreOperation;
import com.microsoft.azure.elasticdb.shard.storeops.base.IStoreOperationGlobal;
import com.microsoft.azure.elasticdb.shard.utils.ExceptionUtils;
import java.sql.Connection;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.stream.Collectors;

/**
 * Default shard mapper, that basically is a container of shards with no keys.
 */
public final class DefaultShardMapper extends BaseShardMapper implements
    IShardMapper<Shard, Shard> {

  /**
   * Default shard mapper, which just manages Shards.
   *
   * @param shardMapManager Reference to ShardMapManager.
   * @param sm Containing shard map.
   */
  public DefaultShardMapper(ShardMapManager shardMapManager, ShardMap sm) {
    super(shardMapManager, sm);
  }

  /**
   * Given a shard, obtains a SqlConnection to the shard. The shard must exist in the mapper.
   *
   * @param key Input shard.
   * @param connectionString Connection string with credential information, the DataSource and
   * Database are obtained from the results of the lookup operation.
   * @return An opened SqlConnection.
   */
  public Connection openConnectionForKey(Shard key, String connectionString) {
    return openConnectionForKey(key, connectionString, ConnectionOptions.Validate);
  }

  /**
   * Given a shard, obtains a SqlConnection to the shard. The shard must exist in the mapper.
   *
   * @param key Input shard.
   * @param connectionString Connection string with credential information, the DataSource and
   * Database are obtained from the results of the lookup operation.
   * @param options Options for validation operations to perform on opened connection.
   * @return An opened SqlConnection.
   */
  public Connection openConnectionForKey(Shard key, String connectionString,
      ConnectionOptions options) {
    Preconditions.checkNotNull(key);
    Preconditions.checkNotNull(connectionString);

    return shardMap.openConnection(this.lookup(key, true), connectionString, options);
  }

  /**
   * Given a shard, asynchronously obtains a SqlConnection to the shard. The shard must exist in the
   * mapper.
   *
   * @param key Input shard.
   * @param connectionString Connection string with credential information, the DataSource and
   * Database are obtained from the results of the lookup operation.
   * @return An opened SqlConnection.
   */
  public Callable<Connection> openConnectionForKeyAsync(Shard key,
      String connectionString) {
    return openConnectionForKeyAsync(key, connectionString, ConnectionOptions.Validate);
  }

  /**
   * Given a shard, asynchronously obtains a SqlConnection to the shard. The shard must exist in the
   * mapper.
   *
   * @param key Input shard.
   * @param connectionString Connection string with credential information, the DataSource and
   * Database are obtained from the results of the lookup operation.
   * @param options Options for validation operations to perform on opened connection.
   * @return An opened SqlConnection.
   */
  public Callable<Connection> openConnectionForKeyAsync(Shard key, String connectionString,
      ConnectionOptions options) {
    Preconditions.checkNotNull(key);
    Preconditions.checkNotNull(connectionString);
    return shardMap.openConnectionAsync(this.lookup(key, true), connectionString, options);
  }

  /**
   * Adds a shard.
   *
   * @param shard Shard being added.
   * @return The added shard object.
   */
  public Shard add(Shard shard) {
    assert shard != null;

    ExceptionUtils.ensureShardBelongsToShardMap(this.shardMapManager, shardMap, shard,
        "CreateShard", "Shard");

    try (IStoreOperation op = this.shardMapManager.getStoreOperationFactory()
        .createAddShardOperation(this.shardMapManager, shardMap.getStoreShardMap(),
            shard.getStoreShard())) {
      op.doOperation();
      return shard;
    } catch (Exception e) {
      e.printStackTrace();
      throw (ShardManagementException) e.getCause();
    }
  }

  /**
   * Removes a shard.
   *
   * @param shard Shard being removed.
   */
  public void remove(Shard shard) {
    remove(shard, new UUID(0L, 0L));
  }

  /**
   * Removes a shard.
   *
   * @param shard Shard being removed.
   * @param lockOwnerId Lock owner id of this mapping
   */
  public void remove(Shard shard, UUID lockOwnerId) {
    assert shard != null;

    ExceptionUtils.ensureShardBelongsToShardMap(this.shardMapManager, shardMap, shard,
        "DeleteShard", "Shard");

    try (IStoreOperation op = this.shardMapManager.getStoreOperationFactory()
        .createRemoveShardOperation(this.shardMapManager, shardMap.getStoreShardMap(),
            shard.getStoreShard())) {
      op.doOperation();
    } catch (Exception e) {
      e.printStackTrace();
      throw (ShardManagementException) e.getCause();
    }
  }

  /**
   * Looks up the given shard in the mapper.
   *
   * @param shard Input shard.
   * @param useCache Whether to use cache for lookups.
   * @return Returns the shard after verifying that it is present in mapper.
   */
  public Shard lookup(Shard shard, boolean useCache) {
    assert shard != null;

    return shard;
  }

  /**
   * Tries to looks up the key value and returns the corresponding mapping.
   *
   * @param key Input shard.
   * @param useCache Whether to use cache for lookups.
   * @param shard Shard that contains the key value.
   * @return <c>true</c> if shard is found, <c>false</c> otherwise.
   */
  public boolean tryLookup(Shard key, boolean useCache, ReferenceObjectHelper<Shard> shard) {
    assert key != null;

    shard.argValue = key;

    return true;
  }

  /**
   * Gets all shards for a shard map.
   *
   * @return All the shards belonging to the shard map.
   */
  public List<Shard> getShards() {
    StoreResults result;

    try (IStoreOperationGlobal op = shardMapManager.getStoreOperationFactory()
        .createGetShardsGlobalOperation("GetShards", this.shardMapManager,
            shardMap.getStoreShardMap())) {
      result = op.doGlobal();
    } catch (Exception e) {
      e.printStackTrace();
      throw (ShardManagementException) e.getCause();
    }

    return result.getStoreShards().stream().map(ss -> new Shard(shardMapManager, shardMap, ss))
        .collect(Collectors.toList());
  }

  /**
   * Gets shard object based on given location.
   *
   * @param location Input location.
   * @return Shard belonging to ShardMap.
   */
  public Shard getShardByLocation(ShardLocation location) {
    assert location != null;

    StoreResults result;

    try (IStoreOperationGlobal op = this.getShardMapManager().getStoreOperationFactory()
        .createFindShardByLocationGlobalOperation(this.getShardMapManager(), "GetShardByLocation",
            this.getShardMap().getStoreShardMap(), location)) {
      result = op.doGlobal();
    } catch (Exception e) {
      e.printStackTrace();
      throw (ShardManagementException) e.getCause();
    }
    StoreShard onlyElement = result.getStoreShards().stream().findFirst().orElse(null);
    return onlyElement == null ? null : new Shard(shardMapManager, shardMap, onlyElement);
  }

  /**
   * Allows for update to a shard with the updates provided in the <paramref name="update"/>
   * parameter.
   *
   * @param currentShard Shard to be updated.
   * @param update Updated properties of the Shard.
   * @return New Shard instance with updated information.
   */
  public Shard updateShard(Shard currentShard, ShardUpdate update) {
    assert currentShard != null;
    assert update != null;

    ExceptionUtils.ensureShardBelongsToShardMap(this.shardMapManager, shardMap, currentShard,
        "UpdateShard", "Shard");

    // CONSIDER(wbasheer): Have refresh semantics for trivial case when nothing is modified.
    if (!update.isAnyPropertySet(ShardUpdatedProperties.All)) {
      return currentShard;
    }

    StoreShard ssNew = new StoreShard(currentShard.getId(), UUID.randomUUID(),
        currentShard.getShardMapId(), currentShard.getLocation(),
        update.isAnyPropertySet(ShardUpdatedProperties.Status) ? update.getStatus().getValue()
            : currentShard.getStoreShard().getStatus());

    try (IStoreOperation op = this.shardMapManager.getStoreOperationFactory()
        .createUpdateShardOperation(this.shardMapManager, shardMap.getStoreShardMap(),
            currentShard.getStoreShard(), ssNew)) {
      op.doOperation();
    } catch (Exception e) {
      e.printStackTrace();
      throw (ShardManagementException) e.getCause();
    }

    return new Shard(this.shardMapManager, shardMap, ssNew);
  }
}
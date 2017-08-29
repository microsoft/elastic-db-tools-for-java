package com.microsoft.azure.elasticdb.shard.schema;

/* Copyright (c) Microsoft. All rights reserved.
Licensed under the MIT license. See LICENSE file in the project root for full license information.*/

import com.microsoft.azure.elasticdb.core.commons.helpers.ReferenceObjectHelper;
import com.microsoft.azure.elasticdb.shard.mapmanager.ShardMapManager;
import com.microsoft.azure.elasticdb.shard.store.StoreResult;
import com.microsoft.azure.elasticdb.shard.store.StoreResults;
import com.microsoft.azure.elasticdb.shard.store.StoreSchemaInfo;
import com.microsoft.azure.elasticdb.shard.storeops.base.IStoreOperationGlobal;
import com.microsoft.azure.elasticdb.shard.utils.Errors;
import com.microsoft.azure.elasticdb.shard.utils.ExceptionUtils;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * Provides storage services to a client for storing or updating or retrieving schema information
 * associated with a sharding scheme and assigning names to individual buckets of information. The
 * class doesn't store the association between a sharding scheme and the metadata unit. It's the
 * caller's responsibility to maintain the mapping.
 */
public class SchemaInfoCollection extends ArrayList<Map.Entry<String, SchemaInfo>> {

  /**
   * Shard map manager object.
   */
  private ShardMapManager shardMapManager;

  /**
   * Constructs an instance of schema info collection.
   *
   * @param shardMapManager Shard map manager object.
   */
  public SchemaInfoCollection(ShardMapManager shardMapManager) {
    this.setShardMapManager(shardMapManager);
  }

  private ShardMapManager getShardMapManager() {
    return shardMapManager;
  }

  private void setShardMapManager(ShardMapManager value) {
    shardMapManager = value;
  }

  /**
   * Adds a <see cref="SchemaInfo"/> that is associated with the given <see cref="ShardMap"/> name.
   * The associated data contains information concerning sharded tables and
   * reference tables. If you try to add a <see cref="SchemaInfo"/> with an existing name,
   * a name-conflict exception will be thrown
   *
   * @param shardMapName The name of the <see cref="ShardMap"/> that the <see cref="SchemaInfo"/>
   * will be associated with
   * @param schemaInfo Sharding schema information.
   */
  public final void add(String shardMapName, SchemaInfo schemaInfo) {
    ExceptionUtils.disallowNullOrEmptyStringArgument(shardMapName, "shardMapName");
    ExceptionUtils.disallowNullArgument(schemaInfo, "schemaInfo");

    StoreSchemaInfo dssi = new StoreSchemaInfo(shardMapName, schemaInfo);

    try (IStoreOperationGlobal op = this.getShardMapManager().getStoreOperationFactory()
        .createAddShardingSchemaInfoGlobalOperation(this.getShardMapManager(), "Add", dssi)) {
      op.doGlobal();
    } catch (Exception e) {
      ExceptionUtils.throwStronglyTypedException(e);
    }
  }

  /**
   * Replaces the <see cref="SchemaInfo"/> with the given <see cref="ShardMap"/> name.
   *
   * @param shardMapName The name of the <see cref="ShardMap"/> whose <see cref="SchemaInfo"/> will
   * be replaced.
   * @param schemaInfo Sharding schema information.
   */
  public final void replace(String shardMapName, SchemaInfo schemaInfo) {
    ExceptionUtils.disallowNullOrEmptyStringArgument(shardMapName, "shardMapName");
    ExceptionUtils.disallowNullArgument(schemaInfo, "schemaInfo");

    StoreSchemaInfo dssi = new StoreSchemaInfo(shardMapName, schemaInfo);

    try (IStoreOperationGlobal op = this.getShardMapManager().getStoreOperationFactory()
        .createUpdateShardingSchemaInfoGlobalOperation(this.getShardMapManager(), "Replace",
            dssi)) {
      op.doGlobal();
    } catch (Exception e) {
      ExceptionUtils.throwStronglyTypedException(e);
    }
  }

  /**
   * Tries to fetch the <see cref="SchemaInfo"/> with the given <see cref="ShardMap"/> name without
   * raising any exception if data doesn't exist.
   *
   * @param shardMapName The name of the <see cref="ShardMap"/> whose <see cref="SchemaInfo"/> will
   * be fetched
   * @param schemaInfo The <see cref="SchemaInfo"/> that was fetched or null if retrieval failed
   * @return true if schema info exists with given name, false otherwise.
   */
  public final boolean tryGet(String shardMapName, ReferenceObjectHelper<SchemaInfo> schemaInfo) {
    ExceptionUtils.disallowNullOrEmptyStringArgument(shardMapName, "shardMapName");

    schemaInfo.argValue = null;

    StoreResults result = null;

    try (IStoreOperationGlobal op = this.getShardMapManager().getStoreOperationFactory()
        .createFindShardingSchemaInfoGlobalOperation(this.getShardMapManager(), "TryGet",
            shardMapName)) {
      result = op.doGlobal();
    } catch (Exception e) {
      ExceptionUtils.throwStronglyTypedException(e);
    }

    if (result.getResult() == StoreResult.SchemaInfoNameDoesNotExist) {
      return false;
    }

    schemaInfo.argValue = result.getStoreSchemaInfoCollection().stream()
        .map(StoreSchemaInfo::getShardingSchemaInfo).findFirst().orElse(new SchemaInfo());

    return true;
  }

  /**
   * Fetches the <see cref="SchemaInfo"/> stored with the supplied <see cref="ShardMap"/> name.
   *
   * @param shardMapName The name of the <see cref="ShardMap"/> to get.
   * @return SchemaInfo object.
   */
  public final SchemaInfo get(String shardMapName) {
    ExceptionUtils.disallowNullOrEmptyStringArgument(shardMapName, "shardMapName");

    StoreResults result = null;

    try (IStoreOperationGlobal op = this.getShardMapManager().getStoreOperationFactory()
        .createFindShardingSchemaInfoGlobalOperation(this.getShardMapManager(), "Get",
            shardMapName)) {
      result = op.doGlobal();
    } catch (Exception e) {
      ExceptionUtils.throwStronglyTypedException(e);
    }

    if (result.getResult() == StoreResult.SchemaInfoNameDoesNotExist) {
      throw new SchemaInfoException(SchemaInfoErrorCode.SchemaInfoNameDoesNotExist,
          Errors._Store_SchemaInfo_NameDoesNotExist, "Get", shardMapName);
    }

    return result.getStoreSchemaInfoCollection().stream()
        .map(StoreSchemaInfo::getShardingSchemaInfo).findFirst().orElse(null);
  }

  /**
   * Removes the <see cref="SchemaInfo"/> with the given <see cref="ShardMap"/> name.
   *
   * @param shardMapName The name of the <see cref="ShardMap"/> whose <see cref="SchemaInfo"/> will
   * be removed
   */
  public final void remove(String shardMapName) {
    ExceptionUtils.disallowNullOrEmptyStringArgument(shardMapName, "shardMapName");

    try (IStoreOperationGlobal op = this.getShardMapManager().getStoreOperationFactory()
        .createRemoveShardingSchemaInfoGlobalOperation(this.getShardMapManager(), "Remove",
            shardMapName)) {
      op.doGlobal();
    } catch (Exception e) {
      ExceptionUtils.throwStronglyTypedException(e);
    }
  }

  /**
   * Returns an enumerator that iterates through the <see cref="SchemaInfoCollection"/>.
   *
   * @return Enumerator of key-value pairs of name and <see cref="SchemaInfo"/> objects.
   */
  public final Iterator<Map.Entry<String, SchemaInfo>> iterator() {
    StoreResults result = null;

    try (IStoreOperationGlobal op = this.getShardMapManager().getStoreOperationFactory()
        .createGetShardingSchemaInfosGlobalOperation(this.getShardMapManager(), "GetEnumerator")) {
      result = op.doGlobal();
    } catch (Exception e) {
      ExceptionUtils.throwStronglyTypedException(e);
    }

    HashMap<String, SchemaInfo> mdCollection = new HashMap<>();

    for (StoreSchemaInfo ssi : result.getStoreSchemaInfoCollection()) {
      mdCollection.put(ssi.getName(), ssi.getShardingSchemaInfo());
    }

    return mdCollection.entrySet().iterator();
  }
}
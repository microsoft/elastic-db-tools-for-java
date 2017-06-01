package com.microsoft.azure.elasticdb.shard.base;

/* Copyright (c) Microsoft. All rights reserved.
Licensed under the MIT license. See LICENSE file in the project root for full license information.*/

import com.google.common.base.Stopwatch;
import com.microsoft.azure.elasticdb.shard.map.ShardMap;
import com.microsoft.azure.elasticdb.shard.mapmanager.ShardManagementException;
import com.microsoft.azure.elasticdb.shard.mapmanager.ShardMapManager;
import com.microsoft.azure.elasticdb.shard.store.StoreMapping;
import com.microsoft.azure.elasticdb.shard.store.StoreShardMap;
import com.microsoft.azure.elasticdb.shard.utils.StringUtilsLocal;
import java.lang.invoke.MethodHandles;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Represents a mapping between the singleton key value of a shardlet (a point) and a <see
 * cref="Shard"/>. <typeparam name="KeyT">Type of the key (point).</typeparam>
 */
public final class PointMapping implements IShardProvider<Object>, Cloneable, IMappingInfoProvider {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  /**
   * Shard object associated with the mapping.
   */
  private Shard shard;

  /**
   * Gets key value.
   */
  private Object value;

  /**
   * Holder of the key value's binary representation.
   */
  private ShardKey key;

  /**
   * Reference to the ShardMapManager.
   */
  private ShardMapManager shardMapManager;

  /**
   * Storage representation of the mapping.
   */
  private StoreMapping storeMapping;

  /**
   * Constructs a point mapping given mapping creation arguments.
   *
   * @param shardMapManager Owning ShardMapManager.
   * @param creationInfo Mapping creation information.
   */
  public PointMapping(ShardMapManager shardMapManager, PointMappingCreationInfo creationInfo) {
    assert shardMapManager != null;
    assert creationInfo != null;
    assert creationInfo.getShard() != null;

    this.setManager(shardMapManager);

    shard = creationInfo.getShard();

    this.setStoreMapping(new StoreMapping(UUID.randomUUID(), creationInfo.getShard(),
        creationInfo.getKey().getRawValue(), null, creationInfo.getStatus().getValue()));

    this.setKey(creationInfo.getKey());
    this.setValue(creationInfo.getValue());
  }

  /**
   * Internal constructor used for deserialization from store representation of
   * the mapping object.
   *
   * @param shardMapManager Owning ShardMapManager.
   * @param shardMap Owning shard map.
   * @param mapping Storage representation of the mapping.
   */
  public PointMapping(ShardMapManager shardMapManager, ShardMap shardMap, StoreMapping mapping) {
    assert shardMapManager != null;
    assert mapping != null;
    assert mapping.getShardMapId() != null;
    assert mapping.getStoreShard().getShardMapId() != null;

    this.setManager(shardMapManager);
    this.setStoreMapping(mapping);

    shard = new Shard(this.getShardMapManager(), shardMap, mapping.getStoreShard());
    this.setKey(ShardKey.fromRawValue(shardMap.getKeyType(), mapping.getMinValue()));
    this.setValue(this.getKey().getValue());
  }

  /**
   * Gets Status of the mapping.
   */
  public MappingStatus getStatus() {
    if (this.getStoreMapping().getStatus() == MappingStatus.Online.getValue()) {
      return MappingStatus.Online;
    }
    return MappingStatus.Offline;
  }

  /**
   * Gets Shard that contains the key value.
   */
  public Shard getShard() {
    return shard;
  }

  public Object getValue() {
    return value;
  }

  private void setValue(Object value) {
    this.value = value;
  }

  public ShardKey getKey() {
    return key;
  }

  public void setKey(ShardKey value) {
    key = value;
  }

  /**
   * Identity of the mapping.
   */
  public UUID getId() {
    return this.getStoreMapping().getId();
  }

  /**
   * Identify of the ShardMap this shard belongs to.
   */
  public UUID getShardMapId() {
    return this.getStoreMapping().getShardMapId();
  }

  public ShardMapManager getShardMapManager() {
    return shardMapManager;
  }

  public void setManager(ShardMapManager value) {
    shardMapManager = value;
  }

  public StoreMapping getStoreMapping() {
    return storeMapping;
  }

  private void setStoreMapping(StoreMapping value) {
    storeMapping = value;
  }

  /**
   * Converts the object to its string representation.
   *
   * @return String representation of the object.
   */
  @Override
  public String toString() {
    return StringUtilsLocal.formatInvariant("P[%s:%s]", this.getId().toString(),
        this.getKey().toString());
  }

  /**
   * Determines whether the specified object is equal to the current object.
   *
   * @param obj The object to compare with the current object.
   * @return True if the specified object is equal to the current object; otherwise, false.
   */
  @Override
  public boolean equals(Object obj) {
    if (obj == null || !(obj instanceof PointMapping)) {
      return false;
    }
    PointMapping other = (PointMapping) obj;
    return this.getId().equals(other.getId()) && this.getKey().equals(other.getKey());
  }

  /**
   * Calculates the hash code for this instance.
   *
   * @return Hash code for the object.
   */
  @Override
  public int hashCode() {
    return this.getId().hashCode();
  }

  /**
   * Shard that contains the key value.
   */
  public Shard getShardInfo() {
    return this.getShard();
  }

  /**
   * Performs validation that the local representation is as up-to-date
   * as the representation on the backing data store.
   *
   * @param shardMap Shard map to which the shard provider belongs.
   * @param conn Connection used for validation.
   */
  @Override
  public void validate(StoreShardMap shardMap, Connection conn) {
    try {
      log.info("PointMapping Validate Start; Connection: {}", conn.getMetaData().getURL());
      Stopwatch stopwatch = Stopwatch.createStarted();

      ValidationUtils.validateMapping(conn, this.getShardMapManager(), shardMap,
          this.getStoreMapping());

      stopwatch.stop();

      log.info("PointMapping Validate Complete; Connection: {}; Duration:{}",
          conn.getMetaData().getURL(), stopwatch.elapsed(TimeUnit.MILLISECONDS));
    } catch (SQLException e) {
      e.printStackTrace();
      throw (ShardManagementException) e.getCause();
    }
  }

  /**
   * Asynchronously performs validation that the local representation is as
   * up-to-date as the representation on the backing data store.
   *
   * @param shardMap Shard map to which the shard provider belongs.
   * @param conn Connection used for validation.
   * @return A task to await validation completion
   */
  @Override
  public Callable validateAsync(StoreShardMap shardMap, Connection conn) {
    Callable returnVal;
    try {
      log.info("PointMapping ValidateAsync Start; Connection: {}", conn.getMetaData().getURL());

      Stopwatch stopwatch = Stopwatch.createStarted();

      returnVal = ValidationUtils.validateMappingAsync(conn, this.getShardMapManager(),
          shardMap, this.getStoreMapping());

      stopwatch.stop();

      log.info("PointMapping ValidateAsync Complete; Connection: {} Duration:{}",
          conn.getMetaData().getURL(), stopwatch.elapsed(TimeUnit.MILLISECONDS));
    } catch (SQLException e) {
      e.printStackTrace();
      throw (ShardManagementException) e.getCause();
    }
    return returnVal;
  }

  /**
   * Clones the instance.
   *
   * @return clone of the instance.
   */
  public PointMapping clone() {
    return new PointMapping(this.getShardMapManager(), this.getShard().getShardMap(),
        this.getStoreMapping());
  }

  /**
   * Type of the mapping.
   */
  public MappingKind getKind() {
    return MappingKind.PointMapping;
  }

  /**
   * Mapping type, useful for diagnostics.
   */
  public String getTypeName() {
    return "PointMapping";
  }
}
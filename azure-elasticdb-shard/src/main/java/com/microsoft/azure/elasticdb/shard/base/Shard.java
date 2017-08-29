package com.microsoft.azure.elasticdb.shard.base;

/* Copyright (c) Microsoft. All rights reserved.
Licensed under the MIT license. See LICENSE file in the project root for full license information.*/

import com.google.common.base.Stopwatch;
import com.microsoft.azure.elasticdb.core.commons.logging.ActivityIdScope;
import com.microsoft.azure.elasticdb.shard.map.ShardMap;
import com.microsoft.azure.elasticdb.shard.mapmanager.ShardManagementException;
import com.microsoft.azure.elasticdb.shard.mapmanager.ShardMapManager;
import com.microsoft.azure.elasticdb.shard.mapper.ConnectionOptions;
import com.microsoft.azure.elasticdb.shard.store.StoreShard;
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
 * Representation of a single shard. Shards are basically locators for data sources i.e. <see
 * cref="ShardLocation"/>s that have been registered with a shard map. Shards are used in mapping as
 * targets of mappings (see <see cref="PointMapping"/> and <see cref="RangeMapping"/>).
 */
public final class Shard implements IShardProvider<ShardLocation>, Cloneable {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  /**
   * Hashcode for the shard.
   */
  private int hashCode;

  /**
   * Shard map object to which shard belongs.
   */
  private ShardMap shardMap;

  /**
   * Reference to the ShardMapManager.
   */
  private ShardMapManager shardMapManager;

  /**
   * Storage representation of the shard.
   */
  private StoreShard storeShard;

  /**
   * Constructs a Shard given shard creation arguments.
   *
   * @param shardMapManager Owning ShardMapManager.
   * @param shardMap Owning shard map.
   * @param creationInfo Shard creation information.
   */
  public Shard(ShardMapManager shardMapManager, ShardMap shardMap, ShardCreationInfo creationInfo) {
    assert shardMapManager != null;
    assert shardMap != null;
    assert creationInfo != null;

    this.setShardMapManager(shardMapManager);
    this.setShardMap(shardMap);

    this.setStoreShard(new StoreShard(UUID.randomUUID(), UUID.randomUUID(), shardMap.getId(),
        creationInfo.getLocation(), creationInfo.getStatus().getValue()));

    hashCode = this.calculateHashCode();
  }

  /**
   * Internal constructor that uses storage representation.
   *
   * @param shardMapManager Owning ShardMapManager.
   * @param shardMap Owning shard map.
   * @param storeShard Storage representation of the shard.
   */
  public Shard(ShardMapManager shardMapManager, ShardMap shardMap, StoreShard storeShard) {
    assert shardMapManager != null;
    this.setShardMapManager(shardMapManager);

    assert shardMap != null;
    this.setShardMap(shardMap);

    assert storeShard.getShardMapId() != null;
    this.setStoreShard(storeShard);

    hashCode = this.calculateHashCode();
  }

  /**
   * Gets Location of the shard.
   */
  public ShardLocation getLocation() {
    return this.getStoreShard().getLocation();
  }

  /**
   * Gets the status of the shard which can be either online or offline. Connections can only be
   * opened using <see cref="Shard.OpenConnection(string, ConnectionOptions)"/> on the shard map
   * when the shard is online. Setting the shard status to offline prevents connections when the
   * shard is undergoing maintenance operations.
   */
  public ShardStatus getStatus() {
    return ShardStatus.forValue(this.getStoreShard().getStatus());
  }

  /**
   * Identity of the shard. Each shard should have a unique one.
   */
  public UUID getId() {
    return this.getStoreShard().getId();
  }

  /**
   * Shard version.
   */
  public UUID getVersion() {
    return this.getStoreShard().getVersion();
  }

  /**
   * Shard for the ShardProvider object.
   */
  public Shard getShardInfo() {
    return this;
  }

  /**
   * Value corresponding to the Shard. Represents traits of the Shard
   * object provided by the ShardInfo property.
   */
  public ShardLocation getValue() {
    return this.getLocation();
  }

  public ShardMap getShardMap() {
    return shardMap;
  }

  public void setShardMap(ShardMap value) {
    shardMap = value;
  }

  /**
   * Identify of the ShardMap this shard belongs to.
   */
  public UUID getShardMapId() {
    return this.getStoreShard().getShardMapId();
  }

  public ShardMapManager getShardMapManager() {
    return shardMapManager;
  }

  public void setShardMapManager(ShardMapManager value) {
    shardMapManager = value;
  }

  public StoreShard getStoreShard() {
    return storeShard;
  }

  public void setStoreShard(StoreShard value) {
    storeShard = value;
  }

  /**
   * Opens a regular <see cref="SqlConnection"/> to the specified shard, with <see
   * cref="ConnectionOptions.Validate"/>.
   *
   * @param connectionString Connection string with credential information such as SQL Server
   * credentials or Integrated Security settings. The hostname of the server and the database name
   * for the shard are obtained from the lookup operation for key.   Note that the <see
   * cref="SqlConnection"/> object returned by this call is not protected against transient faults.
   * Callers should follow best practices to protect the connection against transient faults in
   * their application code, e.g., by using the transient fault handling functionality in the
   * Enterprise Library from Microsoft Patterns and Practices team.
   */
  public Connection openConnection(String connectionString) {
    return this.openConnection(connectionString, ConnectionOptions.Validate);
  }

  /**
   * Opens a regular <see cref="SqlConnection"/> to the specified shard.
   *
   * @param connectionString Connection string with credential information such as SQL Server
   * credentials or Integrated Security settings. The hostname of the server and the database name
   * for the shard are obtained from the lookup operation for key.
   * @param options Options for validation operations to perform on opened connection.  Note that
   * the <see cref="SqlConnection"/> object returned by this call is not protected against transient
   * faults. Callers should follow best practices to protect the connection against transient faults
   * in their application code, e.g., by using the transient fault handling functionality in the
   * Enterprise Library from Microsoft Patterns and Practices team.
   */
  public Connection openConnection(String connectionString, ConnectionOptions options) {
    try (ActivityIdScope activityIdScope = new ActivityIdScope(UUID.randomUUID())) {
      return this.getShardMap().openConnection(this, connectionString, options);
    }
  }

  /**
   * Asynchronously opens a regular <see cref="SqlConnection"/> to the specified shard, with <see
   * cref="ConnectionOptions.Validate"/>.
   *
   * @param connectionString Connection string with credential information such as SQL Server
   * credentials or Integrated Security settings. The hostname of the server and the database name
   * for the shard are obtained from the lookup operation for key.
   * @return A Task encapsulating an opened SqlConnection  Note that the <see cref="SqlConnection"/>
   * object returned by this call is not protected against transient faults. Callers should follow
   * best practices to protect the connection against transient faults in their application code,
   * e.g., by using the transient fault handling functionality in the Enterprise Library from
   * Microsoft Patterns and Practices team. All non-usage errors will be propagated via the returned
   * Task.
   */
  public Callable<Connection> openConnectionAsync(String connectionString) {
    return this.openConnectionAsync(connectionString, ConnectionOptions.Validate);
  }

  /**
   * Asynchronously a regular <see cref="SqlConnection"/> to the specified shard.
   *
   * @param connectionString Connection string with credential information such as SQL Server
   * credentials or Integrated Security settings. The hostname of the server and the database name
   * for the shard are obtained from the lookup operation for key.
   * @param options Options for validation operations to perform on opened connection.
   * @return A Task encapsulating an opened SqlConnection  Note that the <see cref="SqlConnection"/>
   * object returned by this call is not protected against transient faults. Callers should follow
   * best practices to protect the connection against transient faults in their application code,
   * e.g., by using the transient fault handling functionality in the Enterprise Library from
   * Microsoft Patterns and Practices team. All non-usage errors will be propagated via the returned
   * Task.
   */
  public Callable<Connection> openConnectionAsync(String connectionString,
      ConnectionOptions options) {
    try (ActivityIdScope activityIdScope = new ActivityIdScope(UUID.randomUUID())) {
      return this.getShardMap().openConnectionAsync(this, connectionString, options);
    }
  }

  /**
   * Performs validation that the local representation is as
   * up-to-date as the representation on the backing data store.
   *
   * @param shardMap Shard map to which the shard provider belongs.
   * @param conn Connection used for validation.
   */
  @Override
  public void validate(StoreShardMap shardMap, Connection conn) {
    try {
      Stopwatch stopwatch = Stopwatch.createStarted();
      log.info("Shard Validate Start; Connection: {}", conn.getMetaData().getURL());

      ValidationUtils.validateShard(conn, this.getShardMapManager(), shardMap,
          this.getStoreShard());

      stopwatch.stop();

      log.info("Shard Validate Complete; Connection: {} Duration:{}", conn.getMetaData().getURL(),
          stopwatch.elapsed(TimeUnit.MILLISECONDS));
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
      Stopwatch stopwatch = Stopwatch.createStarted();
      log.info("Shard ValidateAsync Start; Connection: {}", conn.getMetaData().getURL());

      returnVal = ValidationUtils.validateShardAsync(conn, this.getShardMapManager(), shardMap,
          this.getStoreShard());

      stopwatch.stop();

      log.info("Shard ValidateAsync Complete; Connection: {} Duration:{}",
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
  public Shard clone() {
    return new Shard(this.getShardMapManager(), this.getShardMap(), this.getStoreShard());
  }

  /**
   * Converts the object to its string representation.
   *
   * @return String representation of the object.
   */
  @Override
  public String toString() {
    return StringUtilsLocal.formatInvariant("S[%s:%s:%s]", this.getId().toString(),
        this.getVersion().toString(), this.getLocation().toString());
  }

  /**
   * Determines whether the specified object is equal to the current object.
   *
   * @param obj The object to compare with the current object.
   * @return True if the specified object is equal to the current object; otherwise, false.
   */
  @Override
  public boolean equals(Object obj) {
    return this.equals((Shard) ((obj instanceof Shard) ? obj : null));
  }

  /**
   * Performs equality comparison with given Shard.
   *
   * @param other Shard to compare with.
   * @return True if this object is equal to other object, false otherwise.
   */
  public boolean equals(Shard other) {
    if (null == other) {
      return false;
    } else {
      if (this.hashCode() != other.hashCode()) {
        return false;
      } else {
        // DEVNOTE(wbasheer): We are assuming identify comparison, without caring about version.
        boolean result = this.getId().equals(other.getId())
            && this.getVersion().equals(other.getVersion());

        assert !result || this.getShardMapId().equals(other.getShardMapId());
        assert !result || (this.getLocation().hashCode() == other.getLocation().hashCode());
        assert !result || (this.getStatus() == other.getStatus());

        return result;
      }
    }
  }

  /**
   * Calculates the hash code for this instance.
   *
   * @return Hash code for the object.
   */
  @Override
  public int hashCode() {
    return hashCode;
  }

  /**
   * Calculates the hash code for the object.
   *
   * @return Hash code for the object.
   */
  private int calculateHashCode() {
    // DEVNOTE(wbasheer): We are assuming identify comparison, without caring about version.
    return this.getId().hashCode();
    //return ShardKey.qpHash(this.Id.GetHashCode(), this.Version.GetHashCode());
  }
}

package com.microsoft.azure.elasticdb.shard.base;

// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

import com.microsoft.azure.elasticdb.core.commons.logging.ActivityIdScope;
import com.microsoft.azure.elasticdb.shard.map.ShardMap;
import com.microsoft.azure.elasticdb.shard.mapmanager.ShardMapManager;
import com.microsoft.azure.elasticdb.shard.mapper.ConnectionOptions;
import com.microsoft.azure.elasticdb.shard.store.StoreShard;
import com.microsoft.azure.elasticdb.shard.store.StoreShardMap;
import com.microsoft.azure.elasticdb.shard.utils.StringUtilsLocal;
import com.microsoft.sqlserver.jdbc.SQLServerConnection;

import java.sql.Connection;
import java.util.UUID;
import java.util.concurrent.Callable;

/**
 * Representation of a single shard. Shards are basically locators for
 * data sources i.e. <see cref="ShardLocation"/>s that have been
 * registered with a shard map. Shards are used in
 * mapping as targets of mappings (see <see cref="PointMapping{TKey}"/>
 * and <see cref="RangeMapping{TKey}"/>).
 */
public final class Shard implements IShardProvider<ShardLocation>, Cloneable {
    /**
     * Hashcode for the shard.
     */
    private int _hashCode;
    /**
     * Shard map object to which shard belongs.
     */
    private ShardMap _shardMap;
    /**
     * Reference to the ShardMapManager.
     */
    private ShardMapManager Manager;
    /**
     * Storage representation of the shard.
     */
    private StoreShard storeShard;

    /**
     * Constructs a Shard given shard creation arguments.
     *
     * @param manager      Owning ShardMapManager.
     * @param shardMap     Owning shard map.
     * @param creationInfo Shard creation information.
     */
    public Shard(ShardMapManager manager, ShardMap shardMap, ShardCreationInfo creationInfo) {
        assert manager != null;
        assert shardMap != null;
        assert creationInfo != null;

        this.setManager(manager);
        this.setShardMap(shardMap);

        this.setStoreShard(new StoreShard(UUID.randomUUID(), UUID.randomUUID(), shardMap.getId(), creationInfo.getLocation(), creationInfo.getStatus().getValue()));

        _hashCode = this.CalculateHashCode();
    }

    /**
     * Internal constructor that uses storage representation.
     *
     * @param manager    Owning ShardMapManager.
     * @param shardMap   Owning shard map.
     * @param storeShard Storage representation of the shard.
     */
    public Shard(ShardMapManager manager, ShardMap shardMap, StoreShard storeShard) {
        assert manager != null;
        this.setManager(manager);

        assert shardMap != null;
        this.setShardMap(shardMap);

        assert storeShard.getShardMapId() != null;
        this.setStoreShard(storeShard);

        _hashCode = this.CalculateHashCode();
    }

    /**
     * Gets Location of the shard.
     */
    public ShardLocation getLocation() {
        return this.getStoreShard().getLocation();
    }

    /**
     * Gets the status of the shard which can be either online or offline.
     * Connections can only be opened using <see cref="Shard.OpenConnection(string, ConnectionOptions)"/>
     * on the shard map when the shard is online. Setting the shard status to offline
     * prevents connections when the shard is undergoing maintenance operations.
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

    @Override
    public void Validate(StoreShardMap shardMap, SQLServerConnection conn) {

    }

    @Override
    public Callable ValidateAsync(StoreShardMap shardMap, SQLServerConnection conn) {
        return null;
    }

    public ShardMap getShardMap() {
        return _shardMap;
    }

    public void setShardMap(ShardMap value) {
        _shardMap = value;
    }

    /**
     * Identify of the ShardMap this shard belongs to.
     */
    public UUID getShardMapId() {
        return this.getStoreShard().getShardMapId();
    }

    public ShardMapManager getManager() {
        return Manager;
    }

    public void setManager(ShardMapManager value) {
        Manager = value;
    }

    public StoreShard getStoreShard() {
        return storeShard;
    }

    public void setStoreShard(StoreShard value) {
        storeShard = value;
    }

    ///#region Sync OpenConnection methods

    /**
     * Opens a regular <see cref="SqlConnection"/> to the specified shard, with <see cref="ConnectionOptions.Validate"/>.
     *
     * @param connectionString Connection string with credential information such as SQL Server credentials or Integrated Security settings.
     *                         The hostname of the server and the database name for the shard are obtained from the lookup operation for key.
     *                         <p>
     *                         <p>
     *                         Note that the <see cref="SqlConnection"/> object returned by this call is not protected against transient faults.
     *                         Callers should follow best practices to protect the connection against transient faults in their application code, e.g., by using the transient fault handling
     *                         functionality in the Enterprise Library from Microsoft Patterns and Practices team.
     */
    public Connection OpenConnection(String connectionString) {
        return this.OpenConnection(connectionString, ConnectionOptions.Validate);
    }

    /**
     * Opens a regular <see cref="SqlConnection"/> to the specified shard.
     *
     * @param connectionString Connection string with credential information such as SQL Server credentials or Integrated Security settings.
     *                         The hostname of the server and the database name for the shard are obtained from the lookup operation for key.
     * @param options          Options for validation operations to perform on opened connection.
     *                         <p>
     *                         Note that the <see cref="SqlConnection"/> object returned by this call is not protected against transient faults.
     *                         Callers should follow best practices to protect the connection against transient faults in their application code, e.g., by using the transient fault handling
     *                         functionality in the Enterprise Library from Microsoft Patterns and Practices team.
     */
    public Connection OpenConnection(String connectionString, ConnectionOptions options) {
        try (ActivityIdScope activityIdScope = new ActivityIdScope(UUID.randomUUID())) {
            return this.getShardMap().OpenConnection((IShardProvider) ((this instanceof IShardProvider) ? this : null), connectionString, options);
        }
    }

    ///#endregion

    ///#region Async OpenConnection methods

    /**
     * Asynchronously opens a regular <see cref="SqlConnection"/> to the specified shard, with <see cref="ConnectionOptions.Validate"/>.
     *
     * @param connectionString Connection string with credential information such as SQL Server credentials or Integrated Security settings.
     *                         The hostname of the server and the database name for the shard are obtained from the lookup operation for key.
     * @return A Task encapsulating an opened SqlConnection
     * <p>
     * Note that the <see cref="SqlConnection"/> object returned by this call is not protected against transient faults.
     * Callers should follow best practices to protect the connection against transient faults in their application code, e.g., by using the transient fault handling
     * functionality in the Enterprise Library from Microsoft Patterns and Practices team.
     * All non-usage errors will be propagated via the returned Task.
     */
    public Callable<SQLServerConnection> OpenConnectionAsync(String connectionString) {
        return this.OpenConnectionAsync(connectionString, ConnectionOptions.Validate);
    }

    /**
     * Asynchronously a regular <see cref="SqlConnection"/> to the specified shard.
     *
     * @param connectionString Connection string with credential information such as SQL Server credentials or Integrated Security settings.
     *                         The hostname of the server and the database name for the shard are obtained from the lookup operation for key.
     * @param options          Options for validation operations to perform on opened connection.
     * @return A Task encapsulating an opened SqlConnection
     * <p>
     * Note that the <see cref="SqlConnection"/> object returned by this call is not protected against transient faults.
     * Callers should follow best practices to protect the connection against transient faults in their application code, e.g., by using the transient fault handling
     * functionality in the Enterprise Library from Microsoft Patterns and Practices team.
     * All non-usage errors will be propagated via the returned Task.
     */
    public Callable<SQLServerConnection> OpenConnectionAsync(String connectionString, ConnectionOptions options) {
        try (ActivityIdScope activityIdScope = new ActivityIdScope(UUID.randomUUID())) {
            return this.getShardMap().OpenConnectionAsync((IShardProvider) ((this instanceof IShardProvider) ? this : null), connectionString, options);
        }
    }

    ///#endregion

    ///#region IShardProvider<Shard>

    /**
     * Performs validation that the local representation is as
     * up-to-date as the representation on the backing data store.
     *
     * @param shardMap Shard map to which the shard provider belongs.
     * @param conn     Connection used for validation.
     */
    @Override
    public void Validate(StoreShardMap shardMap, Connection conn) {
        /*Stopwatch stopwatch = Stopwatch.createStarted();
        getTracer().TraceInfo(TraceSourceConstants.ComponentNames.Shard, "Validate", "Start; Connection: {0};", conn.ConnectionString);*/

        ValidationUtils.ValidateShard(conn, this.getManager(), shardMap, this.getStoreShard());

        /*stopwatch.stop();

        getTracer().TraceInfo(TraceSourceConstants.ComponentNames.Shard, "Validate", "Complete; Connection: {0}; Duration: {1}", conn.ConnectionString, stopwatch.Elapsed);*/
    }

    /**
     * Asynchronously performs validation that the local representation is as
     * up-to-date as the representation on the backing data store.
     *
     * @param shardMap Shard map to which the shard provider belongs.
     * @param conn     Connection used for validation.
     * @return A task to await validation completion
     */
    @Override
    public Callable ValidateAsync(StoreShardMap shardMap, Connection conn) {
        /*Stopwatch stopwatch = Stopwatch.createStarted();
        getTracer().TraceInfo(TraceSourceConstants.ComponentNames.Shard, "ValidateAsync", "Start; Connection: {0};", conn.ConnectionString);*/

        //TODO await
        ValidationUtils.ValidateShardAsync(conn, this.getManager(), shardMap, this.getStoreShard());
        //.ConfigureAwait(false);

        /*stopwatch.stop();

        getTracer().TraceInfo(TraceSourceConstants.ComponentNames.Shard, "ValidateAsync", "Complete; Connection: {0}; Duration: {1}", conn.ConnectionString, stopwatch.Elapsed);*/
        return null;
    }

    ///#endregion IShardProvider<Shard>

    ///#region ICloneable<Shard>

    /**
     * Clones the instance.
     *
     * @return clone of the instance.
     */
    public Shard clone() {
        return new Shard(this.getManager(), this.getShardMap(), this.getStoreShard());
    }

    ///#endregion ICloneable<Shard>

    /**
     * Converts the object to its string representation.
     *
     * @return String representation of the object.
     */
    @Override
    public String toString() {
        return StringUtilsLocal.FormatInvariant("S[%s:%s:%s]", this.getId().toString(), this.getVersion().toString(), this.getLocation().toString());
    }

    ///#region IEquatable

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
                /*boolean result = UUID.OpEquality(this.getId(), other.getId()) && UUID.OpEquality(this.getVersion(), other.getVersion());

                assert !result || UUID.OpEquality(this.getShardMapId(), other.getShardMapId());
                assert !result || (this.getLocation().hashCode() == other.getLocation().hashCode());
                assert !result || (this.getStatus() == other.getStatus());*/

                return false; //result;
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
        return _hashCode;
    }

    ///#endregion IEquatable

    /**
     * Calculates the hash code for the object.
     *
     * @return Hash code for the object.
     */
    private int CalculateHashCode() {
        // DEVNOTE(wbasheer): We are assuming identify comparison, without caring about version.
        return this.getId().hashCode();
        //return ShardKey.QPHash(this.Id.GetHashCode(), this.Version.GetHashCode());
    }
}

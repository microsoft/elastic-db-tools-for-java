package com.microsoft.azure.elasticdb.shard.base;

// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

import com.microsoft.azure.elasticdb.shard.map.ShardMap;
import com.microsoft.azure.elasticdb.shard.mapmanager.ShardMapManager;
import com.microsoft.azure.elasticdb.shard.store.DefaultStoreMapping;
import com.microsoft.azure.elasticdb.shard.store.IStoreMapping;
import com.microsoft.azure.elasticdb.shard.store.StoreShardMap;
import com.microsoft.azure.elasticdb.shard.utils.ICloneable;
import com.microsoft.azure.elasticdb.shard.utils.StringUtilsLocal;
import com.microsoft.sqlserver.jdbc.SQLServerConnection;

import java.sql.Connection;
import java.util.UUID;
import java.util.concurrent.Callable;

/**
 * Represents a mapping between the singleton key value of a shardlet (a point) and a <see cref="Shard"/>.
 * <p>
 * <typeparam name="TKey">Type of the key (point).</typeparam>
 */
public final class PointMapping<TKey> implements IShardProvider<TKey>, ICloneable<PointMapping<TKey>>, IMappingInfoProvider {
    /**
     * Shard object associated with the mapping.
     */
    private Shard _shard;
    /**
     * Gets key value.
     */
    private TKey Value;
    /**
     * Holder of the key value's binary representation.
     */
    private ShardKey Key;
    /**
     * Reference to the ShardMapManager.
     */
    private ShardMapManager _manager;
    /**
     * Storage representation of the mapping.
     */
    private IStoreMapping _storeMapping;

    /**
     * Constructs a point mapping given mapping creation arguments.
     *
     * @param manager      Owning ShardMapManager.
     * @param creationInfo Mapping creation information.
     */
    public PointMapping(ShardMapManager manager, PointMappingCreationInfo<TKey> creationInfo) {
        assert manager != null;
        assert creationInfo != null;
        assert creationInfo.getShard() != null;

        this.setManager(manager);

        _shard = creationInfo.getShard();

        this.setStoreMapping(new DefaultStoreMapping(UUID.randomUUID(), creationInfo.getShard(), creationInfo.getKey().getRawValue(), null, creationInfo.getStatus().getValue()));

        this.setKey(creationInfo.getKey());
        this.setValue(creationInfo.getValue());
    }

    /**
     * Internal constructor used for deserialization from store representation of
     * the mapping object.
     *
     * @param manager  Owning ShardMapManager.
     * @param shardMap Owning shard map.
     * @param mapping  Storage representation of the mapping.
     */
    public PointMapping(ShardMapManager manager, ShardMap shardMap, IStoreMapping mapping) {
        assert manager != null;
        this.setManager(manager);

        assert mapping != null;
        assert mapping.getShardMapId() != null;
        assert mapping.getStoreShard().getShardMapId() != null;
        this.setStoreMapping(mapping);

        //TODO: _shard = new Shard(this.getShardMapManager(), shardMap, mapping.getStoreShard());
        //TODO: this.setKey(ShardKey.FromRawValue(ShardKey.ShardKeyTypeFromType(TKey.class), mapping.getMinValue()));
        this.setValue((TKey) this.getKey().getValue());
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
        return _shard;
    }

    public TKey getValue() {
        return Value;
    }

    private void setValue(TKey value) {
        Value = value;
    }

    @Override
    public void Validate(StoreShardMap shardMap, SQLServerConnection conn) {

    }

    @Override
    public Callable ValidateAsync(StoreShardMap shardMap, SQLServerConnection conn) {
        return null;
    }

    public ShardKey getKey() {
        return Key;
    }

    public void setKey(ShardKey value) {
        Key = value;
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

    public ShardMapManager getManager() {
        return _manager;
    }

    public void setManager(ShardMapManager value) {
        _manager = value;
    }

    public IStoreMapping getStoreMapping() {
        return _storeMapping;
    }

    public void setStoreMapping(IStoreMapping value) {
        _storeMapping = value;
    }

    /**
     * Converts the object to its string representation.
     *
     * @return String representation of the object.
     */
    @Override
    public String toString() {
        return StringUtilsLocal.FormatInvariant("P[{0}:{1}]", this.getId(), this.getKey());
    }

    /**
     * Determines whether the specified object is equal to the current object.
     *
     * @param obj The object to compare with the current object.
     * @return True if the specified object is equal to the current object; otherwise, false.
     */
    @Override
    public boolean equals(Object obj) {
        //TODO:
        /*PointMapping<TKey> other = (PointMapping<TKey>) ((obj instanceof PointMapping<TKey>) ? obj : null);

        if (other == null) {
            return false;
        }

        if (this.getId().equals(other.getId())) {
            assert this.getKey() == other.getKey();
            return true;
        }*/

        return false;
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

    ///#region IShardProvider<TKey>

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
     * @param conn     Connection used for validation.
     */
    @Override
    public void Validate(StoreShardMap shardMap, Connection conn) {
        /*Stopwatch stopwatch = Stopwatch.createStarted();
        getTracer().TraceInfo(TraceSourceConstants.ComponentNames.PointMapping, "Validate", "Start; Connection: {0};", conn.ConnectionString);*/

        ValidationUtils.ValidateMapping(conn, this.getManager(), shardMap, this.getStoreMapping());

        /*stopwatch.stop();

        getTracer().TraceInfo(TraceSourceConstants.ComponentNames.PointMapping, "Validate", "Complete; Connection: {0}; Duration: {1}", conn.ConnectionString, stopwatch.Elapsed);*/
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
        getTracer().TraceInfo(TraceSourceConstants.ComponentNames.PointMapping, "ValidateAsync", "Start; Connection: {0};", conn.ConnectionString);*/

        //TODO: await
        ValidationUtils.ValidateMappingAsync(conn, this.getManager(), shardMap, this.getStoreMapping());
        //.ConfigureAwait(false);

        /*stopwatch.stop();

        getTracer().TraceInfo(TraceSourceConstants.ComponentNames.PointMapping, "ValidateAsync", "Complete; Connection: {0}; Duration: {1}", conn.ConnectionString, stopwatch.Elapsed);*/
        return null;
    }

    ///#endregion IShardProvider<TKey>

    ///#region ICloneable<PointMapping<TKey>>

    /**
     * Clones the instance.
     *
     * @return Clone of the instance.
     */
    public PointMapping<TKey> Clone() {
        return new PointMapping<TKey>(this.getManager(), this.getShard().getShardMap(), this.getStoreMapping());
    }

    ///#endregion ICloneable<PointMapping<TKey>>

    ///#region IMappingInfoProvider

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

    ///#endregion IMappingInfoProvider
}
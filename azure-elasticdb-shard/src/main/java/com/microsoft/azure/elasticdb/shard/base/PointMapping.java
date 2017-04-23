package com.microsoft.azure.elasticdb.shard.base;

// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

import com.google.common.base.Stopwatch;
import com.microsoft.azure.elasticdb.shard.map.ShardMap;
import com.microsoft.azure.elasticdb.shard.mapmanager.ShardMapManager;
import com.microsoft.azure.elasticdb.shard.store.StoreMapping;
import com.microsoft.azure.elasticdb.shard.store.StoreShardMap;
import com.microsoft.azure.elasticdb.shard.utils.StringUtilsLocal;
import com.microsoft.sqlserver.jdbc.SQLServerConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.invoke.MethodHandles;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

/**
 * Represents a mapping between the singleton key value of a shardlet (a point) and a <see cref="Shard"/>.
 * <p>
 * <typeparam name="TKey">Type of the key (point).</typeparam>
 */
public final class PointMapping implements IShardProvider<Object>, Cloneable, IMappingInfoProvider {
    private final static Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    /**
     * Shard object associated with the mapping.
     */
    private Shard _shard;
    /**
     * Gets key value.
     */
    private Object Value;
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
    private StoreMapping _storeMapping;

    /**
     * Constructs a point mapping given mapping creation arguments.
     *
     * @param manager      Owning ShardMapManager.
     * @param creationInfo Mapping creation information.
     */
    public PointMapping(ShardMapManager manager, PointMappingCreationInfo creationInfo) {
        assert manager != null;
        assert creationInfo != null;
        assert creationInfo.getShard() != null;

        this.setManager(manager);

        _shard = creationInfo.getShard();

        this.setStoreMapping(new StoreMapping(UUID.randomUUID(), creationInfo.getShard(), creationInfo.getKey().getRawValue(), null, creationInfo.getStatus().getValue()));

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
    public PointMapping(ShardMapManager manager, ShardMap shardMap, StoreMapping mapping) {
        assert manager != null;
        assert mapping != null;
        assert mapping.getShardMapId() != null;
        assert mapping.getStoreShard().getShardMapId() != null;

        this.setManager(manager);
        this.setStoreMapping(mapping);

        _shard = new Shard(this.getManager(), shardMap, mapping.getStoreShard());
        this.setKey(ShardKey.FromRawValue(shardMap.getKeyType(), mapping.getMinValue()));
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
        return _shard;
    }

    public Object getValue() {
        return Value;
    }

    private void setValue(Object value) {
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

    public StoreMapping getStoreMapping() {
        return _storeMapping;
    }

    public void setStoreMapping(StoreMapping value) {
        _storeMapping = value;
    }

    /**
     * Converts the object to its string representation.
     *
     * @return String representation of the object.
     */
    @Override
    public String toString() {
        return StringUtilsLocal.FormatInvariant("P[%s:%s]", this.getId().toString(), this.getKey().toString());
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
        try {
            log.info("PointMapping Validate Start; Connection: {}", conn.getMetaData().getURL());
            Stopwatch stopwatch = Stopwatch.createStarted();

            ValidationUtils.ValidateMapping(conn, this.getManager(), shardMap, this.getStoreMapping());

            stopwatch.stop();

            log.info("PointMapping Validate Complete; Connection: {}; Duration:{}", conn.getMetaData().getURL(), stopwatch.elapsed(TimeUnit.MILLISECONDS));
        } catch (SQLException e) {
            e.printStackTrace();
        }
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
        try {
            log.info("PointMapping ValidateAsync Start; Connection: {}", conn.getMetaData().getURL());

            Stopwatch stopwatch = Stopwatch.createStarted();

            //TODO: await
            ValidationUtils.ValidateMappingAsync(conn, this.getManager(), shardMap, this.getStoreMapping());
            //.ConfigureAwait(false);

            stopwatch.stop();

            log.info("PointMapping ValidateAsync Complete; Connection: {} Duration:{}", conn.getMetaData().getURL(), stopwatch.elapsed(TimeUnit.MILLISECONDS));
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return null;
    }

    ///#endregion IShardProvider<TKey>

    ///#region ICloneable<PointMapping<TKey>>

    /**
     * Clones the instance.
     *
     * @return clone of the instance.
     */
    public PointMapping clone() {
        return new PointMapping(this.getManager(), this.getShard().getShardMap(), this.getStoreMapping());
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
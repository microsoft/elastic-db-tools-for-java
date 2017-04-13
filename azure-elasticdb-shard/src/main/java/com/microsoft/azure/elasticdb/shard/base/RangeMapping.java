package com.microsoft.azure.elasticdb.shard.base;

// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

import com.microsoft.azure.elasticdb.shard.map.ShardMap;
import com.microsoft.azure.elasticdb.shard.mapmanager.ShardMapManager;
import com.microsoft.azure.elasticdb.shard.store.StoreMapping;
import com.microsoft.azure.elasticdb.shard.store.StoreShardMap;
import com.microsoft.azure.elasticdb.shard.utils.StringUtilsLocal;
import com.microsoft.sqlserver.jdbc.SQLServerConnection;

import java.sql.Connection;
import java.util.UUID;
import java.util.concurrent.Callable;

/**
 * Represents a mapping between a range of key values and a <see cref="Shard"/>.
 * <p>
 * <typeparam name="TKey">Key type.</typeparam>
 */
public final class RangeMapping<TKey> implements IShardProvider<Range<TKey>>, Cloneable, IMappingInfoProvider {
    /**
     * Shard object associated with the mapping.
     */
    private Shard _shard;
    /**
     * Gets the Range of the mapping.
     */
    private Range<TKey> Value;
    /**
     * Holder of the range value's binary representation.
     */
    private ShardRange Range;
    /**
     * Reference to the ShardMapManager.
     */
    private ShardMapManager Manager;
    /**
     * Storage representation of the mapping.
     */
    private StoreMapping storeMapping;

    /**
     * Constructs a range mapping given mapping creation arguments.
     *
     * @param manager      Owning ShardMapManager.
     * @param creationInfo Mapping creation information.
     */
    public RangeMapping(ShardMapManager manager, RangeMappingCreationInfo<TKey> creationInfo) {
        assert manager != null;
        assert creationInfo != null;
        assert creationInfo.getShard() != null;

        this.setManager(manager);
        _shard = creationInfo.getShard();

        this.setStoreMapping(new StoreMapping(UUID.randomUUID()
                , creationInfo.getShard()
                , creationInfo.getRange().getLow().getRawValue()
                , creationInfo.getRange().getHigh().getRawValue()
                , creationInfo.getStatus().getValue()));

        this.setRange(creationInfo.getRange());
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
    public RangeMapping(ShardMapManager manager, ShardMap shardMap, StoreMapping mapping) {
        assert manager != null;
        this.setManager(manager);

        assert mapping != null;
        assert mapping.getShardMapId() != null;
        assert mapping.getStoreShard().getShardMapId() != null;
        this.setStoreMapping(mapping);

        _shard = new Shard(this.getManager(), shardMap, mapping.getStoreShard());

        /*this.setRange(new ShardRange(
                ShardKey.FromRawValue(ShardKey.ShardKeyTypeFromType(TKey.class), mapping.getMinValue())
                , ShardKey.FromRawValue(ShardKey.ShardKeyTypeFromType(TKey.class), mapping.getMaxValue())));*/

        this.setValue(this.getRange().getHigh().getIsMax() ? new Range<>(this.getRange().getLow().GetValue()) : new Range<>(this.getRange().getLow().GetValue(), this.getRange().getHigh().GetValue()));
    }

    /**
     * Gets the <see cref="MappingStatus"/> of the mapping.
     */
    public MappingStatus getStatus() {
        if (this.getStoreMapping().getStatus() == MappingStatus.Online.getValue()) {
            return MappingStatus.Online;
        }
        return MappingStatus.Offline;
    }

    /**
     * Gets Shard that contains the range of values.
     */
    public Shard getShard() {
        return _shard;
    }

    public Range<TKey> getValue() {
        return Value;
    }

    private void setValue(Range<TKey> value) {
        Value = value;
    }

    @Override
    public void Validate(StoreShardMap shardMap, SQLServerConnection conn) {

    }

    @Override
    public Callable ValidateAsync(StoreShardMap shardMap, SQLServerConnection conn) {
        return null;
    }

    public ShardRange getRange() {
        return Range;
    }

    public void setRange(ShardRange value) {
        Range = value;
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
        return Manager;
    }

    public void setManager(ShardMapManager value) {
        Manager = value;
    }

    public StoreMapping getStoreMapping() {
        return storeMapping;
    }

    public void setStoreMapping(StoreMapping value) {
        storeMapping = value;
    }

    /**
     * Converts the object to its string representation.
     *
     * @return String representation of the object.
     */
    @Override
    public String toString() {
        return StringUtilsLocal.FormatInvariant("R[{0}:{1}]", this.getId(), this.getRange());
    }

    /**
     * Determines whether the specified object is equal to the current object.
     *
     * @param obj The object to compare with the current object.
     * @return True if the specified object is equal to the current object; otherwise, false.
     */
    @Override
    public boolean equals(Object obj) {
        if (obj == null || ! (obj instanceof RangeMapping)) {
            return false;
        }
        RangeMapping other = (RangeMapping) obj;
        if (this.getId().equals(other.getId())) {
            assert this.getRange().equals(other.getRange());
            return true;
        }
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

    ///#region IShardProvider<Range<TKey>>

    /**
     * Shard that contains the range of values.
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
        getTracer().TraceInfo(TraceSourceConstants.ComponentNames.RangeMapping, "Validate", "Start; Connection: {0};", conn.ConnectionString);*/

        ValidationUtils.ValidateMapping(conn, this.getManager(), shardMap, this.getStoreMapping());

        /*stopwatch.stop();

        getTracer().TraceInfo(TraceSourceConstants.ComponentNames.RangeMapping, "Validate", "Complete; Connection: {0}; Duration: {1}", conn.ConnectionString, stopwatch.Elapsed);*/
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
        getTracer().TraceInfo(TraceSourceConstants.ComponentNames.RangeMapping, "ValidateAsync", "Start; Connection: {0};", conn.ConnectionString);*/

        //TODO await
        ValidationUtils.ValidateMappingAsync(conn, this.getManager(), shardMap, this.getStoreMapping());
        //.ConfigureAwait(false);

        /*stopwatch.stop();

        getTracer().TraceInfo(TraceSourceConstants.ComponentNames.RangeMapping, "ValidateAsync", "Complete; Connection: {0}; Duration: {1}", conn.ConnectionString, stopwatch.Elapsed);*/
        return null;
    }

    ///#endregion IShardProvider<Range<TKey>>

    ///#region ICloneable<RangeMapping<TKey>>

    /**
     * Clones the instance which implements the interface.
     *
     * @return clone of the instance.
     */
    public RangeMapping<TKey> clone() {
        return new RangeMapping<TKey>(this.getManager(), this.getShard().getShardMap(), this.getStoreMapping());
    }

    ///#endregion ICloneable<RangeMapping<TKey>>

    ///#region IMappingInfoProvider

    /**
     * Type of the mapping.
     */
    public MappingKind getKind() {
        return MappingKind.RangeMapping;
    }

    /**
     * Mapping type, useful for diagnostics.
     */
    public String getTypeName() {
        return "RangeMapping";
    }

    ///#endregion IMappingInfoProvider
}
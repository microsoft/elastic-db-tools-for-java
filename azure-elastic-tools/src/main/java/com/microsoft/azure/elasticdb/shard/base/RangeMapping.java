package com.microsoft.azure.elasticdb.shard.base;

import java.lang.invoke.MethodHandles;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/*
 * Elastic database tools for Azure SQL Database.
 * 
 * Copyright(c) Microsoft Corporation All rights reserved.
 * 
 * This program is made available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

import com.google.common.base.Stopwatch;
import com.microsoft.azure.elasticdb.shard.map.ShardMap;
import com.microsoft.azure.elasticdb.shard.mapmanager.ShardManagementException;
import com.microsoft.azure.elasticdb.shard.mapmanager.ShardMapManager;
import com.microsoft.azure.elasticdb.shard.store.StoreMapping;
import com.microsoft.azure.elasticdb.shard.store.StoreShardMap;
import com.microsoft.azure.elasticdb.shard.utils.StringUtilsLocal;

/**
 * Represents a mapping between a range of key values and a <see cref="Shard"/>.
 */
public final class RangeMapping implements IShardProvider<Range>, Cloneable, IMappingInfoProvider {

    private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    /**
     * Shard object associated with the mapping.
     */
    private Shard shard;

    /**
     * Gets the Range of the mapping.
     */
    private Range value;

    /**
     * Holder of the range value's binary representation.
     */
    private ShardRange range;

    /**
     * Reference to the ShardMapManager.
     */
    private ShardMapManager shardMapManager;

    /**
     * Storage representation of the mapping.
     */
    private StoreMapping storeMapping;

    /**
     * Constructs a range mapping given mapping creation arguments.
     *
     * @param shardMapManager
     *            Owning ShardMapManager.
     * @param creationInfo
     *            Mapping creation information.
     */
    public RangeMapping(ShardMapManager shardMapManager,
            RangeMappingCreationInfo creationInfo) {
        assert shardMapManager != null;
        assert creationInfo != null;
        assert creationInfo.getShard() != null;

        this.setShardMapManager(shardMapManager);
        shard = creationInfo.getShard();

        this.setStoreMapping(new StoreMapping(UUID.randomUUID(), creationInfo.getShard(), creationInfo.getRange().getLow().getRawValue(),
                creationInfo.getRange().getHigh().getRawValue(), creationInfo.getStatus().getValue()));

        this.setRange(creationInfo.getRange());
        this.setValue(creationInfo.getValue());
    }

    /**
     * Internal constructor used for deserialization from store representation of the mapping object.
     *
     * @param shardMapManager
     *            Owning ShardMapManager.
     * @param shardMap
     *            Owning shard map.
     * @param mapping
     *            Storage representation of the mapping.
     */
    public RangeMapping(ShardMapManager shardMapManager,
            ShardMap shardMap,
            StoreMapping mapping) {
        assert shardMapManager != null;
        this.setShardMapManager(shardMapManager);

        assert mapping != null;
        assert mapping.getShardMapId() != null;
        assert mapping.getStoreShard().getShardMapId() != null;
        this.setStoreMapping(mapping);

        shard = new Shard(this.getShardMapManager(), shardMap, mapping.getStoreShard());

        this.setRange(new ShardRange(ShardKey.fromRawValue(shardMap.getKeyType(), mapping.getMinValue()),
                ShardKey.fromRawValue(shardMap.getKeyType(), mapping.getMaxValue())));

        ShardKey high = this.getRange().getHigh();
        ShardKey low = this.getRange().getLow();
        Class lowDataType = low.getDataType();
        this.setValue(high.getIsMax() ? new Range(low.getValueWithCheck(lowDataType))
                : new Range(low.getValueWithCheck(lowDataType), high.getValueWithCheck(high.getDataType())));
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
        return shard;
    }

    public Range getValue() {
        return value;
    }

    private void setValue(Range value) {
        this.value = value;
    }

    public ShardRange getRange() {
        return range;
    }

    public void setRange(ShardRange value) {
        range = value;
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

    public void setShardMapManager(ShardMapManager value) {
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
        return StringUtilsLocal.formatInvariant("R[%s:%s]", this.getId().toString(), this.getRange().toString());
    }

    /**
     * Determines whether the specified object is equal to the current object.
     *
     * @param obj
     *            The object to compare with the current object.
     * @return True if the specified object is equal to the current object; otherwise, false.
     */
    @Override
    public boolean equals(Object obj) {
        if (obj == null || !(obj instanceof RangeMapping)) {
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

    /**
     * Shard that contains the range of values.
     */
    public Shard getShardInfo() {
        return this.getShard();
    }

    /**
     * Performs validation that the local representation is as up-to-date as the representation on the backing data store.
     *
     * @param shardMap
     *            Shard map to which the shard provider belongs.
     * @param conn
     *            Connection used for validation.
     */
    @Override
    public void validate(StoreShardMap shardMap,
            Connection conn) {
        try {
            log.info("RangeMapping Validate Start; Connection: {}", conn.getMetaData().getURL());

            Stopwatch stopwatch = Stopwatch.createStarted();

            ValidationUtils.validateMapping(conn, this.getShardMapManager(), shardMap, this.getStoreMapping());

            stopwatch.stop();

            log.info("RangeMapping Validate Complete; Connection: {} Duration:{}", conn.getMetaData().getURL(),
                    stopwatch.elapsed(TimeUnit.MILLISECONDS));
        }
        catch (SQLException e) {
            e.printStackTrace();
            throw (ShardManagementException) e.getCause();
        }
    }

    /**
     * Asynchronously performs validation that the local representation is as up-to-date as the representation on the backing data store.
     *
     * @param shardMap
     *            Shard map to which the shard provider belongs.
     * @param conn
     *            Connection used for validation.
     * @return A task to await validation completion
     */
    @Override
    public Callable validateAsync(StoreShardMap shardMap,
            Connection conn) {
        Callable returnVal;
        try {
            log.info("RangeMapping ValidateAsync Start; Connection: {}", conn.getMetaData().getURL());

            Stopwatch stopwatch = Stopwatch.createStarted();

            returnVal = ValidationUtils.validateMappingAsync(conn, this.getShardMapManager(), shardMap, this.getStoreMapping());

            stopwatch.stop();

            log.info("RangeMapping ValidateAsync Complete; Connection: {} Duration:{}", conn.getMetaData().getURL(),
                    stopwatch.elapsed(TimeUnit.MILLISECONDS));
        }
        catch (SQLException e) {
            e.printStackTrace();
            throw (ShardManagementException) e.getCause();
        }
        return returnVal;
    }

    /**
     * Clones the instance which implements the interface.
     *
     * @return clone of the instance.
     */
    public RangeMapping clone() {
        return new RangeMapping(this.getShardMapManager(), this.getShard().getShardMap(), this.getStoreMapping());
    }

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
}
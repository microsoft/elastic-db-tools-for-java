package com.microsoft.azure.elasticdb.shard.base;

/*
 * Elastic database tools for Azure SQL Database.
 * 
 * Copyright(c) Microsoft Corporation All rights reserved.
 * 
 * This program is made available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

import com.microsoft.azure.elasticdb.shard.utils.ExceptionUtils;

/**
 * Arguments used to create a <see cref="PointMapping"/>.
 */
public final class PointMappingCreationInfo {

    /**
     * Gets the point value being mapped.
     */
    private Object value;

    /**
     * Gets the Shard of the mapping.
     */
    private Shard shard;

    /**
     * Gets the Status of the mapping.
     */
    private MappingStatus status = MappingStatus.values()[0];

    /**
     * Gets the key value associated with the <see cref="PointMapping{KeyT}"/>.
     */
    private ShardKey key;

    /**
     * Arguments used to create a point mapping.
     *
     * @param point
     *            Point value being mapped.
     * @param shard
     *            Shard used as the mapping target.
     * @param status
     *            Status of the mapping.
     */
    public PointMappingCreationInfo(Object point,
            Shard shard,
            MappingStatus status) {
        ExceptionUtils.disallowNullArgument(shard, "shard");
        this.setValue(point);
        this.setShard(shard);
        this.setStatus(status);

        this.setKey(new ShardKey(ShardKey.shardKeyTypeFromType(point.getClass()), point));
    }

    public Object getValue() {
        return value;
    }

    private void setValue(Object value) {
        this.value = value;
    }

    public Shard getShard() {
        return shard;
    }

    private void setShard(Shard value) {
        shard = value;
    }

    public MappingStatus getStatus() {
        return status;
    }

    private void setStatus(MappingStatus value) {
        status = value;
    }

    public ShardKey getKey() {
        return key;
    }

    public void setKey(ShardKey value) {
        key = value;
    }
}
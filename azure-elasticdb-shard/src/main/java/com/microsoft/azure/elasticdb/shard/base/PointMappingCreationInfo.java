package com.microsoft.azure.elasticdb.shard.base;

// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

import com.microsoft.azure.elasticdb.shard.utils.ExceptionUtils;

/**
 * Arguments used to create a <see cref="PointMapping{TKey}"/>.
 * <p>
 * <typeparam name="TKey">Type of the key (point).</typeparam>
 */
public final class PointMappingCreationInfo<TKey> {
    /**
     * Gets the point value being mapped.
     */
    private Object Value;
    /**
     * Gets the Shard of the mapping.
     */
    private Shard Shard;
    /**
     * Gets the Status of the mapping.
     */
    private MappingStatus Status = MappingStatus.values()[0];
    /**
     * Gets the key value associated with the <see cref="PointMapping{TKey}"/>.
     */
    private ShardKey Key;

    /**
     * Arguments used to create a point mapping.
     *
     * @param point  Point value being mapped.
     * @param shard  Shard used as the mapping target.
     * @param status Status of the mapping.
     */
    public PointMappingCreationInfo(Object point, Shard shard, MappingStatus status) {
        ExceptionUtils.DisallowNullArgument(shard, "shard");
        this.setValue(point);
        this.setShard(shard);
        this.setStatus(status);

        this.setKey(new ShardKey(ShardKey.ShardKeyTypeFromType(point.getClass()), point));
    }

    public Object getValue() {
        return Value;
    }

    private void setValue(Object value) {
        Value = value;
    }

    public Shard getShard() {
        return Shard;
    }

    private void setShard(Shard value) {
        Shard = value;
    }

    public MappingStatus getStatus() {
        return Status;
    }

    private void setStatus(MappingStatus value) {
        Status = value;
    }

    public ShardKey getKey() {
        return Key;
    }

    public void setKey(ShardKey value) {
        Key = value;
    }
}
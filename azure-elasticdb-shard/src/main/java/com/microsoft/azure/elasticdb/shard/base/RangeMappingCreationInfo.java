package com.microsoft.azure.elasticdb.shard.base;

// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

import com.microsoft.azure.elasticdb.shard.utils.ExceptionUtils;

/**
 * Arguments used to create a <see cref="RangeMapping{TKey}"/>.
 * <p>
 * <typeparam name="TKey">Type of the key (boundary values).</typeparam>
 */
public final class RangeMappingCreationInfo<TKey> {
    /**
     * Gets Range being mapped.
     */
    private Range<TKey> Value;
    /**
     * Gets Shard for the mapping.
     */
    private Shard Shard;
    /**
     * Gets Status of the mapping.
     */
    private MappingStatus Status = MappingStatus.values()[0];
    /**
     * Gets Range associated with the <see cref="RangeMapping{TKey}"/>.
     */
    private ShardRange Range;

    /**
     * Arguments used for creation of a range mapping.
     *
     * @param value  Range being mapped.
     * @param shard  Shard used as the mapping target.
     * @param status Status of the mapping.
     */
    public RangeMappingCreationInfo(Range<TKey> value, Shard shard, MappingStatus status) {
        ExceptionUtils.DisallowNullArgument(value, "value");
        ExceptionUtils.DisallowNullArgument(shard, "shard");
        this.setValue(value);
        this.setShard(shard);
        this.setStatus(status);

        //TODO: this.setRange(new ShardRange(new ShardKey(ShardKey.ShardKeyTypeFromType(TKey.class), value.getLow()), new ShardKey(ShardKey.ShardKeyTypeFromType(TKey.class), value.getHighIsMax() ? null : (Object) value.getHigh())));
    }

    public Range<TKey> getValue() {
        return Value;
    }

    private void setValue(Range<TKey> value) {
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

    public ShardRange getRange() {
        return Range;
    }

    public void setRange(ShardRange value) {
        Range = value;
    }
}
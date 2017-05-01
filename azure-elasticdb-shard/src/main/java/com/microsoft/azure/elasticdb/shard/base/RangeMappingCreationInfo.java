package com.microsoft.azure.elasticdb.shard.base;

/* Copyright (c) Microsoft. All rights reserved.
Licensed under the MIT license. See LICENSE file in the project root for full license information.*/

import com.microsoft.azure.elasticdb.shard.utils.ExceptionUtils;

/**
 * Arguments used to create a <see cref="RangeMapping{KeyT}"/>.
 *
 * <typeparam name="KeyT">Type of the key (boundary values).</typeparam>
 */
public final class RangeMappingCreationInfo<KeyT> {

  /**
   * Gets Range being mapped.
   */
  private Range value;
  /**
   * Gets Shard for the mapping.
   */
  private Shard shard;
  /**
   * Gets Status of the mapping.
   */
  private MappingStatus status = MappingStatus.values()[0];
  /**
   * Gets Range associated with the <see cref="RangeMapping{KeyT}"/>.
   */
  private ShardRange range;

  /**
   * Arguments used for creation of a range mapping.
   *
   * @param value Range being mapped.
   * @param shard Shard used as the mapping target.
   * @param status Status of the mapping.
   */
  public RangeMappingCreationInfo(Range value, Shard shard, MappingStatus status) {
    ExceptionUtils.disallowNullArgument(value, "value");
    ExceptionUtils.disallowNullArgument(shard, "shard");
    this.setValue(value);
    this.setShard(shard);
    this.setStatus(status);
    ShardKey low = new ShardKey(value.getLow());
    ShardKey high =
        value.isHighMax() ? new ShardKey(low.getKeyType(), null) : new ShardKey(value.getHigh());
    this.setRange(new ShardRange(low, high));
  }

  public Range getValue() {
    return value;
  }

  private void setValue(Range value) {
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

  public ShardRange getRange() {
    return range;
  }

  public void setRange(ShardRange value) {
    range = value;
  }
}
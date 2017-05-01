package com.microsoft.azure.elasticdb.shard.base;

/* Copyright (c) Microsoft. All rights reserved.
Licensed under the MIT license. See LICENSE file in the project root for full license information.*/

/**
 * Represents a left-inclusive, right-exclusive range of values of type T.
 * <typeparam name="KeyT">Type of values.</typeparam>
 */
public final class Range {

  /**
   * The shard range value corresponding to this value.
   */
  private ShardRange shardRange;
  /**
   * Gets the low boundary value (inclusive).
   */
  private Object low;
  /**
   * Gets the high boundary value (exclusive).
   */
  private Object high;
  /**
   * True if the high boundary value equals +infinity; otherwise, false.
   */
  private boolean highIsMax;

  /**
   * Constructs range based on its low and high boundary values.
   *
   * @param low Low boundary value (inclusive).
   * @param high High boundary value (exclusive).
   */
  public Range(Object low, Object high) {
    ShardKeyType k = ShardKey.shardKeyTypeFromType(low.getClass());
    shardRange = new ShardRange(new ShardKey(k, low), new ShardKey(k, high));
    this.low = low;
    this.high = high;
  }

  /**
   * Constructs range based on its low boundary value. The low boundary value is
   * set to the one specified in <paramref name="low"/> while the
   * high boundary value is set to maximum possible value i.e. +infinity.
   *
   * @param low Low boundary value (inclusive).
   */
  public Range(Object low) {
    ShardKeyType k = ShardKey.shardKeyTypeFromType(low.getClass());
    shardRange = new ShardRange(new ShardKey(k, low), new ShardKey(k, null));
    this.low = low;
    this.setHighIsMax(true);
  }

  public ShardRange getShardRange() {
    return shardRange;
  }

  public Object getLow() {
    return low;
  }

  public Object getHigh() {
    return high;
  }

  public boolean isHighMax() {
    return highIsMax;
  }

  private void setHighIsMax(boolean value) {
    highIsMax = value;
  }

  /**
   * Converts the object to its string representation.
   *
   * @return String representation of the object.
   */
  @Override
  public String toString() {
    return shardRange.toString();
  }

  /**
   * Calculates the hash code for this instance.
   *
   * @return Hash code for the object.
   */
  @Override
  public int hashCode() {
    return shardRange.hashCode();
  }

  /**
   * Determines whether the specified object is equal to the current object.
   *
   * @param obj The object to compare with the current object.
   * @return True if the specified object is equal to the current object; otherwise, false.
   */
  @Override
  public boolean equals(Object obj) {
    return false; //TODO this.equals((Range<KeyT>)((obj instanceof Range<KeyT>) ? obj : null));
  }

  /**
   * Performs equality comparison with another Range.
   *
   * @param other Range to compare with.
   * @return True if same Range, false otherwise.
   */
  public boolean equals(Range other) {
    if (other == null) {
      return false;
    } else {
      return shardRange.equals(other.shardRange);
    }
  }
}

package com.microsoft.azure.elasticdb.shard.base;

/* Copyright (c) Microsoft. All rights reserved.
Licensed under the MIT license. See LICENSE file in the project root for full license information.*/

/**
 * Represents a left-inclusive, right-exclusive range of values of type T.
 * <typeparam name="TKey">Type of values.</typeparam>
 */
public final class Range {

  /**
   * The shard range value corresponding to this value.
   */
  private ShardRange _r;
  /**
   * Gets the low boundary value (inclusive).
   */
  private Object Low;
  /**
   * Gets the high boundary value (exclusive).
   */
  private Object High;
  /**
   * True if the high boundary value equals +infinity; otherwise, false.
   */
  private boolean HighIsMax;

  /**
   * Constructs range based on its low and high boundary values.
   *
   * @param low Low boundary value (inclusive).
   * @param high High boundary value (exclusive).
   */
  public Range(Object low, Object high) {
    ShardKeyType k = ShardKey.shardKeyTypeFromType(low.getClass());
    _r = new ShardRange(new ShardKey(k, low), new ShardKey(k, high));
    Low = low;
    High = high;
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
    _r = new ShardRange(new ShardKey(k, low), new ShardKey(k, null));
    Low = low;
    this.setHighIsMax(true);
  }

  public ShardRange getShardRange() {
    return _r;
  }

  public Object getLow() {
    return Low;
  }

  public Object getHigh() {
    return High;
  }

  public boolean isHighMax() {
    return HighIsMax;
  }

  private void setHighIsMax(boolean value) {
    HighIsMax = value;
  }

  /**
   * Converts the object to its string representation.
   *
   * @return String representation of the object.
   */
  @Override
  public String toString() {
    return _r.toString();
  }

  /**
   * Calculates the hash code for this instance.
   *
   * @return Hash code for the object.
   */
  @Override
  public int hashCode() {
    return _r.hashCode();
  }

  /**
   * Determines whether the specified object is equal to the current object.
   *
   * @param obj The object to compare with the current object.
   * @return True if the specified object is equal to the current object; otherwise, false.
   */
  @Override
  public boolean equals(Object obj) {
    return false; //TODO this.equals((Range<TKey>)((obj instanceof Range<TKey>) ? obj : null));
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
      return _r.equals(other._r);
    }
  }
}

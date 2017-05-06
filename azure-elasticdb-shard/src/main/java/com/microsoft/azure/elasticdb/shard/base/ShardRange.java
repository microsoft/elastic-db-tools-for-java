package com.microsoft.azure.elasticdb.shard.base;

/* Copyright (c) Microsoft. All rights reserved.
Licensed under the MIT license. See LICENSE file in the project root for full license information.*/

import com.microsoft.azure.elasticdb.shard.utils.Errors;
import com.microsoft.azure.elasticdb.shard.utils.ExceptionUtils;
import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlElement;

/**
 * A range of shard keys between a low key and a high key. The low key is inclusive (part of the
 * range) while the high key is exclusive (not part of the range). The ShardRange class is
 * immutable.
 */
@XmlAccessorType(XmlAccessType.NONE)
public final class ShardRange implements Comparable<ShardRange> {

  public static final ShardRange NULL = new ShardRange();

  /**
   * Full range that starts from the min value for a key to the max value.
   */
  private static ShardRange shardRangeInt32 = new ShardRange(ShardKey.getMinInt(),
      ShardKey.getMaxInt());

  /**
   * Full range that starts from the min value for a key to the max value.
   */
  private static ShardRange shardRangeInt64 = new ShardRange(ShardKey.getMinLong(),
      ShardKey.getMaxLong());

  /**
   * Full range that starts from the min value for a key to the max value.
   */
  private static ShardRange shardRangeGuid = new ShardRange(ShardKey.getMinGuid(),
      ShardKey.getMaxGuid());

  /**
   * Full range that starts from the min value for a key to the max value.
   */
  private static ShardRange shardRangeBinary = new ShardRange(ShardKey.getMinBinary(),
      ShardKey.getMaxBinary());

  /**
   * Full range that starts from the min value for a key to the max value.
   */
  private static ShardRange shardRangeDateTime = new ShardRange(ShardKey.getMinDateTime(),
      ShardKey.getMaxDateTime());

  /**
   * Full range that starts from the min value for a key to the max value.
   */
  private static ShardRange shardRangeTimeSpan = new ShardRange(ShardKey.getMinTimeSpan(),
      ShardKey.getMaxTimeSpan());

  /**
   * Full range that starts from the min value for a key to the max value.
   */
  private static ShardRange shardRangeDateTimeOffset = new ShardRange(
      ShardKey.getMinDateTimeOffset(), ShardKey.getMaxDateTimeOffset());

  /**
   * Hashcode for the shard range.
   */
  private int hashCode;

  /**
   * Accessor for low boundary (inclusive).
   */
  @XmlElement(name = "MinValue")
  private ShardKey low;

  /**
   * Accessor for high boundary (exclusive).
   */
  @XmlElement(name = "MaxValue")
  private ShardKey high;

  /**
   * Gets the key type of shard range.
   */
  private ShardKeyType keyType;

  @XmlAttribute(name = "Null")
  private int isNull;

  public ShardRange() {
    isNull = 1;
  }

  /**
   * Constructs a shard range from low boundary (inclusive) to high high boundary (exclusive).
   *
   * @param low low boundary (inclusive)
   * @param high high boundary (exclusive)
   */
  public ShardRange(ShardKey low, ShardKey high) {
    ExceptionUtils.disallowNullArgument(low, "low");
    ExceptionUtils.disallowNullArgument(high, "high");

    if (low.compareTo(high) > 0) {
      throw new IllegalArgumentException(String.format(
          Errors._ShardRange_LowGreaterThanOrEqualToHigh, low, high));
    }

    this.setLow(low);
    this.setHigh(high);
    this.setKeyType(getLow().getKeyType());
    hashCode = this.calculateHashCode();
    isNull = 0;
  }

  /**
   * Full range that starts from the min value for a key to the max value.
   */
  public static ShardRange getFullRangeInt32() {
    return shardRangeInt32;
  }

  /**
   * Full range that starts from the min value for a key to the max value.
   */
  public static ShardRange getFullRangeInt64() {
    return shardRangeInt64;
  }

  /**
   * Full range that starts from the min value for a key to the max value.
   */
  public static ShardRange getFullRangeGuid() {
    return shardRangeGuid;
  }

  /**
   * Full range that starts from the min value for a key to the max value.
   */
  public static ShardRange getFullRangeBinary() {
    return shardRangeBinary;
  }

  /**
   * Full range that starts from the min value for a key to the max value.
   */
  public static ShardRange getFullRangeDateTime() {
    return shardRangeDateTime;
  }

  /**
   * Full range that starts from the min value for a key to the max value.
   */
  public static ShardRange getFullRangeTimeSpan() {
    return shardRangeTimeSpan;
  }

  /**
   * Full range that starts from the min value for a key to the max value.
   */
  public static ShardRange getFullRangeDateTimeOffset() {
    return shardRangeDateTimeOffset;
  }

  /**
   * Compares two <see cref="ShardRange"/> using lexicographic order (less than).
   *
   * @param left Left hand side <see cref="ShardRange"/> of the operator.
   * @param right Right hand side <see cref="ShardRange"/> of the operator.
   * @return True if lhs &lt; rhs
   */
  public static boolean opLessThan(ShardRange left, ShardRange right) {
    if (left == null) {
      return right != null;
    } else {
      return (left.compareTo(right) < 0);
    }
  }

  /**
   * Compares two <see cref="ShardRange"/> using lexicographic order (greater than).
   *
   * @param left Left hand side <see cref="ShardRange"/> of the operator.
   * @param right Right hand side <see cref="ShardRange"/> of the operator.
   * @return True if lhs &gt; rhs
   */
  public static boolean opGreaterThan(ShardRange left, ShardRange right) {
    return opLessThan(right, left);
  }

  /**
   * Compares two <see cref="ShardRange"/> using lexicographic order (less or equal).
   *
   * @param left Left hand side <see cref="ShardRange"/> of the operator.
   * @param right Right hand side <see cref="ShardRange"/> of the operator.
   * @return True if lhs &lt;= rhs
   */
  public static boolean opLessThanOrEqual(ShardRange left, ShardRange right) {
    return !opGreaterThan(left, right);
  }

  /**
   * Compares two <see cref="ShardRange"/> using lexicographic order (greater or equal).
   *
   * @param left Left hand side <see cref="ShardRange"/> of the operator.
   * @param right Right hand side <see cref="ShardRange"/> of the operator.
   * @return True if lhs &gt;= rhs
   */
  public static boolean opGreaterThanOrEqual(ShardRange left, ShardRange right) {
    return !opLessThan(left, right);
  }

  /**
   * Equality operator.
   *
   * @param left Left hand side
   * @param right Right hand side
   * @return True if the two objects are equal, false in all other cases
   */
  public static boolean opEquality(ShardRange left, ShardRange right) {
    return left.equals(right);
  }

  /**
   * Inequality operator.
   *
   * @param left Left hand side
   * @param right Right hand side
   * @return True if the two objects are not equal, false in all other cases
   */
  public static boolean opInequality(ShardRange left, ShardRange right) {
    return !opEquality(left, right);
  }

  /**
   * Gets a shard range corresponding to a specified key type.
   *
   * @param keyType Type of key.
   * @return Full range for given key type.
   */
  public static ShardRange getFullRange(ShardKeyType keyType) {
    assert keyType != ShardKeyType.None;

    switch (keyType) {
      case Int32:
        return ShardRange.getFullRangeInt32();
      case Int64:
        return ShardRange.getFullRangeInt64();
      case Guid:
        return ShardRange.getFullRangeGuid();
      case Binary:
        return ShardRange.getFullRangeBinary();
      case DateTime:
        return ShardRange.getFullRangeDateTime();
      case TimeSpan:
        return ShardRange.getFullRangeTimeSpan();
      case DateTimeOffset:
        return ShardRange.getFullRangeDateTimeOffset();
      default:
        //Debug.Fail("Unexpected ShardKeyType.");
        return null;
    }
  }

  public ShardKey getLow() {
    return low;
  }

  private void setLow(ShardKey value) {
    low = value;
  }

  public ShardKey getHigh() {
    return high;
  }

  private void setHigh(ShardKey value) {
    high = value;
  }

  public ShardKeyType getKeyType() {
    return keyType;
  }

  private void setKeyType(ShardKeyType value) {
    keyType = value;
  }

  /**
   * Converts the object to its string representation.
   *
   * @return String representation of the object.
   */
  @Override
  public String toString() {
    return String.format("[%s:%s]", this.getLow().toString(), this.getHigh().toString());
  }

  /**
   * Calculates the hash code for this instance.
   *
   * @return Hash code for the object.
   */
  @Override
  public int hashCode() {
    return hashCode;
  }

  /**
   * Determines whether the specified object is equal to the current object.
   *
   * @param obj The object to compare with the current object.
   * @return True if the specified object is equal to the current object; otherwise, false.
   */
  @Override
  public boolean equals(Object obj) {
    return this.equals((ShardRange) ((obj instanceof ShardRange) ? obj : null));
  }

  /**
   * Performs equality comparison with another given ShardRange.
   *
   * @param other ShardRange to compare with.
   * @return True if same shard range, false otherwise.
   */
  public boolean equals(ShardRange other) {
    return other != null && this.hashCode() == other.hashCode() && this.compareTo(other) == 0;
  }

  /**
   * Checks whether the specified key is inside the range.
   *
   * @param key The key to check
   * @return True if inside, false otherwise
   */
  public boolean contains(ShardKey key) {
    ExceptionUtils.disallowNullArgument(key, "key");

    return (key.compareTo(getLow()) >= 0 && key.compareTo(getHigh()) < 0);
  }

  /**
   * Checks whether the range is inside the range.
   *
   * @param range The range to check.
   * @return True if inside, false otherwise.
   */
  public boolean contains(ShardRange range) {
    ExceptionUtils.disallowNullArgument(range, "range");

    return ShardKey.opGreaterThanOrEqual(range.getLow(), getLow())
        && ShardKey.opLessThanOrEqual(range.getHigh(), getHigh());
  }

  /**
   * Performs comparison between two shard range values.
   *
   * @param other The shard range compared with this object.
   * @return -1 : if this range's low boundary is less than the <paramref name="other"/>'s low
   * boundary; -1 : if the low boundary values match and the high boundary value of this range is
   * less than the <paramref name="other"/>'s. 1 : if this range's high boundary is greater than the
   * <paramref name="other"/>'s high boundary; 1 : if the low boundary value of this range is higher
   * than <paramref name="other"/>'s low boundary and high boundary value of this range is less than
   * or equal to <paramref name="other"/>'s high boundary . 0 : if this range has the same
   * boundaries as <paramref name="other"/>.
   */
  public int compareTo(ShardRange other) {
    ExceptionUtils.disallowNullArgument(other, "other");

    if (ShardKey.opLessThan(this.getLow(), other.getLow())) {
      return -1;
    }

    if (ShardKey.opGreaterThan(this.getHigh(), other.getHigh())) {
      return 1;
    }

    if (ShardKey.opEquality(this.getLow(), other.getLow())) {
      if (ShardKey.opEquality(this.getHigh(), other.getHigh())) {
        return 0;
      } else {
        return -1;
      }
    } else {
      assert ShardKey.opGreaterThan(this.getLow(), other.getLow());
      assert ShardKey.opLessThanOrEqual(this.getHigh(), other.getHigh());
      return 1;
    }
  }

  /**
   * Checks whether the range intersects with the current range.
   *
   * @param range The range to check.
   * @return True if it intersects, False otherwise.
   */
  public boolean intersects(ShardRange range) {
    ExceptionUtils.disallowNullArgument(range, "range");

    return ShardKey.opGreaterThan(range.getHigh(), getLow())
        && ShardKey.opLessThan(range.getLow(), getHigh());
  }

  /**
   * Returns the intersection of two ranges.
   *
   * @param range Range to intersect with.
   * @return The intersection of the current range and the specified range, null if ranges dont
   * intersect.
   */
  public ShardRange intersect(ShardRange range) {
    ExceptionUtils.disallowNullArgument(range, "range");

    ShardKey intersectLow = ShardKey.max(getLow(), range.getLow());
    ShardKey intersectHigh = ShardKey.min(getHigh(), range.getHigh());

    if (ShardKey.opGreaterThanOrEqual(intersectLow, intersectHigh)) {
      return null;
    }

    return new ShardRange(intersectLow, intersectHigh);
  }

  /**
   * Calculates the hash code for the object.
   *
   * @return Hash code for the object.
   */
  private int calculateHashCode() {
    return ShardKey.qpHash(this.getLow().hashCode(), this.getHigh().hashCode());
  }
}

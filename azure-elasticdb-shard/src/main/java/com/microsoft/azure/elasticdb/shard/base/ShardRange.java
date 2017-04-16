package com.microsoft.azure.elasticdb.shard.base;

// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

import com.microsoft.azure.elasticdb.shard.utils.ExceptionUtils;
import com.microsoft.azure.elasticdb.shard.utils.StringUtilsLocal;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlElement;

/**
 * A range of shard keys between a low key and a high key.
 * <p>
 * <p>
 * The low key is inclusive (part of the range) while the high key is exclusive
 * (not part of the range). The ShardRange class is immutable.
 */
@XmlAccessorType(XmlAccessType.NONE)
public final class ShardRange implements Comparable<ShardRange> {
    public static final ShardRange NULL = new ShardRange();
    /**
     * Full range that starts from the min value for a key to the max value.
     */
    private static ShardRange s_fullRangeInt32 = new ShardRange(ShardKey.getMinInt(), ShardKey.getMaxInt());
    /**
     * Full range that starts from the min value for a key to the max value.
     */
    private static ShardRange s_fullRangeInt64 = new ShardRange(ShardKey.getMinLong(), ShardKey.getMaxLong());
    /**
     * Full range that starts from the min value for a key to the max value.
     */
    private static ShardRange s_fullRangeGuid = new ShardRange(ShardKey.getMinGuid(), ShardKey.getMaxGuid());
    /**
     * Full range that starts from the min value for a key to the max value.
     */
    private static ShardRange s_fullRangeBinary = new ShardRange(ShardKey.getMinBinary(), ShardKey.getMaxBinary());
    /**
     * Full range that starts from the min value for a key to the max value.
     */
    private static ShardRange s_fullRangeDateTime = new ShardRange(ShardKey.getMinDateTime(), ShardKey.getMaxDateTime());
    /**
     * Full range that starts from the min value for a key to the max value.
     */
    private static ShardRange s_fullRangeTimeSpan = new ShardRange(ShardKey.getMinTimeSpan(), ShardKey.getMaxTimeSpan());
    /**
     * Full range that starts from the min value for a key to the max value.
     */
    private static ShardRange s_fullRangeDateTimeOffset = new ShardRange(ShardKey.getMinDateTimeOffset(), ShardKey.getMaxDateTimeOffset());
    /**
     * Hashcode for the shard range.
     */
    private int _hashCode;
    /**
     * Accessor for low boundary (inclusive).
     */
    @XmlElement(name = "MinValue")
    private ShardKey Low;
    /**
     * Accessor for high boundary (exclusive).
     */
    @XmlElement(name = "MaxValue")
    private ShardKey High;
    /**
     * Gets the key type of shard range.
     */
    private ShardKeyType KeyType;

    @XmlAttribute(name = "Null")
    private int isNull;

    public ShardRange() {
        isNull = 1;
    }

    /**
     * Constructs a shard range from low boundary (inclusive) to high high boundary (exclusive)
     *
     * @param low  Low boundary (inclusive)
     * @param high High boundary (exclusive)
     */
    public ShardRange(ShardKey low, ShardKey high) {
        ExceptionUtils.DisallowNullArgument(low, "low");
        ExceptionUtils.DisallowNullArgument(high, "high");

        if (low.compareTo(high) > 0) {
            //throw new IllegalArgumentException(String.format(Errors._ShardRange_LowGreaterThanOrEqualToHigh, low, high));
        }

        this.setLow(low);
        this.setHigh(high);
        this.setKeyType(getLow().getKeyType());
        _hashCode = this.CalculateHashCode();
        isNull = 0;
    }

    /**
     * Full range that starts from the min value for a key to the max value.
     */
    public static ShardRange getFullRangeInt32() {
        return s_fullRangeInt32;
    }

    /**
     * Full range that starts from the min value for a key to the max value.
     */
    public static ShardRange getFullRangeInt64() {
        return s_fullRangeInt64;
    }

    /**
     * Full range that starts from the min value for a key to the max value.
     */
    public static ShardRange getFullRangeGuid() {
        return s_fullRangeGuid;
    }

    /**
     * Full range that starts from the min value for a key to the max value.
     */
    public static ShardRange getFullRangeBinary() {
        return s_fullRangeBinary;
    }

    /**
     * Full range that starts from the min value for a key to the max value.
     */
    public static ShardRange getFullRangeDateTime() {
        return s_fullRangeDateTime;
    }

    /**
     * Full range that starts from the min value for a key to the max value.
     */
    public static ShardRange getFullRangeTimeSpan() {
        return s_fullRangeTimeSpan;
    }

    /**
     * Full range that starts from the min value for a key to the max value.
     */
    public static ShardRange getFullRangeDateTimeOffset() {
        return s_fullRangeDateTimeOffset;
    }

    /**
     * Compares two <see cref="ShardRange"/> using lexicographic order (less than).
     *
     * @param left  Left hand side <see cref="ShardRange"/> of the operator.
     * @param right Right hand side <see cref="ShardRange"/> of the operator.
     * @return True if lhs &lt; rhs
     */
    public static boolean OpLessThan(ShardRange left, ShardRange right) {
        if (left == null) {
            return (right == null) ? false : true;
        } else {
            return (left.compareTo(right) < 0);
        }
    }

    /**
     * Compares two <see cref="ShardRange"/> using lexicographic order (greater than).
     *
     * @param left  Left hand side <see cref="ShardRange"/> of the operator.
     * @param right Right hand side <see cref="ShardRange"/> of the operator.
     * @return True if lhs &gt; rhs
     */
    public static boolean OpGreaterThan(ShardRange left, ShardRange right) {
        return OpLessThan(right, left);
    }

    /**
     * Compares two <see cref="ShardRange"/> using lexicographic order (less or equal).
     *
     * @param left  Left hand side <see cref="ShardRange"/> of the operator.
     * @param right Right hand side <see cref="ShardRange"/> of the operator.
     * @return True if lhs &lt;= rhs
     */
    public static boolean OpLessThanOrEqual(ShardRange left, ShardRange right) {
        return !OpGreaterThan(left, right);
    }

    /**
     * Compares two <see cref="ShardRange"/> using lexicographic order (greater or equal).
     *
     * @param left  Left hand side <see cref="ShardRange"/> of the operator.
     * @param right Right hand side <see cref="ShardRange"/> of the operator.
     * @return True if lhs &gt;= rhs
     */
    public static boolean OpGreaterThanOrEqual(ShardRange left, ShardRange right) {
        return !OpLessThan(left, right);
    }

    /**
     * Equality operator.
     *
     * @param left  Left hand side
     * @param right Right hand side
     * @return True if the two objects are equal, false in all other cases
     */
    public static boolean OpEquality(ShardRange left, ShardRange right) {
        return left.equals(right);
    }

    /**
     * Inequality operator.
     *
     * @param left  Left hand side
     * @param right Right hand side
     * @return True if the two objects are not equal, false in all other cases
     */
    public static boolean OpInequality(ShardRange left, ShardRange right) {
        return !OpEquality(left, right);
    }

    /**
     * Gets a shard range corresponding to a specified key type.
     *
     * @param keyType Type of key.
     * @return Full range for given key type.
     */
    public static ShardRange GetFullRange(ShardKeyType keyType) {
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
        return Low;
    }

    private void setLow(ShardKey value) {
        Low = value;
    }

    public ShardKey getHigh() {
        return High;
    }

    private void setHigh(ShardKey value) {
        High = value;
    }

    public ShardKeyType getKeyType() {
        return KeyType;
    }

    private void setKeyType(ShardKeyType value) {
        KeyType = value;
    }

    /**
     * Converts the object to its string representation.
     *
     * @return String representation of the object.
     */
    @Override
    public String toString() {
        return StringUtilsLocal.FormatInvariant("[%s:%s)", this.getLow().toString(), this.getHigh().toString());
    }

    /**
     * Calculates the hash code for this instance.
     *
     * @return Hash code for the object.
     */
    @Override
    public int hashCode() {
        return _hashCode;
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
        if (other == null) {
            return false;
        } else {
            if (this.hashCode() != other.hashCode()) {
                return false;
            } else {
                return this.compareTo(other) == 0;
            }
        }
    }

    /**
     * Checks whether the specified key is inside the range.
     *
     * @param key The key to check
     * @return True if inside, false otherwise
     */
    public boolean Contains(ShardKey key) {
        ExceptionUtils.DisallowNullArgument(key, "key");

        return (key.compareTo(getLow()) >= 0 && key.compareTo(getHigh()) < 0);
    }

    /**
     * Checks whether the range is inside the range.
     *
     * @param range The range to check.
     * @return True if inside, false otherwise.
     */
    public boolean Contains(ShardRange range) {
        ExceptionUtils.DisallowNullArgument(range, "range");

        return false; //TODO (range.getLow() >= getLow()) && (range.getHigh() <= getHigh());
    }

    /**
     * Performs comparison between two shard range values.
     *
     * @param other The shard range compared with this object.
     * @return -1 : if this range's low boundary is less than the <paramref name="other"/>'s low boundary;
     * -1 : if the low boundary values match and the high boundary value of this range is less than the <paramref name="other"/>'s.
     * 1 : if this range's high boundary is greater than the <paramref name="other"/>'s high boundary;
     * 1 : if the low boundary value of this range is higher than <paramref name="other"/>'s low boundary and high boundary value of this range is less than or equal to <paramref name="other"/>'s high boundary .
     * 0 : if this range has the same boundaries as <paramref name="other"/>.
     */
    public int compareTo(ShardRange other) {
        ExceptionUtils.DisallowNullArgument(other, "other");

        /*if (this.getLow() < other.getLow()) {
            return -1;
        }

        if (this.getHigh() > other.getHigh()) {
            return 1;
        }

        if (this.getLow() == other.getLow()) {
            if (this.getHigh() == other.getHigh()) {
                return 0;
            } else {
                return -1;
            }
        } else {
            assert this.getLow() > other.getLow();
            assert this.getHigh() <= other.getHigh();
            return 1;
        }*/
        return 0; //TODO
    }

    /**
     * Checks whether the range intersects with the current range.
     *
     * @param range The range to check.
     * @return True if it intersects, False otherwise.
     */
    public boolean Intersects(ShardRange range) {
        ExceptionUtils.DisallowNullArgument(range, "range");

        return false; //TODO (range.getHigh() > getLow()) && (range.getLow() < getHigh());
    }

    /**
     * Returns the intersection of two ranges.
     *
     * @param range Range to intersect with.
     * @return The intersection of the current range and the specified range, null if ranges dont intersect.
     */
    public ShardRange Intersect(ShardRange range) {
        ExceptionUtils.DisallowNullArgument(range, "range");

        ShardKey intersectLow = ShardKey.Max(getLow(), range.getLow());
        ShardKey intersectHigh = ShardKey.Min(getHigh(), range.getHigh());

        //TODO
        /*if (intersectLow >= intersectHigh) {
            return null;
        }*/

        return new ShardRange(intersectLow, intersectHigh);
    }

    /**
     * Calculates the hash code for the object.
     *
     * @return Hash code for the object.
     */
    private int CalculateHashCode() {
        return ShardKey.QPHash(this.getLow().hashCode(), this.getHigh().hashCode());
    }
}

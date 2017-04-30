package com.microsoft.azure.elasticdb.shard.base;

/* Copyright (c) Microsoft. All rights reserved.
Licensed under the MIT license. See LICENSE file in the project root for full license information.*/

import com.microsoft.azure.elasticdb.shard.utils.Errors;
import com.microsoft.azure.elasticdb.shard.utils.ExceptionUtils;
import com.microsoft.azure.elasticdb.shard.utils.StringUtilsLocal;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.temporal.ChronoField;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Objects;
import java.util.UUID;
import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;

/**
 * Shard key value. Wraps the type and value and allows normalization/denormalization
 * for serialization.
 */
@XmlAccessorType(XmlAccessType.NONE)
public final class ShardKey implements Comparable<ShardKey> {

  /**
   * Size of Guid.
   */
  public static final int SIZE_OF_GUID = 16;

  /**
   * Size of Guid.
   */
  public static final int SIZE_OF_DATE_TIME_OFFSET = 16;

  /**
   * Maximum size allowed for VarBytes keys.
   */
  public static final int MAXIMUM_VAR_BYTES_KEY_SIZE = 128;

  ///#region Private Variables

  private static final long TICKS_AT_EPOCH = 621355968000000000L;

  private static final long TICKS_PER_MILLISECOND = 10000;

  /**
   * String representation of +ve infinity.
   */
  private static final String POSITIVE_INFINITY = "+inf";

  /**
   * An empty array.
   */
  private static final byte[] S_EMPTY_ARRAY = new byte[0];

  /**
   * Mapping b/w CLR type and corresponding ShardKeyType.
   */
  private static final HashMap<Class, ShardKeyType> CLASS_SHARD_KEY_TYPE_HASH_MAP =
      new HashMap<Class, ShardKeyType>() {
        {
          put(Integer.class, ShardKeyType.Int32);
          put(Long.class, ShardKeyType.Int64);
          put(UUID.class, ShardKeyType.Guid);
          put(byte[].class, ShardKeyType.Binary);
          put(LocalDateTime.class, ShardKeyType.DateTime);
          put(Duration.class, ShardKeyType.TimeSpan);
          put(OffsetDateTime.class, ShardKeyType.DateTimeOffset);
        }
      };

  /**
   * Mapping b/w ShardKeyType and corresponding CLR type.
   */
  private static final HashMap<ShardKeyType, Class> SHARD_KEY_TYPE_CLASS_HASH_MAP =
      new HashMap<ShardKeyType, Class>() {
        {
          put(ShardKeyType.Int32, Integer.class);
          put(ShardKeyType.Int64, Long.class);
          put(ShardKeyType.Guid, UUID.class);
          put(ShardKeyType.Binary, byte[].class);
          put(ShardKeyType.DateTime, LocalDateTime.class);
          put(ShardKeyType.TimeSpan, Duration.class);
          put(ShardKeyType.DateTimeOffset, OffsetDateTime.class);
        }
      };

  /**
   * Represents negative infinity.
   */
  private static ShardKey sMinInt32 = new ShardKey(ShardKeyType.Int32, Integer.MIN_VALUE);

  /**
   * Represents negative infinity.
   */
  private static ShardKey sMaxInt32 = new ShardKey(ShardKeyType.Int32, null);

  /**
   * Represents negative infinity.
   */
  private static ShardKey sMinInt64 = new ShardKey(ShardKeyType.Int64, Long.MIN_VALUE);

  /**
   * Represents negative infinity.
   */
  private static ShardKey sMaxInt64 = new ShardKey(ShardKeyType.Int64, null);

  /**
   * Represents negative infinity.
   */
  private static ShardKey sMinGuid = new ShardKey(ShardKeyType.Guid, null);

  /**
   * Represents negative infinity.
   */
  private static ShardKey sMaxGuid = new ShardKey(ShardKeyType.Guid, null);

  /**
   * Represents negative infinity.
   */
  private static ShardKey sMinBinary = new ShardKey(ShardKeyType.Binary, ShardKey.S_EMPTY_ARRAY);

  /**
   * Represents negative infinity.
   */
  private static ShardKey sMaxBinary = new ShardKey(ShardKeyType.Binary, null);

  /**
   * Represents negative infinity.
   */
  private static ShardKey sMinDateTime = new ShardKey(ShardKeyType.DateTime, LocalDateTime.MIN);

  /**
   * Represents negative infinity.
   */
  private static ShardKey sMaxDateTime = new ShardKey(ShardKeyType.DateTime, null);

  /**
   * Represents negative infinity.
   */
  private static ShardKey sMinTimeSpan = new ShardKey(ShardKeyType.TimeSpan, Duration.ZERO);

  /**
   * Represents negative infinity.
   */
  private static ShardKey sMaxTimeSpan = new ShardKey(ShardKeyType.TimeSpan, null);

  /**
   * Represents negative infinity.
   */
  private static ShardKey sMinDateTimeOffset = new ShardKey(ShardKeyType.DateTimeOffset, null);

  /**
   * Represents negative infinity.
   */
  private static ShardKey sMaxDateTimeOffset = new ShardKey(ShardKeyType.DateTimeOffset, null);

  /**
   * Type of shard key.
   */
  private ShardKeyType keyType;

  /**
   * Value as saved in persistent storage. Empty byte array represents the minimum value,
   * and a null value represents the maximum value.
   */
  private byte[] value;

  /**
   * Hashcode for the shard key.
   */
  private int hashCode;

  ///#endregion

  ///#region Constructors

  public ShardKey() {
  }

  /**
   * Constructs a shard key using 32-bit integer value.
   *
   * @param value Input 32-bit integer.
   */
  public ShardKey(int value) {
    this(ShardKeyType.Int32, ShardKey.normalize(value), false);
  }

  /**
   * Constructs a shard key using 64-bit integer value.
   *
   * @param value Input 64-bit integer.
   */
  public ShardKey(long value) {
    this(ShardKeyType.Int64, ShardKey.normalize(value), false);
  }

  /**
   * Constructs a shard key using a Guid.
   *
   * @param value Input Guid.
   */
  public ShardKey(UUID value) {
    this(ShardKeyType.Guid, ShardKey.normalize(value), false);
  }

  /**
   * Constructs a shard key using a byte array.
   *
   * @param value Input byte array.
   */
  public ShardKey(byte[] value) {
    this(ShardKeyType.Binary, ShardKey.normalize(value), true);
  }

  /**
   * Constructs a shard key using DateTime value.
   *
   * @param value Input DateTime.
   */
  public ShardKey(LocalDateTime value) {
    this(ShardKeyType.DateTime, ShardKey.normalize(value), false);
  }

  /**
   * Constructs a shard key using TimeSpan value.
   *
   * @param value Input TimeSpan.
   */
  public ShardKey(Duration value) {
    this(ShardKeyType.TimeSpan, ShardKey.normalize(value), false);
  }

  /**
   * Constructs a shard key using TimeSpan value.
   *
   * @param value Input DateTimeOffset.
   */
  public ShardKey(OffsetDateTime value) {
    this(ShardKeyType.DateTimeOffset, ShardKey.normalize(value), false);
  }

  /**
   * Constructs a shard key using given object.
   *
   * @param value Input object.
   */
  public ShardKey(Object value) {
    ExceptionUtils.DisallowNullArgument(value, "value");
    ShardKey shardKey = (ShardKey) ((value instanceof ShardKey) ? value : null);

    if (shardKey != null) {
      keyType = shardKey.keyType;
      this.value = shardKey.value;
    } else {
      keyType = ShardKey.detectShardKeyType(value);
      this.value = ShardKey.normalize(keyType, value);
    }

    hashCode = Objects.hash(keyType, this.value);
  }

  /**
   * Constructs a shard key using given object and keyType.
   *
   * @param keyType The key type of value in object.
   * @param value Input object.
   */
  public ShardKey(ShardKeyType keyType, Object value) {
    if (keyType == ShardKeyType.None) {
      throw new IllegalArgumentException(Errors._ShardKey_UnsupportedShardKeyType);
    }

    this.keyType = keyType;

    if (value != null) {
      ShardKeyType detectedKeyType = ShardKey.detectShardKeyType(value);

      if (this.keyType != detectedKeyType) {
        throw new IllegalArgumentException(
            String.format(Errors._ShardKey_ValueDoesNotMatchShardKeyType, this.keyType));
      }

      this.value = ShardKey.normalize(this.keyType, value);
    } else {
      // Null represents positive infinity.
      this.value = null;
    }

    hashCode = Objects.hash(keyType, value);
  }

  /**
   * Instantiates the key with given type and raw value and optionally validates
   * the key type and raw representation of the value.
   *
   * @param keyType Type of shard key.
   * @param rawValue Raw value of the key.
   * @param validate Whether to validate the key type and raw value.
   */
  private ShardKey(ShardKeyType keyType, byte[] rawValue, boolean validate) {
    this.keyType = keyType;
    this.value = rawValue;
    hashCode = Objects.hash(keyType, rawValue);

    if (validate) {

      // +ve & -ve infinity. Correct size provided.
      if (this.value == null || this.value.length == 0 || this.value.length == this.keyType
          .getByteArraySize()) {
        return;
      }

      // Only allow byte[] values to be of different length than expected,
      // since there could be smaller values than 128 bytes. For anything
      // else any non-zero length should match the expected length.
      if (this.keyType != ShardKeyType.Binary || this.value.length > this.keyType
          .getByteArraySize()) {
        throw new IllegalArgumentException(String
            .format(Errors._ShardKey_ValueLengthUnexpected, this.value.length,
                this.keyType.getByteArraySize(), this.keyType));
      }
    }
  }

  ///#endregion

  ///#region Static Getters

  /**
   * Represents negative infinity.
   */
  public static ShardKey getMinInt() {
    return sMinInt32;
  }

  /**
   * Represents negative infinity.
   */
  public static ShardKey getMaxInt() {
    return sMaxInt32;
  }

  /**
   * Represents negative infinity.
   */
  public static ShardKey getMinLong() {
    return sMinInt64;
  }

  /**
   * Represents negative infinity.
   */
  public static ShardKey getMaxLong() {
    return sMaxInt64;
  }

  /**
   * Represents negative infinity.
   */
  public static ShardKey getMinGuid() {
    return sMinGuid;
  }

  /**
   * Represents negative infinity.
   */
  public static ShardKey getMaxGuid() {
    return sMaxGuid;
  }

  /**
   * Represents negative infinity.
   */
  public static ShardKey getMinBinary() {
    return sMinBinary;
  }

  /**
   * Represents negative infinity.
   */
  public static ShardKey getMaxBinary() {
    return sMaxBinary;
  }

  /**
   * Represents negative infinity.
   */
  public static ShardKey getMinDateTime() {
    return sMinDateTime;
  }

  /**
   * Represents negative infinity.
   */
  public static ShardKey getMaxDateTime() {
    return sMaxDateTime;
  }

  /**
   * Represents negative infinity.
   */
  public static ShardKey getMinTimeSpan() {
    return sMinTimeSpan;
  }

  /**
   * Represents negative infinity.
   */
  public static ShardKey getMaxTimeSpan() {
    return sMaxTimeSpan;
  }

  /**
   * Represents negative infinity.
   */
  public static ShardKey getMinDateTimeOffset() {
    return sMinDateTimeOffset;
  }

  /**
   * Represents negative infinity.
   */
  public static ShardKey getMaxDateTimeOffset() {
    return sMaxDateTimeOffset;
  }

  ///#endregion

  ///#region Operators

  /**
   * Compares two <see cref="ShardKey"/> using lexicographic order (less than).
   *
   * @param left Left hand side <see cref="ShardKey"/> of the operator.
   * @param right Right hand side <see cref="ShardKey"/> of the operator.
   * @return True if lhs &lt; rhs
   */
  public static boolean opLessThan(ShardKey left, ShardKey right) {
    if (left == null) {
      return right != null;
    } else {
      return (left.compareTo(right) < 0);
    }
  }

  /**
   * Compares two <see cref="ShardKey"/> using lexicographic order (greater than).
   *
   * @param left Left hand side <see cref="ShardKey"/> of the operator.
   * @param right Right hand side <see cref="ShardKey"/> of the operator.
   * @return True if lhs &gt; rhs
   */
  public static boolean opGreaterThan(ShardKey left, ShardKey right) {
    return opLessThan(right, left);
  }

  /**
   * Compares two <see cref="ShardKey"/> using lexicographic order (less or equal).
   *
   * @param left Left hand side <see cref="ShardKey"/> of the operator.
   * @param right Right hand side <see cref="ShardKey"/> of the operator.
   * @return True if lhs &lt;= rhs
   */
  public static boolean opLessThanOrEqual(ShardKey left, ShardKey right) {
    return !opGreaterThan(left, right);
  }

  /**
   * Compares two <see cref="ShardKey"/> using lexicographic order (greater or equal).
   *
   * @param left Left hand side <see cref="ShardKey"/> of the operator.
   * @param right Right hand side <see cref="ShardKey"/> of the operator.
   * @return True if lhs &gt;= rhs
   */
  public static boolean opGreaterThanOrEqual(ShardKey left, ShardKey right) {
    return !opLessThan(left, right);
  }

  /**
   * Equality operator.
   *
   * @param left Left hand side
   * @param right Right hand side
   * @return True if the two objects are equal, false in all other cases
   */
  public static boolean opEquality(ShardKey left, ShardKey right) {
    return left.equals(right);
  }

  /**
   * Inequality operator.
   *
   * @param left Left hand side
   * @param right Right hand side
   * @return True if the two objects are not equal, false in all other cases
   */
  public static boolean opInequality(ShardKey left, ShardKey right) {
    return !opEquality(left, right);
  }

  /**
   * Gets the minimum of two shard keys.
   *
   * @param left Left hand side.
   * @param right Right hand side.
   * @return Minimum of two shard keys.
   */
  public static ShardKey min(ShardKey left, ShardKey right) {
    if (opLessThan(left, right)) {
      return left;
    } else {
      return right;
    }
  }

  /**
   * Gets the maximum of two shard keys.
   *
   * @param left Left hand side.
   * @param right Right hand side.
   * @return Maximum of two shard keys.
   */
  public static ShardKey max(ShardKey left, ShardKey right) {
    if (opGreaterThan(left, right)) {
      return left;
    } else {
      return right;
    }
  }

  ///#endregion

  ///#region Normalize

  /**
   * Take an object and convert it to its normalized representation as a byte array.
   *
   * @param keyType The type of the <see cref="ShardKey"/>.
   * @param value The value
   * @return The normalized <see cref="ShardKey"/> information
   */
  private static byte[] normalize(ShardKeyType keyType, Object value) {
    switch (keyType) {
      case Int32:
        return ShardKey.normalize((Integer) value);

      case Int64:
        return ShardKey.normalize((Long) value);

      case Guid:
        return ShardKey.normalize((UUID) value);

      case DateTime:
        return ShardKey.normalize((LocalDateTime) value);

      case TimeSpan:
        return ShardKey.normalize((Duration) value);

      case DateTimeOffset:
        return ShardKey.normalize((OffsetDateTime) value);

      default:
        assert keyType == ShardKeyType.Binary;
        return ShardKey.normalize((byte[]) value);
    }
  }

  /**
   * Converts given 32-bit integer to normalized binary representation.
   *
   * @param value Input 32-bit integer.
   * @return Normalized array of bytes.
   */
  private static byte[] normalize(int value) {
    if (value == Integer.MIN_VALUE) {
      return ShardKey.S_EMPTY_ARRAY;
    } else {
      byte[] retValue = ByteBuffer.allocate(ShardKeyType.Int32.getByteArraySize()).putInt(value)
          .array();
      retValue[0] ^= 0x80;
      return retValue;
    }
  }

  /**
   * Converts given 64-bit integer to normalized binary representation.
   *
   * @param value Input 64-bit integer.
   * @return Normalized array of bytes.
   */
  private static byte[] normalize(long value) {
    if (value == Long.MIN_VALUE) {
      return ShardKey.S_EMPTY_ARRAY;
    } else {
      byte[] retValue = ByteBuffer.allocate(ShardKeyType.Int64.getByteArraySize()).putLong(value)
          .array();
      retValue[0] ^= 0x80;
      return retValue;
    }
  }

  /**
   * Converts given GUID to normalized binary representation.
   *
   * @param value Input GUID.
   * @return Normalized array of bytes.
   */
  private static byte[] normalize(UUID value) {
    if (value == null || value.equals(new UUID(0L, 0L))) {
      return ShardKey.S_EMPTY_ARRAY;
    } else {
      ByteBuffer bb = ByteBuffer.wrap(new byte[16]);
      bb.putLong(value.getMostSignificantBits());
      bb.putLong(value.getLeastSignificantBits());
      byte[] source = bb.array();

      // For normalization follow the pattern of SQL Server comparison.
      byte[] normalized = new byte[ShardKey.SIZE_OF_GUID];

      // Last 6 bytes are the most significant (bytes 10 through 15)
      normalized[0] = source[10];
      normalized[1] = source[11];
      normalized[2] = source[12];
      normalized[3] = source[13];
      normalized[4] = source[14];
      normalized[5] = source[15];

      // Then come bytes 8,9
      normalized[6] = source[8];
      normalized[7] = source[9];

      // Then come bytes 6,7
      normalized[8] = source[7];
      normalized[9] = source[6];

      // Then come bytes 4,5
      normalized[10] = source[5];
      normalized[11] = source[4];

      // Then come the first 4 bytes  (bytes 0 through 4)
      normalized[12] = source[3];
      normalized[13] = source[2];
      normalized[14] = source[1];
      normalized[15] = source[0];

      return normalized;
    }
  }

  /**
   * Converts given DateTime to normalized binary representation.
   *
   * @param value Input DateTime value.
   * @return Normalized array of bytes.
   */
  private static byte[] normalize(LocalDateTime value) {
    if (LocalDateTime.MIN.equals(value)) {
      return ShardKey.S_EMPTY_ARRAY;
    } else {
      long epochSeconds = value.atZone(ZoneId.systemDefault()).toEpochSecond();
      long dtTicks = (epochSeconds * TICKS_PER_MILLISECOND) + TICKS_AT_EPOCH;
      return normalize(dtTicks);
    }
  }

  /**
   * Converts given TimeSpan to normalized binary representation.
   *
   * @param value Input TimeSpan value.
   * @return Normalized array of bytes.
   */
  private static byte[] normalize(Duration value) {
    if (value.equals(Duration.ZERO)) {
      return ShardKey.S_EMPTY_ARRAY;
    } else {
      return normalize(value.getSeconds());
    }
  }

  /**
   * Converts given DateTimeOffset to normalized binary representation.
   *
   * @param value Input DateTimeOffset value.
   * @return Normalized array of bytes.
   */
  private static byte[] normalize(OffsetDateTime value) {
    if (value.equals(OffsetDateTime.MIN)) {
      return ShardKey.S_EMPTY_ARRAY;
    } else {
      // we store this as 2 parts: a date part and an offset part.
      // the date part is the utc value of the input
      long storedDtValue = value.toEpochSecond();
      long storedOffsetTicks = value.getOffset().getLong(ChronoField.OFFSET_SECONDS);

      byte[] normalizedDtValue = normalize(storedDtValue);
      byte[] normalizedOffsetTicks = normalize(storedOffsetTicks);

      byte[] result = new byte[SIZE_OF_DATE_TIME_OFFSET];
      System.arraycopy(normalizedDtValue, 0, result, 0, normalizedDtValue.length);
      System.arraycopy(normalizedOffsetTicks, 0, result,
          normalizedDtValue.length, normalizedOffsetTicks.length);

      return result;
    }
  }

  /**
   * Converts given byte array to normalized binary representation.
   *
   * @param value Input byte array.
   * @return Normalized array of bytes.
   */
  private static byte[] normalize(byte[] value) {
    return truncateTrailingZero(value);
  }

  /**
   * Truncate tailing zero of a byte array.
   *
   * @param a The array from which truncate trailing zeros
   * @return a new byte array with non-zero tail
   */
  private static byte[] truncateTrailingZero(byte[] a) {
    if (a != null) {
      if (a.length == 0) {
        return ShardKey.S_EMPTY_ARRAY;
      }

      // Get the index of last byte with non-zero value
      int lastNonZeroIndex = a.length;

      while (--lastNonZeroIndex >= 0 && a[lastNonZeroIndex] == 0) {
      }

      // If the index of the last non-zero byte is not the last index of the array,
      // there are trailing zeros
      int countOfTrailingZero = a.length - lastNonZeroIndex - 1;

      byte[] tmp = a;
      a = new byte[a.length - countOfTrailingZero];
      for (int i = 0; i < a.length; i++) {
        // Copy byte by byte until the last non-zero byte
        a[i] = tmp[i];
      }
    }

    return a;
  }

  ///#endregion

  /**
   * Instantiates a new shard key using the specified type and binary representation.
   *
   * @param keyType Type of the shard key (Int32, Int64, Guid, byte[] etc.).
   * @param rawValue Binary representation of the key.
   * @return A new shard key instance.
   */
  public static ShardKey fromRawValue(ShardKeyType keyType, byte[] rawValue) {
    return new ShardKey(keyType, rawValue, true);
  }

  /**
   * Given an object detect its ShardKeyType.
   *
   * @param value Given value. Must be non-null.
   * @return Corresponding ShardKeyType.
   */
  public static ShardKeyType detectShardKeyType(Object value) {
    ExceptionUtils.DisallowNullArgument(value, "value");
    return shardKeyTypeFromType(value.getClass());
  }

  /**
   * Checks whether the specified type is supported as ShardKey type.
   *
   * @param type Input type.
   * @return True if supported, false otherwise.
   */
  public static boolean isSupportedType(Class type) {
    return CLASS_SHARD_KEY_TYPE_HASH_MAP.containsKey(type);
  }

  /**
   * Gets the CLR type corresponding to the specified ShardKeyType.
   *
   * @param keyType Input ShardKeyType.
   * @return CLR type.
   */
  public static Class typeFromShardKeyType(ShardKeyType keyType) {
    if (keyType == ShardKeyType.None) {
      throw new IllegalArgumentException(Errors._ShardKey_UnsupportedShardKeyType,
          new Throwable("keyType"));
    }

    return SHARD_KEY_TYPE_CLASS_HASH_MAP.get(keyType);
  }

  /**
   * Gets the ShardKeyType corresponding to CLR type.
   *
   * @param type CLR type.
   * @return ShardKey type.
   */
  public static ShardKeyType shardKeyTypeFromType(Class type) {
    if (CLASS_SHARD_KEY_TYPE_HASH_MAP.containsKey(type)) {
      return CLASS_SHARD_KEY_TYPE_HASH_MAP.get(type);
    } else {
      throw new IllegalArgumentException(Errors._ShardKey_UnsupportedType + " type:" + type);
    }
  }

  /**
   * Mix up the hash key and add the specified value into it.
   *
   * @param hashKey The previous value of the hash
   * @param value The additional value to mix into the hash
   * @return The updated hash value
   */
  public static int qpHash(int hashKey, int value) {
    //WARNING: The right shift operator was not replaced by
    // Java's logical right shift operator since the left operand
    // was not confirmed to be of an unsigned type, but you should review
    // whether the logical right shift operator (>>>) is more appropriate:
    return hashKey ^ ((hashKey << 11) + (hashKey << 5) + (hashKey >> 2) + value);
  }

  ///#region Denormalize

  /**
   * Takes a byte array and a shard key type and convert it to its native denormalized Java type.
   *
   * @return The denormalized object
   */
  private Object deNormalize(ShardKeyType keyType, byte[] value) {
    // Return null for positive infinity.
    if (value == null) {
      return null;
    }

    switch (keyType) {
      case Int32:
        return deNormalizeInt32(value);

      case Int64:
        return deNormalizeInt64(value);

      case Guid:
        return deNormalizeGuid(value);

      case DateTime:
        long dtTicks = deNormalizeInt64(value);
        if (dtTicks == Long.MIN_VALUE) {
          return LocalDateTime.MIN;
        }
        long epochSeconds = (dtTicks - TICKS_AT_EPOCH) / TICKS_PER_MILLISECOND;
        ZoneOffset offset = ZoneId.systemDefault().getRules().getOffset(LocalDateTime.now());
        return LocalDateTime.ofEpochSecond(epochSeconds, 0, offset);

      case TimeSpan:
        long tsTicks = deNormalizeInt64(value);
        return tsTicks == Long.MIN_VALUE ? Duration.ZERO : Duration.ofSeconds(tsTicks);

      case DateTimeOffset:
        return deNormalizeDateTimeOffset(value);

      default:
        // For varbinary type, we simply keep it as a VarBytes object
        assert keyType == ShardKeyType.Binary;
        return this.value;
    }
  }

  private int deNormalizeInt32(byte[] value) {
    if (value.length == 0) {
      return Integer.MIN_VALUE;
    } else {
      value[0] ^= 0x80; // modify new array.
      return ByteBuffer.wrap(value).getInt();
    }
  }

  private long deNormalizeInt64(byte[] value) {
    if (value.length == 0) {
      return Long.MIN_VALUE;
    } else {
      value[0] ^= 0x80; // modify new array.
      return ByteBuffer.wrap(value).getLong();
    }
  }

  private UUID deNormalizeGuid(byte[] value) {
    if (value == null) {
      return null;
    } else if (value.length == 0) {
      return new UUID(0L, 0L);
    } else {
      // Shuffle bytes to the denormalized form
      byte[] denormalized = new byte[ShardKey.SIZE_OF_GUID];

      // Get the last 4 bytes first
      denormalized[0] = value[15];
      denormalized[1] = value[14];
      denormalized[2] = value[13];
      denormalized[3] = value[12];

      // Get every two bytes of the prev 6 bytes
      denormalized[4] = value[11];
      denormalized[5] = value[10];

      denormalized[6] = value[9];
      denormalized[7] = value[8];

      denormalized[8] = value[6];
      denormalized[9] = value[7];

      // Copy the first 6 bytes
      denormalized[10] = value[0];
      denormalized[11] = value[1];
      denormalized[12] = value[2];
      denormalized[13] = value[3];
      denormalized[14] = value[4];
      denormalized[15] = value[5];

      byte[] most = Arrays.copyOfRange(denormalized, 0, SIZE_OF_GUID / 2);
      byte[] least = Arrays.copyOfRange(denormalized, SIZE_OF_GUID / 2, SIZE_OF_GUID);
      return new UUID(ByteBuffer.wrap(most).getLong(),
          ByteBuffer.wrap(least).getLong());
    }
  }

  private OffsetDateTime deNormalizeDateTimeOffset(byte[] value) {
    // we stored the date and offset as 2 normalized Int64s. So split our input
    // byte array and de-normalize the pieces
    byte[] denormalizedDtValue = new byte[(Long.SIZE / Byte.SIZE)];
    byte[] denormalizedOffsetTicks = new byte[(Long.SIZE / Byte.SIZE)];

    System.arraycopy(value, 0, denormalizedDtValue, 0, denormalizedDtValue.length);
    System.arraycopy(value, denormalizedDtValue.length, denormalizedOffsetTicks,
        0, denormalizedOffsetTicks.length);

    long datePart = deNormalizeInt64(denormalizedDtValue);
    long offsetPart = deNormalizeInt64(denormalizedOffsetTicks);

    if (datePart == Long.MIN_VALUE) {
      return OffsetDateTime.MIN;
    }

    ZoneOffset zoneOffset = ZoneId.systemDefault().getRules().getOffset(LocalDateTime.now());
    LocalDateTime date = LocalDateTime.ofEpochSecond(datePart, 0, zoneOffset)
        .plusSeconds(offsetPart);
    return OffsetDateTime.of(date, zoneOffset);
  }

  ///#endregion

  /**
   * True if the key has a value; otherwise, false. Positive infinity returns false.
   */
  public boolean getHasValue() {
    return this.value != null;
  }

  /**
   * Returns true if the key value is negative infinity; otherwise, false.
   */
  public boolean getIsMin() {
    return this.value != null && this.value.length == 0;
  }

  /**
   * True if the key value is positive infinity; otherwise, false.
   */
  public boolean getIsMax() {
    return this.value == null;
  }

  /**
   * Gets a byte array representing the key value.
   */
  public byte[] getRawValue() {
    if (this.value == null) {
      return null;
    } else {
      return Arrays.copyOf(this.value, this.value.length);
    }
  }

  /**
   * Gets the denormalized value of the key.
   */
  @XmlElement(name = "Value")
  public Object getValue() {
    return deNormalize(keyType, getRawValue());
  }

  Object getValueWithCheck(Class<?> keyTypeClassName) {
    if (!shardKeyTypeFromType(keyTypeClassName).equals(keyType)) {
      throw new IllegalStateException(
          StringUtilsLocal.FormatInvariant(
              Errors._ShardKey_RequestedTypeDoesNotMatchShardKeyType,
              keyTypeClassName,
              keyType));
    }
    if (this.getIsMax()) {
      throw new IllegalStateException(Errors._ShardKey_MaxValueCannotBeRepresented);
    }
    return deNormalize(keyType, getRawValue());
  }

  /**
   * Gets the type of the shard key.
   */
  public ShardKeyType getKeyType() {
    return keyType;
  }

  /**
   * Gets the type of the value present in the object.
   */
  public Class getDataType() {
    return ShardKey.SHARD_KEY_TYPE_CLASS_HASH_MAP.get(keyType);
  }

  /**
   * Converts the object to its string representation.
   *
   * @return String representation of the object.
   */
  @Override
  public String toString() {
    if (this.getIsMax()) {
      return ShardKey.POSITIVE_INFINITY;
    } else {
      switch (keyType) {
        case Int32:
        case Int64:
        case Guid:
        case DateTime:
        case DateTimeOffset:
        case TimeSpan:
          return keyType.name() + "=" + this.getValue().toString();
        case Binary:
          return StringUtilsLocal.ByteArrayToString(this.value);
        default:
          assert keyType == ShardKeyType.None;
          //Debug.Fail("Unexpected type for string representation.");
          return "";
      }
    }
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
    return this.equals((ShardKey) ((obj instanceof ShardKey) ? obj : null));
  }

  /**
   * Performs equality comparison with another given ShardKey.
   *
   * @param other ShardKey to compare with.
   * @return True if same shard key, false otherwise.
   */
  public boolean equals(ShardKey other) {
    return other != null && (this.hashCode() == other.hashCode() || this.compareTo(other) == 0);
  }

  /**
   * Compares between two <see cref="ShardKey"/> values.
   *
   * @param other The <see cref="ShardKey"/> compared with this object.
   * @return 0 for equality, &lt; -1 if this key is less than <paramref name="other"/>, &gt; 1
   * otherwise.
   */
  public int compareTo(ShardKey other) {
    if (other == null) {
      return 1;
    }

    // Handle the obvious case of same objects.
    if (this == other) {
      return 0;
    }

    if (keyType != other.getKeyType()) {
      throw new IllegalStateException(StringUtilsLocal
          .FormatInvariant(Errors._ShardKey_ShardKeyTypesMustMatchForComparison, keyType,
              other.keyType));
    }

    // Handle if any of the keys is MaxKey
    if (this.getIsMax()) {
      if (other.getIsMax()) {
        return 0;
      }

      return 1;
    }

    if (other.getIsMax()) {
      return -1;
    }

    // If both values reference the same array, they are equal.
    if (this.value == other.value) {
      return 0;
    }

    // if it's DateTimeOffset we compare just the date part
    if (getKeyType() == ShardKeyType.DateTimeOffset) {
      byte[] rawThisValue = new byte[(Long.SIZE / Byte.SIZE)];
      byte[] rawOtherValue = new byte[(Long.SIZE / Byte.SIZE)];

      System.arraycopy(this.value, 0, rawThisValue, 0, rawThisValue.length);
      System.arraycopy(other.value, 0, rawOtherValue, 0, rawOtherValue.length);

      ShardKey interimKeyThis = ShardKey.fromRawValue(ShardKeyType.DateTime, rawThisValue);
      ShardKey interimKeyOther = ShardKey.fromRawValue(ShardKeyType.DateTime, rawOtherValue);

      return interimKeyThis.compareTo(interimKeyOther);
    }

    int minLength = Math.min(this.value.length, other.value.length);

    int differentByteIndex;

    for (differentByteIndex = 0; differentByteIndex < minLength; differentByteIndex++) {
      if (this.value[differentByteIndex] != other.value[differentByteIndex]) {
        break;
      }
    }

    if (differentByteIndex == minLength) {
      // If all they bytes are same, then the key with the longer byte array is bigger.
      // Note that we remove trailing 0's which are inert and could break this logic.
      return (new Integer(this.value.length)).compareTo(other.value.length);
    } else {
      // Compare the most significant different byte.
      Byte leftValue = (byte) (this.value[differentByteIndex] ^ 0x80);
      Byte rightValue = (byte) (other.value[differentByteIndex] ^ 0x80);
      return leftValue.compareTo(rightValue);
    }
  }

  /**
   * Gets the next higher key
   *
   * @return Incremented newly constructed ShardKey Returns a new ShardKey that is the numerical
   * successor of this ShardKey (add a binary bit). For example, if this ShardKey has the integer
   * value 0, getNextKey() returns a ShardKey with the value 1. Alternatively, if this ShardKey is a
   * byte array with the value 0x1234, getNextKey() returns a ShardKey with the value 0x1234...251
   * zeros....1
   */
  public ShardKey getNextKey() {
    if (this.getIsMax()) {
      throw new IllegalStateException(Errors._ShardKey_MaxValueCannotBeIncremented);
    } else {
      int len = 0;

      switch (this.getKeyType()) {
        case Int32:
          len = (Integer.SIZE / Byte.SIZE);
          break;

        case Int64:
        case DateTime:
        case TimeSpan:
          len = (Long.SIZE / Byte.SIZE);
          break;

        case Guid:
          len = ShardKey.SIZE_OF_GUID;
          break;

        case Binary:
          len = ShardKey.MAXIMUM_VAR_BYTES_KEY_SIZE;
          break;

        case DateTimeOffset:
          byte[] denormalizedDtValue = new byte[(Long.SIZE / Byte.SIZE)];

          // essentially we do get next key of the date part (stored in utc) and
          // re-store that along with the original offset
          System.arraycopy(this.value, 0, denormalizedDtValue, 0, denormalizedDtValue.length);
          ShardKey interimKey = ShardKey.fromRawValue(ShardKeyType.DateTime, denormalizedDtValue);
          ShardKey interimNextKey = interimKey.getNextKey();
          byte[] byteRes = new byte[SIZE_OF_DATE_TIME_OFFSET];
          System.arraycopy(interimNextKey.getRawValue(), 0, byteRes, 0,
              interimNextKey.getRawValue().length);
          System.arraycopy(value, interimNextKey.getRawValue().length, byteRes,
              interimNextKey.getRawValue().length, (Long.SIZE / Byte.SIZE));

          return ShardKey.fromRawValue(ShardKeyType.DateTimeOffset, byteRes);
        default:
          //Debug.Fail("Unexpected shard key kind.");
          break;
      }

      byte[] b = new byte[len];
      System.arraycopy(this.value, 0, b, 0, this.value.length);

      // push carry forward, (per byte for now)
      while (--len >= 0 && ++b[len] == 0) {
        ;
      }

      // Overflow, the current key's value is the maximum in the key spectrum.
      // Return +inf i.e. ShardKey with IsMax set to true.
      if (len < 0) {
        return new ShardKey(this.getKeyType(), null);
      } else {
        return ShardKey.fromRawValue(this.getKeyType(), b);
      }
    }
  }
}

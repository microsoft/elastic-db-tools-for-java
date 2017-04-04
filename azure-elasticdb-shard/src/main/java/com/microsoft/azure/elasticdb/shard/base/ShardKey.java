package com.microsoft.azure.elasticdb.shard.base;

// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

import com.microsoft.azure.elasticdb.core.commons.helpers.ReferenceObjectHelper;
import com.microsoft.azure.elasticdb.shard.utils.Errors;
import com.microsoft.azure.elasticdb.shard.utils.ExceptionUtils;
import com.microsoft.azure.elasticdb.shard.utils.StringUtilsLocal;

import java.util.HashMap;
import java.util.UUID;

/**
 * Shard key value. Wraps the type and value and allows normalization/denormalization
 * for serialization.
 */
public final class ShardKey implements Comparable<ShardKey> {
    /**
     * Size of Guid.
     */
    private static final int SizeOfGuid = 16;

    /**
     * Size of Guid.
     */
    private static final int SizeOfDateTimeOffset = 16;

    /**
     * Maximum size allowed for VarBytes keys.
     */
    private static final int MaximumVarBytesKeySize = 128;

    /**
     * String representation of +ve infinity.
     */
    private static final String PositiveInfinity = "+inf";

    /**
     * An empty array.
     */
    private static final byte[] s_emptyArray = new byte[0];

    /**
     * Mapping b/w CLR type and corresponding ShardKeyType.
     */
    private static final Lazy<HashMap<Class, ShardKeyType>> s_typeToShardKeyType = new Lazy<HashMap<Class, ShardKeyType>>(() -> new HashMap<Class, ShardKeyType>() {
        {
            Integer.class, ShardKeyType.Int32
        },

        {
            Long.class, ShardKeyType.Int64
        },

        {
            UUID.class, ShardKeyType.Guid
        },

        {
            byte[].class, ShardKeyType.Binary
        },

        {
            java.time.LocalDateTime.class, ShardKeyType.DateTime
        },

        {
            TimeSpan.class, ShardKeyType.TimeSpan
        },

        {
            DateTimeOffset.class, ShardKeyType.DateTimeOffset
        }
    },
            LazyThreadSafetyMode.PublicationOnly);

    /**
     * Mapping b/w ShardKeyType and corresponding CLR type.
     */
//WARNING: Unsigned integer types have no direct equivalent in Java:
//ORIGINAL LINE: private static readonly Lazy<Dictionary<ShardKeyType, Type>> s_shardKeyTypeToType = new Lazy<Dictionary<ShardKeyType,Type>>(() => new Dictionary<ShardKeyType, Type>() { { ShardKeyType.Int32, typeof(int) }, { ShardKeyType.Int64, typeof(long) }, { ShardKeyType.Guid, typeof(Guid) }, { ShardKeyType.Binary, typeof(byte[]) }, { ShardKeyType.DateTime, typeof(DateTime) }, { ShardKeyType.TimeSpan, typeof(TimeSpan) }, { ShardKeyType.DateTimeOffset, typeof(DateTimeOffset) }}, LazyThreadSafetyMode.PublicationOnly);
    private static final Lazy<HashMap<ShardKeyType, Class>> s_shardKeyTypeToType = new Lazy<HashMap<ShardKeyType, Class>>(() -> new HashMap<ShardKeyType, Class>() {
        {
            ShardKeyType.Int32, Integer.class
        },

        {
            ShardKeyType.Int64, Long.class
        },

        {
            ShardKeyType.Guid, UUID.class
        },

        {
            ShardKeyType.Binary, byte[].class
        },

        {
            ShardKeyType.DateTime, java.time.LocalDateTime.class
        },

        {
            ShardKeyType.TimeSpan, TimeSpan.class
        },

        {
            ShardKeyType.DateTimeOffset, DateTimeOffset.class
        }
    },
            LazyThreadSafetyMode.PublicationOnly);

    /**
     * Represents negative infinity.
     */
    private static Lazy<ShardKey> s_minInt32 = new Lazy<ShardKey>(() -> new ShardKey(ShardKeyType.Int32, Integer.MIN_VALUE), LazyThreadSafetyMode.PublicationOnly);

    /**
     * Represents negative infinity.
     */
    private static Lazy<ShardKey> s_maxInt32 = new Lazy<ShardKey>(() -> new ShardKey(ShardKeyType.Int32, null), LazyThreadSafetyMode.PublicationOnly);

    /**
     * Represents negative infinity.
     */
    private static Lazy<ShardKey> s_minInt64 = new Lazy<ShardKey>(() -> new ShardKey(ShardKeyType.Int64, Long.MIN_VALUE), LazyThreadSafetyMode.PublicationOnly);

    /**
     * Represents negative infinity.
     */
    private static Lazy<ShardKey> s_maxInt64 = new Lazy<ShardKey>(() -> new ShardKey(ShardKeyType.Int64, null), LazyThreadSafetyMode.PublicationOnly);

    /**
     * Represents negative infinity.
     */
    private static Lazy<ShardKey> s_minGuid = new Lazy<ShardKey>(() -> new ShardKey(ShardKeyType.Guid, null), LazyThreadSafetyMode.PublicationOnly);

    /**
     * Represents negative infinity.
     */
    private static Lazy<ShardKey> s_maxGuid = new Lazy<ShardKey>(() -> new ShardKey(ShardKeyType.Guid, null), LazyThreadSafetyMode.PublicationOnly);

    /**
     * Represents negative infinity.
     */
    private static Lazy<ShardKey> s_minBinary = new Lazy<ShardKey>(() -> new ShardKey(ShardKeyType.Binary, ShardKey.s_emptyArray), LazyThreadSafetyMode.PublicationOnly);

    /**
     * Represents negative infinity.
     */
    private static Lazy<ShardKey> s_maxBinary = new Lazy<ShardKey>(() -> new ShardKey(ShardKeyType.Binary, null), LazyThreadSafetyMode.PublicationOnly);

    /**
     * Represents negative infinity.
     */
    private static Lazy<ShardKey> s_minDateTime = new Lazy<ShardKey>(() -> new ShardKey(ShardKeyType.DateTime, java.time.LocalDateTime.MIN), LazyThreadSafetyMode.PublicationOnly);

    /**
     * Represents negative infinity.
     */
    private static Lazy<ShardKey> s_maxDateTime = new Lazy<ShardKey>(() -> new ShardKey(ShardKeyType.DateTime, null), LazyThreadSafetyMode.PublicationOnly);

    /**
     * Represents negative infinity.
     */
    private static Lazy<ShardKey> s_minTimeSpan = new Lazy<ShardKey>(() -> new ShardKey(ShardKeyType.TimeSpan, TimeSpan.MinValue), LazyThreadSafetyMode.PublicationOnly);

    /**
     * Represents negative infinity.
     */
    private static Lazy<ShardKey> s_maxTimeSpan = new Lazy<ShardKey>(() -> new ShardKey(ShardKeyType.TimeSpan, null), LazyThreadSafetyMode.PublicationOnly);

    /**
     * Represents negative infinity.
     */
    private static Lazy<ShardKey> s_minDateTimeOffset = new Lazy<ShardKey>(() -> new ShardKey(ShardKeyType.DateTimeOffset, DateTimeOffset.MinValue), LazyThreadSafetyMode.PublicationOnly);

    /**
     * Represents negative infinity.
     */
    private static Lazy<ShardKey> s_maxDateTimeOffset = new Lazy<ShardKey>(() -> new ShardKey(ShardKeyType.DateTimeOffset, null), LazyThreadSafetyMode.PublicationOnly);
    /**
     * Type of shard key.
     */
    private ShardKeyType _keyType;
    /**
     * Value as saved in persistent storage. Empty byte array represents the minimum value,
     * and a null value represents the maximum value.
     */
//WARNING: Unsigned integer types have no direct equivalent in Java:
//ORIGINAL LINE: private readonly byte[] _value;
    private byte[] _value;
    /**
     * Hashcode for the shard key.
     */
    private int _hashCode;

    /**
     * Constructs a shard key using 32-bit integer value.
     *
     * @param value Input 32-bit integer.
     */
    public ShardKey(int value) {
        this(ShardKeyType.Int32, ShardKey.Normalize(value), false);
    }

    /**
     * Constructs a shard key using 64-bit integer value.
     *
     * @param value Input 64-bit integer.
     */
    public ShardKey(long value) {
        this(ShardKeyType.Int64, ShardKey.Normalize(value), false);
    }

    /**
     * Constructs a shard key using a Guid.
     *
     * @param value Input Guid.
     */
    public ShardKey(UUID value) {
        this(ShardKeyType.Guid, ShardKey.Normalize(value), false);
    }

    /**
     * Constructs a shard key using a byte array.
     *
     * @param value Input byte array.
     */
//WARNING: Unsigned integer types have no direct equivalent in Java:
//ORIGINAL LINE: public ShardKey(byte[] value)
    public ShardKey(byte[] value) {
        this(ShardKeyType.Binary, ShardKey.Normalize(value), true);
    }

    /**
     * Constructs a shard key using DateTime value.
     *
     * @param value Input DateTime.
     */
    public ShardKey(java.time.LocalDateTime value) {
        this(ShardKeyType.DateTime, ShardKey.Normalize(value), false);
    }

    /**
     * Constructs a shard key using TimeSpan value.
     *
     * @param value Input TimeSpan.
     */
    public ShardKey(TimeSpan value) {
        this(ShardKeyType.TimeSpan, ShardKey.Normalize(value), false);
    }

    /**
     * Constructs a shard key using TimeSpan value.
     *
     * @param value Input DateTimeOffset.
     */
    public ShardKey(DateTimeOffset value) {
        this(ShardKeyType.DateTimeOffset, ShardKey.Normalize(value), false);
    }

    /**
     * Constructs a shard key using given object.
     *
     * @param value Input object.
     */
    public ShardKey(Object value) {
        ExceptionUtils.DisallowNullArgument(value, "value");

        // We can't detect the type if value is null.
        if (DBNull.Value.equals(value)) {
            throw new IllegalArgumentException("value");
        }

        ShardKey shardKey = (ShardKey) ((value instanceof ShardKey) ? value : null);

        if (shardKey != null) {
            _keyType = shardKey._keyType;
            _value = shardKey._value;
        } else {
            _keyType = ShardKey.DetectShardKeyType(value);
            _value = ShardKey.Normalize(_keyType, value);
        }

        _hashCode = this.CalculateHashCode();
    }

    /**
     * Constructs a shard key using given object and keyType.
     *
     * @param keyType The key type of value in object.
     * @param value   Input object.
     */
    public ShardKey(ShardKeyType keyType, Object value) {
        if (keyType == ShardKeyType.None) {
            throw new IllegalArgumentException("keyType", keyType, Errors._ShardKey_UnsupportedShardKeyType);
        }

        _keyType = keyType;

        if (value != null && !DBNull.Value.equals(value)) {
            ShardKeyType detectedKeyType = ShardKey.DetectShardKeyType(value);

            if (_keyType != detectedKeyType) {
                throw new IllegalArgumentException(StringUtilsLocal.FormatInvariant(Errors._ShardKey_ValueDoesNotMatchShardKeyType, "keyType"), "value");
            }

            _value = ShardKey.Normalize(_keyType, value);
        } else {
            // Null represents positive infinity.
            _value = null;
        }

        _hashCode = this.CalculateHashCode();
    }

    /**
     * Instantiates the key with given type and raw value and optionally validates
     * the key type and raw representation of the value.
     *
     * @param keyType  Type of shard key.
     * @param rawValue Raw value of the key.
     * @param validate Whether to validate the key type and raw value.
     */
//WARNING: Unsigned integer types have no direct equivalent in Java:
//ORIGINAL LINE: private ShardKey(ShardKeyType keyType, byte[] rawValue, bool validate)
    private ShardKey(ShardKeyType keyType, byte[] rawValue, boolean validate) {
        _keyType = keyType;
        _value = rawValue;
        _hashCode = this.CalculateHashCode();

        if (validate) {
            int expectedLength;

            switch (keyType) {
                case ShardKeyType.Int32:
//WARNING: Unsigned integer types have no direct equivalent in Java:
//ORIGINAL LINE: expectedLength = sizeof(int);
                    expectedLength = (Integer.SIZE / Byte.SIZE);
                    break;

                case ShardKeyType.Int64:
                case ShardKeyType.DateTime:
                case ShardKeyType.TimeSpan:
//WARNING: Unsigned integer types have no direct equivalent in Java:
//ORIGINAL LINE: expectedLength = sizeof(long);
                    expectedLength = (Long.SIZE / Byte.SIZE);
                    break;

                case ShardKeyType.Guid:
                    expectedLength = ShardKey.SizeOfGuid;
                    break;

                case ShardKeyType.Binary:
                    expectedLength = ShardKey.MaximumVarBytesKeySize;
                    break;

                case ShardKeyType.DateTimeOffset:
                    expectedLength = SizeOfDateTimeOffset;
                    break;

                default:
                    throw new IllegalArgumentException("keyType", keyType, Errors._ShardKey_UnsupportedShardKeyType);
            }

            // +ve & -ve infinity. Correct size provided.
            if (_value == null || _value.length == 0 || _value.length == expectedLength) {
                return;
            }

            // Only allow byte[] values to be of different length than expected,
            // since there could be smaller values than 128 bytes. For anything
            // else any non-zero length should match the expected length.
            if (_keyType != ShardKeyType.Binary || _value.length > expectedLength) {
                throw new IllegalArgumentException("rawValue", rawValue, String.format(CultureInfo.InvariantCulture, Errors._ShardKey_ValueLengthUnexpected, _value.length, expectedLength, _keyType));
            }
        }
    }

    /**
     * Represents negative infinity.
     */
    public static ShardKey getMinInt32() {
        return s_minInt32.Value;
    }

    /**
     * Represents negative infinity.
     */
    public static ShardKey getMaxInt32() {
        return s_maxInt32.Value;
    }

    /**
     * Represents negative infinity.
     */
    public static ShardKey getMinInt64() {
        return s_minInt64.Value;
    }

    /**
     * Represents negative infinity.
     */
    public static ShardKey getMaxInt64() {
        return s_maxInt64.Value;
    }

    /**
     * Represents negative infinity.
     */
    public static ShardKey getMinGuid() {
        return s_minGuid.Value;
    }

    /**
     * Represents negative infinity.
     */
    public static ShardKey getMaxGuid() {
        return s_maxGuid.Value;
    }

    /**
     * Represents negative infinity.
     */
    public static ShardKey getMinBinary() {
        return s_minBinary.Value;
    }

    /**
     * Represents negative infinity.
     */
    public static ShardKey getMaxBinary() {
        return s_maxBinary.Value;
    }

    /**
     * Represents negative infinity.
     */
    public static ShardKey getMinDateTime() {
        return s_minDateTime.Value;
    }

    /**
     * Represents negative infinity.
     */
    public static ShardKey getMaxDateTime() {
        return s_maxDateTime.Value;
    }

    /**
     * Represents negative infinity.
     */
    public static ShardKey getMinTimeSpan() {
        return s_minTimeSpan.Value;
    }

    /**
     * Represents negative infinity.
     */
    public static ShardKey getMaxTimeSpan() {
        return s_maxTimeSpan.Value;
    }

    /**
     * Represents negative infinity.
     */
    public static ShardKey getMinDateTimeOffset() {
        return s_minDateTimeOffset.Value;
    }

    /**
     * Represents negative infinity.
     */
    public static ShardKey getMaxDateTimeOffset() {
        return s_maxDateTimeOffset.Value;
    }

    /**
     * Instantiates a new shard key using the specified type and binary representation.
     *
     * @param keyType  Type of the shard key (Int32, Int64, Guid, byte[] etc.).
     * @param rawValue Binary representation of the key.
     * @return A new shard key instance.
     */
//WARNING: Unsigned integer types have no direct equivalent in Java:
//ORIGINAL LINE: public static ShardKey FromRawValue(ShardKeyType keyType, byte[] rawValue)
    public static ShardKey FromRawValue(ShardKeyType keyType, byte[] rawValue) {
        return new ShardKey(keyType, rawValue, true);
    }

    /**
     * Compares two <see cref="ShardKey"/> using lexicographic order (less than).
     *
     * @param left  Left hand side <see cref="ShardKey"/> of the operator.
     * @param right Right hand side <see cref="ShardKey"/> of the operator.
     * @return True if lhs &lt; rhs
     */
    public static boolean OpLessThan(ShardKey left, ShardKey right) {
        if (left == null) {
            return (right == null) ? false : true;
        } else {
            return (left.compareTo(right) < 0);
        }
    }

    /**
     * Compares two <see cref="ShardKey"/> using lexicographic order (greater than).
     *
     * @param left  Left hand side <see cref="ShardKey"/> of the operator.
     * @param right Right hand side <see cref="ShardKey"/> of the operator.
     * @return True if lhs &gt; rhs
     */
    public static boolean OpGreaterThan(ShardKey left, ShardKey right) {
        return Microsoft.Azure.SqlDatabase.ElasticScale.ShardManagement.ShardKey.OpLessThan(right, left);
    }

    /**
     * Compares two <see cref="ShardKey"/> using lexicographic order (less or equal).
     *
     * @param left  Left hand side <see cref="ShardKey"/> of the operator.
     * @param right Right hand side <see cref="ShardKey"/> of the operator.
     * @return True if lhs &lt;= rhs
     */
    public static boolean OpLessThanOrEqual(ShardKey left, ShardKey right) {
        return !Microsoft.Azure.SqlDatabase.ElasticScale.ShardManagement.ShardKey.OpGreaterThan(left, right);
    }

    /**
     * Compares two <see cref="ShardKey"/> using lexicographic order (greater or equal).
     *
     * @param left  Left hand side <see cref="ShardKey"/> of the operator.
     * @param right Right hand side <see cref="ShardKey"/> of the operator.
     * @return True if lhs &gt;= rhs
     */
    public static boolean OpGreaterThanOrEqual(ShardKey left, ShardKey right) {
        return !Microsoft.Azure.SqlDatabase.ElasticScale.ShardManagement.ShardKey.OpLessThan(left, right);
    }

    /**
     * Equality operator.
     *
     * @param left  Left hand side
     * @param right Right hand side
     * @return True if the two objects are equal, false in all other cases
     */
    public static boolean OpEquality(ShardKey left, ShardKey right) {
        return left.equals(right);
    }

    /**
     * Inequality operator.
     *
     * @param left  Left hand side
     * @param right Right hand side
     * @return True if the two objects are not equal, false in all other cases
     */
    public static boolean OpInequality(ShardKey left, ShardKey right) {
        return !Microsoft.Azure.SqlDatabase.ElasticScale.ShardManagement.ShardKey.OpEquality(left, right);
    }

    /**
     * Gets the minimum of two shard keys.
     *
     * @param left  Left hand side.
     * @param right Right hand side.
     * @return Minimum of two shard keys.
     */
    public static ShardKey Min(ShardKey left, ShardKey right) {
        if (Microsoft.Azure.SqlDatabase.ElasticScale.ShardManagement.ShardKey.OpLessThan(left, right)) {
            return left;
        } else {
            return right;
        }
    }

    /**
     * Gets the maximum of two shard keys.
     *
     * @param left  Left hand side.
     * @param right Right hand side.
     * @return Maximum of two shard keys.
     */
    public static ShardKey Max(ShardKey left, ShardKey right) {
        if (Microsoft.Azure.SqlDatabase.ElasticScale.ShardManagement.ShardKey.OpGreaterThan(left, right)) {
            return left;
        } else {
            return right;
        }
    }

    /**
     * Given an object detect its ShardKeyType.
     *
     * @param value Given value. Must be non-null.
     * @return Corresponding ShardKeyType.
     */
    public static ShardKeyType DetectShardKeyType(Object value) {
        ExceptionUtils.DisallowNullArgument(value, "value");

        ShardKeyType keyType = null;

        ReferenceObjectHelper<TValue> tempRef_keyType = new ReferenceObjectHelper<TValue>(keyType);
        if (!ShardKey.s_typeToShardKeyType.Value.TryGetValue(value.getClass(), tempRef_keyType)) {
            keyType = tempRef_keyType.argValue;
            throw new IllegalArgumentException(StringUtilsLocal.FormatInvariant(Errors._ShardKey_UnsupportedValue, value.getClass()), "value");
        } else {
            keyType = tempRef_keyType.argValue;
        }

        return keyType;
    }

    /**
     * Checks whether the specified type is supported as ShardKey type.
     *
     * @param type Input type.
     * @return True if supported, false otherwise.
     */
    public static boolean IsSupportedType(Class type) {
        return s_typeToShardKeyType.Value.containsKey(type);
    }

    /**
     * Gets the CLR type corresponding to the specified ShardKeyType.
     *
     * @param keyType Input ShardKeyType.
     * @return CLR type.
     */
    public static Class TypeFromShardKeyType(ShardKeyType keyType) {
        if (keyType == ShardKeyType.None) {
            throw new IllegalArgumentException("keyType", keyType, Errors._ShardKey_UnsupportedShardKeyType);
        }

        return s_shardKeyTypeToType.Value.get(keyType);
    }

    /**
     * Gets the ShardKeyType corresponding to CLR type.
     *
     * @param type CLR type.
     * @return ShardKey type.
     */
    public static ShardKeyType ShardKeyTypeFromType(Class type) {
        if (s_typeToShardKeyType.Value.containsKey(type)) {
            return s_typeToShardKeyType.Value.get(type);
        } else {
            throw new IllegalArgumentException("type", type, Errors._ShardKey_UnsupportedType);
        }
    }

    /**
     * Mix up the hash key and add the specified value into it.
     *
     * @param hashKey The previous value of the hash
     * @param value   The additional value to mix into the hash
     * @return The updated hash value
     */
    public static int QPHash(int hashKey, int value) {
//WARNING: The right shift operator was not replaced by Java's logical right shift operator since the left operand was not confirmed to be of an unsigned type, but you should review whether the logical right shift operator (>>>) is more appropriate:
        return hashKey ^ ((hashKey << 11) + (hashKey << 5) + (hashKey >> 2) + value);
    }

    ///#region Operators

    /**
     * Take an object and convert it to its normalized representation as a byte array.
     *
     * @param keyType The type of the <see cref="ShardKey"/>.
     * @param value   The value
     * @return The normalized <see cref="ShardKey"/> information
     */
//WARNING: Unsigned integer types have no direct equivalent in Java:
//ORIGINAL LINE: private static byte[] Normalize(ShardKeyType keyType, object value)
    private static byte[] Normalize(ShardKeyType keyType, Object value) {
        switch (keyType) {
            case ShardKeyType.Int32:
                return ShardKey.Normalize((Integer) value);

            case ShardKeyType.Int64:
                return ShardKey.Normalize((Long) value);

            case ShardKeyType.Guid:
                return ShardKey.Normalize((UUID) value);

            case ShardKeyType.DateTime:
                return ShardKey.Normalize((java.time.LocalDateTime) value);

            case ShardKeyType.TimeSpan:
                return ShardKey.Normalize((TimeSpan) value);

            case ShardKeyType.DateTimeOffset:
                return ShardKey.Normalize((DateTimeOffset) value);

            default:
                assert keyType == ShardKeyType.Binary;
//WARNING: Unsigned integer types have no direct equivalent in Java:
//ORIGINAL LINE: return ShardKey.Normalize((byte[])value);
                return ShardKey.Normalize((byte[]) value);
        }
    }

    /**
     * Takes a byte array and a shard key type and convert it to its native denormalized C# type.
     *
     * @return The denormalized object
     */
//WARNING: Unsigned integer types have no direct equivalent in Java:
//ORIGINAL LINE: private static object DeNormalize(ShardKeyType keyType, byte[] value)
    private static Object DeNormalize(ShardKeyType keyType, byte[] value) {
        // Return null for positive infinity.
        if (value == null) {
            return null;
        }

        switch (keyType) {
            case ShardKeyType.Int32:
                return ShardKey.DenormalizeInt32(value);

            case ShardKeyType.Int64:
                return ShardKey.DenormalizeInt64(value);

            case ShardKeyType.Guid:
                return ShardKey.DenormalizeGuid(value);

            case ShardKeyType.DateTime:
                long dtTicks = ShardKey.DenormalizeInt64(value);
                return java.time.LocalDateTime.of(dtTicks);

            case ShardKeyType.TimeSpan:
                long tsTicks = ShardKey.DenormalizeInt64(value);
                return new TimeSpan(tsTicks);

            case ShardKeyType.DateTimeOffset:
                return DenormalizeDateTimeOffset(value);

            default:
                // For varbinary type, we simply keep it as a VarBytes object
                assert keyType == ShardKeyType.Binary;
                return ShardKey.DenormalizeByteArray(value);
        }
    }

    /**
     * Converts given 32-bit integer to normalized binary representation.
     *
     * @param value Input 32-bit integer.
     * @return Normalized array of bytes.
     */
//WARNING: Unsigned integer types have no direct equivalent in Java:
//ORIGINAL LINE: private static byte[] Normalize(int value)
    private static byte[] Normalize(int value) {
        if (value == Integer.MIN_VALUE) {
            return ShardKey.s_emptyArray;
        } else {
//WARNING: Unsigned integer types have no direct equivalent in Java:
//ORIGINAL LINE: byte[] normalized = BitConverter.GetBytes(IPAddress.HostToNetworkOrder(value));
            byte[] normalized = BitConverter.GetBytes(IPAddress.HostToNetworkOrder(value));

            // Maps Int32.Min - Int32.Max to UInt32.Min - UInt32.Max.
            normalized[0] ^= 0x80;

            return normalized;
        }
    }

    /**
     * Converts given 64-bit integer to normalized binary representation.
     *
     * @param value Input 64-bit integer.
     * @return Normalized array of bytes.
     */
//WARNING: Unsigned integer types have no direct equivalent in Java:
//ORIGINAL LINE: private static byte[] Normalize(long value)
    private static byte[] Normalize(long value) {
        if (value == Long.MIN_VALUE) {
            return ShardKey.s_emptyArray;
        } else {
//WARNING: Unsigned integer types have no direct equivalent in Java:
//ORIGINAL LINE: byte[] normalized = BitConverter.GetBytes(IPAddress.HostToNetworkOrder(value));
            byte[] normalized = BitConverter.GetBytes(IPAddress.HostToNetworkOrder(value));

            // Maps Int64.Min - Int64.Max to UInt64.Min - UInt64.Max.
            normalized[0] ^= 0x80;

            return normalized;
        }
    }

    /**
     * Converts given GUID to normalized binary representation.
     *
     * @param value Input GUID.
     * @return Normalized array of bytes.
     */
//WARNING: Unsigned integer types have no direct equivalent in Java:
//ORIGINAL LINE: private static byte[] Normalize(Guid value)
    private static byte[] Normalize(UUID value) {
        if (value == null) {
            return ShardKey.s_emptyArray;
        } else {
//WARNING: Unsigned integer types have no direct equivalent in Java:
//ORIGINAL LINE: byte[] source = value.ToByteArray();
            byte[] source = value.ToByteArray();

            // For normalization follow the pattern of SQL Server comparison.
//WARNING: Unsigned integer types have no direct equivalent in Java:
//ORIGINAL LINE: byte[] normalized = new byte[ShardKey.SizeOfGuid];
            byte[] normalized = new byte[ShardKey.SizeOfGuid];

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
            normalized[8] = source[6];
            normalized[9] = source[7];

            // Then come bytes 4,5
            normalized[10] = source[4];
            normalized[11] = source[5];

            // Then come the first 4 bytes  (bytes 0 through 4)
            normalized[12] = source[0];
            normalized[13] = source[1];
            normalized[14] = source[2];
            normalized[15] = source[3];

            return normalized;
        }
    }

    /**
     * Converts given DateTime to normalized binary representation.
     *
     * @param value Input DateTime value.
     * @return Normalized array of bytes.
     */
//WARNING: Unsigned integer types have no direct equivalent in Java:
//ORIGINAL LINE: private static byte[] Normalize(DateTime value)
    private static byte[] Normalize(java.time.LocalDateTime value) {
        if (java.time.LocalDateTime.MIN.equals(value)) {
            return ShardKey.s_emptyArray;
        } else {
            return Normalize(value.getTime());
        }
    }

    /**
     * Converts given TimeSpan to normalized binary representation.
     *
     * @param value Input TimeSpan value.
     * @return Normalized array of bytes.
     */
//WARNING: Unsigned integer types have no direct equivalent in Java:
//ORIGINAL LINE: private static byte[] Normalize(TimeSpan value)
    private static byte[] Normalize(TimeSpan value) {
        if (System.TimeSpan.OpEquality(value, TimeSpan.MinValue)) {
            return ShardKey.s_emptyArray;
        } else {
            return Normalize(value.Ticks);
        }
    }

    /**
     * Converts given DateTimeOffset to normalized binary representation.
     *
     * @param value Input DateTimeOffset value.
     * @return Normalized array of bytes.
     */
//WARNING: Unsigned integer types have no direct equivalent in Java:
//ORIGINAL LINE: private static byte[] Normalize(DateTimeOffset value)
    private static byte[] Normalize(DateTimeOffset value) {
        if (System.DateTimeOffset.OpEquality(value, DateTimeOffset.MinValue)) {
            return ShardKey.s_emptyArray;
        } else {
            // we store this as 2 parts: a date part and an offset part.
            // the date part is the utc value of the input
            long storedDtValue = value.UtcTicks;
            long storedOffsetTicks = value.Offset.Ticks;

//WARNING: Unsigned integer types have no direct equivalent in Java:
//ORIGINAL LINE: byte[] normalizedDtValue = Normalize(storedDtValue);
            byte[] normalizedDtValue = Normalize(storedDtValue);
//WARNING: Unsigned integer types have no direct equivalent in Java:
//ORIGINAL LINE: byte[] normalizedOffsetTicks = Normalize(storedOffsetTicks);
            byte[] normalizedOffsetTicks = Normalize(storedOffsetTicks);

//WARNING: Unsigned integer types have no direct equivalent in Java:
//ORIGINAL LINE: byte[] result = new byte[SizeOfDateTimeOffset];
            byte[] result = new byte[SizeOfDateTimeOffset];
            Buffer.BlockCopy(normalizedDtValue, 0, result, 0, Buffer.ByteLength(normalizedDtValue));
            Buffer.BlockCopy(normalizedOffsetTicks, 0, result, Buffer.ByteLength(normalizedDtValue), Buffer.ByteLength(normalizedOffsetTicks));

            return result;
        }
    }

    ///#endregion

    /**
     * Converts given byte array to normalized binary representation.
     *
     * @param value Input byte array.
     * @return Normalized array of bytes.
     */
//WARNING: Unsigned integer types have no direct equivalent in Java:
//ORIGINAL LINE: private static byte[] Normalize(byte[] value)
    private static byte[] Normalize(byte[] value) {
        return TruncateTrailingZero(value);
    }

    //WARNING: Unsigned integer types have no direct equivalent in Java:
//ORIGINAL LINE: private static int DenormalizeInt32(byte[] value)
    private static int DenormalizeInt32(byte[] value) {
        if (value.length == 0) {
            return Integer.MIN_VALUE;
        } else {
            // Make a copy of the normalized array
//WARNING: Unsigned integer types have no direct equivalent in Java:
//ORIGINAL LINE: byte[] denormalized = new byte[value.Length];
            byte[] denormalized = new byte[value.length];

            System.arraycopy(value, 0, denormalized, 0, value.length);

            // Flip the last bit and cast it to an integer
            denormalized[0] ^= 0x80;

            return System.Net.IPAddress.HostToNetworkOrder(BitConverter.ToInt32(denormalized, 0));
        }
    }

    //WARNING: Unsigned integer types have no direct equivalent in Java:
//ORIGINAL LINE: private static long DenormalizeInt64(byte[] value)
    private static long DenormalizeInt64(byte[] value) {
        if (value.length == 0) {
            return Long.MIN_VALUE;
        } else {
            // Make a copy of the normalized array
//WARNING: Unsigned integer types have no direct equivalent in Java:
//ORIGINAL LINE: byte[] denormalized = new byte[value.Length];
            byte[] denormalized = new byte[value.length];

            System.arraycopy(value, 0, denormalized, 0, value.length);

            // Flip the last bit and cast it to an integer
            denormalized[0] ^= 0x80;

            return System.Net.IPAddress.HostToNetworkOrder(BitConverter.ToInt64(denormalized, 0));
        }
    }

    //WARNING: Unsigned integer types have no direct equivalent in Java:
//ORIGINAL LINE: private static Guid DenormalizeGuid(byte[] value)
    private static UUID DenormalizeGuid(byte[] value) {
        if (value.length == 0) {
            return null;
        } else {
            // Shuffle bytes to the denormalized form
//WARNING: Unsigned integer types have no direct equivalent in Java:
//ORIGINAL LINE: byte[] denormalized = new byte[ShardKey.SizeOfGuid];
            byte[] denormalized = new byte[ShardKey.SizeOfGuid];

            // Get the last 4 bytes first
            denormalized[0] = value[12];
            denormalized[1] = value[13];
            denormalized[2] = value[14];
            denormalized[3] = value[15];

            // Get every two bytes of the prev 6 bytes
            denormalized[4] = value[10];
            denormalized[5] = value[11];

            denormalized[6] = value[8];
            denormalized[7] = value[9];

            denormalized[8] = value[6];
            denormalized[9] = value[7];

            // Copy the first 6 bytes
            denormalized[10] = value[0];
            denormalized[11] = value[1];
            denormalized[12] = value[2];
            denormalized[13] = value[3];
            denormalized[14] = value[4];
            denormalized[15] = value[5];

            return new UUID(denormalized);
        }
    }

    //WARNING: Unsigned integer types have no direct equivalent in Java:
//ORIGINAL LINE: private static DateTimeOffset DenormalizeDateTimeOffset(byte[] value)
    private static DateTimeOffset DenormalizeDateTimeOffset(byte[] value) {
        // we stored the date and offset as 2 normalized Int64s. So split our input
        // byte array and de-normalize the pieces
//WARNING: Unsigned integer types have no direct equivalent in Java:
//ORIGINAL LINE: byte[] denormalizedDtValue = new byte[sizeof(long)];
        byte[] denormalizedDtValue = new byte[(Long.SIZE / Byte.SIZE)];
//WARNING: Unsigned integer types have no direct equivalent in Java:
//ORIGINAL LINE: byte[] denormalizedOffsetTicks = new byte[sizeof(long)];
        byte[] denormalizedOffsetTicks = new byte[(Long.SIZE / Byte.SIZE)];

        Buffer.BlockCopy(value, 0, denormalizedDtValue, 0, Buffer.ByteLength(denormalizedDtValue));
        Buffer.BlockCopy(value, Buffer.ByteLength(denormalizedDtValue), denormalizedOffsetTicks, 0, Buffer.ByteLength(denormalizedOffsetTicks));

        long datePart = DenormalizeInt64(denormalizedDtValue);
        long offsetPart = DenormalizeInt64(denormalizedOffsetTicks);

        TimeSpan offset = new TimeSpan(offsetPart);

        // we stored the date part as utc so convert back from utc by applying the offset
        java.time.LocalDateTime date = (java.time.LocalDateTime.of(datePart)).Add(offset);
        DateTimeOffset result = new DateTimeOffset(date, offset);
        return result;
    }

    //WARNING: Unsigned integer types have no direct equivalent in Java:
//ORIGINAL LINE: private static byte[] DenormalizeByteArray(byte[] value)
    private static byte[] DenormalizeByteArray(byte[] value) {
        return value;
    }

    /**
     * Truncate tailing zero of a byte array.
     *
     * @param a The array from which truncate trailing zeros
     * @return a new byte array with non-zero tail
     */
//WARNING: Unsigned integer types have no direct equivalent in Java:
//ORIGINAL LINE: private static byte[] TruncateTrailingZero(byte[] a)
    private static byte[] TruncateTrailingZero(byte[] a) {
        if (a != null) {
            if (a.length == 0) {
                return ShardKey.s_emptyArray;
            }

            // Get the index of last byte with non-zero value
            int lastNonZeroIndex = a.length;

            while (--lastNonZeroIndex >= 0 && a[lastNonZeroIndex] == 0) {
            }

            // If the index of the last non-zero byte is not the last index of the array, there are trailing zeros
            int countOfTrailingZero = a.length - lastNonZeroIndex - 1;

//WARNING: Unsigned integer types have no direct equivalent in Java:
//ORIGINAL LINE: byte[] tmp = a;
            byte[] tmp = a;
//WARNING: Unsigned integer types have no direct equivalent in Java:
//ORIGINAL LINE: a = new byte[a.Length - countOfTrailingZero];
            a = new byte[a.length - countOfTrailingZero];
            for (int i = 0; i < a.length; i++) {
                // Copy byte by byte until the last non-zero byte
                a[i] = tmp[i];
            }
        }

        return a;
    }

    /**
     * True if the key has a value; otherwise, false. Positive infinity returns false.
     */
    public boolean getHasValue() {
        return _value != null;
    }

    /**
     * Returns true if the key value is negative infinity; otherwise, false.
     */
    public boolean getIsMin() {
        return _value != null && _value.length == 0;
    }

    /**
     * True if the key value is positive infinity; otherwise, false.
     */
    public boolean getIsMax() {
        return _value == null;
    }

    /**
     * Gets a byte array representing the key value.
     */
//WARNING: Unsigned integer types have no direct equivalent in Java:
//ORIGINAL LINE: public byte[] getRawValue()
    public byte[] getRawValue() {
        if (_value == null) {
            return null;
        } else {
//WARNING: Unsigned integer types have no direct equivalent in Java:
//ORIGINAL LINE: byte[] rawValueCpy = new byte[_value.Length];
            byte[] rawValueCpy = new byte[_value.length];
            System.arraycopy(_value, 0, rawValueCpy, 0, _value.length);
            return rawValueCpy;
        }
    }

    /**
     * Gets the denormalized value of the key.
     */
    public Object getValue() {
        return ShardKey.DeNormalize(_keyType, _value);
    }

    /**
     * Gets the type of the shard key.
     */
    public ShardKeyType getKeyType() {
        return _keyType;
    }

    /**
     * Gets the type of the value present in the object.
     */
    public Class getDataType() {
        return ShardKey.s_shardKeyTypeToType.Value.get(_keyType);
    }

    /**
     * Gets the strongly typed value of the shard key.
     * <p>
     * <typeparam name="T">Type of the key.</typeparam>
     *
     * @return Value of the key.
     */
    public <T> T GetValue() {
        if (this.getDataType() != T.class) {
            throw new IllegalStateException(StringUtilsLocal.FormatInvariant(Errors._ShardKey_RequestedTypeDoesNotMatchShardKeyType, T.class, this.getKeyType()));
        }

        if (this.getIsMax()) {
            throw new IllegalStateException(Errors._ShardKey_MaxValueCannotBeRepresented);
        }

        return (T) this.getValue();
    }

    /**
     * Converts the object to its string representation.
     *
     * @return String representation of the object.
     */
    @Override
    public String toString() {
        if (this.getIsMax()) {
            return ShardKey.PositiveInfinity;
        } else {
            switch (_keyType) {
                case ShardKeyType.Int32:
                case ShardKeyType.Int64:
                case ShardKeyType.Guid:
                case ShardKeyType.DateTime:
                case ShardKeyType.DateTimeOffset:
                case ShardKeyType.TimeSpan:
                    return this.getValue().toString();
                case ShardKeyType.Binary:
                    return StringUtils.ByteArrayToString(_value);
                default:
                    assert _keyType == ShardKeyType.None;
                    Debug.Fail("Unexpected type for string representation.");
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
        return this.equals((ShardKey) ((obj instanceof ShardKey) ? obj : null));
    }

    /**
     * Performs equality comparison with another given ShardKey.
     *
     * @param other ShardKey to compare with.
     * @return True if same shard key, false otherwise.
     */
    public boolean equals(ShardKey other) {
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
     * Compares between two <see cref="ShardKey"/> values.
     *
     * @param other The <see cref="ShardKey"/> compared with this object.
     * @return 0 for equality, &lt; -1 if this key is less than <paramref name="other"/>, &gt; 1 otherwise.
     */
    public int compareTo(ShardKey other) {
        if (other == null) {
            return 1;
        }

        // Handle the obvious case of same objects.
        if (this == other) {
            return 0;
        }

        if (_keyType != other.getKeyType()) {
            throw new IllegalStateException(StringUtilsLocal.FormatInvariant(Errors._ShardKey_ShardKeyTypesMustMatchForComparison, _keyType, other._keyType));
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
        if (_value == other._value) {
            return 0;
        }

        // if it's DateTimeOffset we compare just the date part
        if (getKeyType() == ShardKeyType.DateTimeOffset) {
//WARNING: Unsigned integer types have no direct equivalent in Java:
//ORIGINAL LINE: byte[] rawThisValue = new byte[sizeof(long)];
            byte[] rawThisValue = new byte[(Long.SIZE / Byte.SIZE)];
//WARNING: Unsigned integer types have no direct equivalent in Java:
//ORIGINAL LINE: byte[] rawOtherValue = new byte[sizeof(long)];
            byte[] rawOtherValue = new byte[(Long.SIZE / Byte.SIZE)];

            Buffer.BlockCopy(_value, 0, rawThisValue, 0, Buffer.ByteLength(rawThisValue));
            Buffer.BlockCopy(other._value, 0, rawOtherValue, 0, Buffer.ByteLength(rawOtherValue));

            ShardKey interimKeyThis = ShardKey.FromRawValue(ShardKeyType.DateTime, rawThisValue);
            ShardKey interimKeyOther = ShardKey.FromRawValue(ShardKeyType.DateTime, rawOtherValue);

            return interimKeyThis.compareTo(interimKeyOther);
        }

        int minLength = Math.min(_value.length, other._value.length);

        int differentByteIndex;

        for (differentByteIndex = 0; differentByteIndex < minLength; differentByteIndex++) {
            if (_value[differentByteIndex] != other._value[differentByteIndex]) {
                break;
            }
        }

        if (differentByteIndex == minLength) {
            // If all they bytes are same, then the key with the longer byte array is bigger.
            // Note that we remove trailing 0's which are inert and could break this logic.
            return (new Integer(_value.length)).compareTo(other._value.length);
        } else {
            // Compare the most significant different byte.
//WARNING: Unsigned integer types have no direct equivalent in Java:
//ORIGINAL LINE: return _value[differentByteIndex].CompareTo(other._value[differentByteIndex]);
            return (new Byte(_value[differentByteIndex])).compareTo(other._value[differentByteIndex]);
        }
    }

    /**
     * Gets the next higher key
     *
     * @return Incremented newly constructed ShardKey
     * Returns a new ShardKey that is the numerical successor of this ShardKey (add a binary bit).
     * For example, if this ShardKey has the integer value 0, GetNextKey() returns a ShardKey
     * with the value 1. Alternatively, if this ShardKey is a byte array with the value 0x1234,
     * GetNextKey() returns a ShardKey with the value 0x1234...251 zeros....1
     */
    public ShardKey GetNextKey() {
        if (this.getIsMax()) {
            throw new IllegalStateException(Errors._ShardKey_MaxValueCannotBeIncremented);
        } else {
            int len = 0;

            switch (this.getKeyType()) {
                case ShardKeyType.Int32:
//WARNING: Unsigned integer types have no direct equivalent in Java:
//ORIGINAL LINE: len = sizeof(int);
                    len = (Integer.SIZE / Byte.SIZE);
                    break;

                case ShardKeyType.Int64:
                case ShardKeyType.DateTime:
                case ShardKeyType.TimeSpan:
//WARNING: Unsigned integer types have no direct equivalent in Java:
//ORIGINAL LINE: len = sizeof(long);
                    len = (Long.SIZE / Byte.SIZE);
                    break;

                case ShardKeyType.Guid:
                    len = ShardKey.SizeOfGuid;
                    break;

                case ShardKeyType.Binary:
                    len = ShardKey.MaximumVarBytesKeySize;
                    break;

                case ShardKeyType.DateTimeOffset:
//WARNING: Unsigned integer types have no direct equivalent in Java:
//ORIGINAL LINE: byte[] denormalizedDtValue = new byte[sizeof(long)];
                    byte[] denormalizedDtValue = new byte[(Long.SIZE / Byte.SIZE)];

                    // essentially we do get next key of the date part (stored in utc) and
                    // re-store that along with the original offset
                    Buffer.BlockCopy(_value, 0, denormalizedDtValue, 0, Buffer.ByteLength(denormalizedDtValue));
                    ShardKey interimKey = ShardKey.FromRawValue(ShardKeyType.DateTime, denormalizedDtValue);
                    ShardKey interimNextKey = interimKey.GetNextKey();
//WARNING: Unsigned integer types have no direct equivalent in Java:
//ORIGINAL LINE: byte[] bRes = new byte[SizeOfDateTimeOffset];
                    byte[] bRes = new byte[SizeOfDateTimeOffset];
                    Buffer.BlockCopy(interimNextKey.getRawValue(), 0, bRes, 0, Buffer.ByteLength(interimNextKey.getRawValue()));
//WARNING: Unsigned integer types have no direct equivalent in Java:
//ORIGINAL LINE: Buffer.BlockCopy(_value, Buffer.ByteLength(interimNextKey.RawValue), bRes, Buffer.ByteLength(interimNextKey.RawValue), sizeof(long));
                    Buffer.BlockCopy(_value, Buffer.ByteLength(interimNextKey.getRawValue()), bRes, Buffer.ByteLength(interimNextKey.getRawValue()), (Long.SIZE / Byte.SIZE));

                    ShardKey resKey = ShardKey.FromRawValue(ShardKeyType.DateTimeOffset, bRes);
                    return resKey;

                default:
                    Debug.Fail("Unexpected shard key kind.");
                    break;
            }

//WARNING: Unsigned integer types have no direct equivalent in Java:
//ORIGINAL LINE: byte[] b = new byte[len];
            byte[] b = new byte[len];
            System.arraycopy(_value, 0, b, 0, _value.length);

            // push carry forward, (per byte for now)
            while (--len >= 0 && ++b[len] == 0) {
                ;
            }

            // Overflow, the current key's value is the maximum in the key spectrum. Return +inf i.e. ShardKey with IsMax set to true.
            if (len < 0) {
                return new ShardKey(this.getKeyType(), null);
            } else {
                return ShardKey.FromRawValue(this.getKeyType(), b);
            }
        }
    }

    /**
     * Calculates the hash code for the object.
     *
     * @return Hash code for the object.
     */
    private int CalculateHashCode() {
        int hash = (int) _keyType * 3137;

        if (null != _value) {
//WARNING: Unsigned integer types have no direct equivalent in Java:
//ORIGINAL LINE: byte[] tempArray = null;
            byte[] tempArray = null;
            if (getKeyType() == ShardKeyType.DateTimeOffset && _value.length == SizeOfDateTimeOffset) {
//WARNING: Unsigned integer types have no direct equivalent in Java:
//ORIGINAL LINE: tempArray = new byte[sizeof(long)];
                tempArray = new byte[(Long.SIZE / Byte.SIZE)];
                Buffer.BlockCopy(_value, 0, tempArray, 0, Buffer.ByteLength(tempArray));
            } else {
//WARNING: Unsigned integer types have no direct equivalent in Java:
//ORIGINAL LINE: tempArray = new byte[_value.Length];
                tempArray = new byte[_value.length];
                System.arraycopy(_value, 0, tempArray, 0, _value.length);
            }

//WARNING: Unsigned integer types have no direct equivalent in Java:
//ORIGINAL LINE: foreach (byte b in tempArray)
            for (byte b : tempArray) {
                hash = ShardKey.QPHash(hash, b);
            }
        }

        return hash;
    }
}

package com.microsoft.azure.elasticdb.shard.unittests;

import static org.junit.Assert.assertEquals;

import com.microsoft.azure.elasticdb.shard.base.ShardKey;
import com.microsoft.azure.elasticdb.shard.base.ShardKeyType;
import java.util.Arrays;
import java.util.HashMap;
import org.junit.Test;

/**
 * Test related to ShardKey class and date/time input values.
 */
public class ShardKeyTest {

  /**
   * The length in bytes of each ShardKeyType.
   */
  private HashMap<ShardKeyType, Integer> shardKeyTypeLength =
      new HashMap<ShardKeyType, Integer>() {
        {
          put(ShardKeyType.Int32, 4);
          put(ShardKeyType.Int64, 8);
          put(ShardKeyType.Guid, 16);
          put(ShardKeyType.Binary, 128);
          put(ShardKeyType.DateTime, 8);
          put(ShardKeyType.DateTimeOffset, 16);
          put(ShardKeyType.TimeSpan, 8);
        }
      };

  /**
   * Truncate tailing zero of a byte array.
   *
   * @param a The array from which truncate trailing zeros
   * @return a new byte array with non-zero tail
   */
  private static byte[] truncateTrailingZero(byte[] a) {
    if (a != null) {
      if (a.length == 0) {
        return new byte[0];
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
      System.arraycopy(tmp, 0, a, 0, a.length);
    }

    return a;
  }

  /**
   * Verifies that new ShardKey(keyType, value) returns the correct ShardKey.Value
   */
  @Test
  public void testShardKeyValue() {
    for (ShardKeyInfo shardKeyInfo : ShardKeyInfo.allTestShardKeyInfos) {
      System.out.println(shardKeyInfo);

      // Verify ShardKey.Value with value type-specific Equals
      if (shardKeyInfo.keyType == ShardKeyType.Binary && shardKeyInfo.value != null) {
        AssertExtensions.assertSequenceEqual(truncateTrailingZero((byte[]) shardKeyInfo.value),
            (byte[]) shardKeyInfo.getShardKeyFromValue().getValue());
      } else {
        assertEquals(shardKeyInfo.value, shardKeyInfo.getShardKeyFromValue().getValue());
      }
    }
  }

  /**
   * Verifies that new ShardKey(keyType, value) returns the correct RawValue.
   */
  @Test
  public void testShardKeySerialization() {
    for (ShardKeyInfo shardKeyInfo : ShardKeyInfo.allTestShardKeyInfos) {
      System.out.println(shardKeyInfo);

      byte[] expectedSerializedValue = shardKeyInfo.getRawValue();
      byte[] actualSerializedValue = shardKeyInfo.getShardKeyFromValue().getRawValue();

      if (expectedSerializedValue == null) {
        assertEquals(expectedSerializedValue, actualSerializedValue);
      } else {
        AssertExtensions.assertSequenceEqual(expectedSerializedValue, actualSerializedValue);
      }
    }
  }

  /**
   * Verifies that ShardKey.FromRawValue(keyType, rawValue) returns the correct ShardKey and
   * ShardKey.Value
   */
  @Test
  public void testShardKeyDeserialization() {
    for (ShardKeyInfo shardKeyInfo : ShardKeyInfo.allTestShardKeyInfos) {
      System.out.println(shardKeyInfo);

      ShardKey expectedDeserializedShardKey = shardKeyInfo.getShardKeyFromValue();
      ShardKey actualDeserializedShardKey = shardKeyInfo.getShardKeyFromRawValue();

      // Verify ShardKey with ShardKey.Equals
      assertEquals(expectedDeserializedShardKey, actualDeserializedShardKey);

      // Verify ShardKey.Value with value type-specific Equals
      assertEquals(expectedDeserializedShardKey.getValue(), actualDeserializedShardKey.getValue());
    }
  }

  /**
   * Verifies that ShardKey.FromRawValue(keyType, rawValue) returns the correct ShardKey and
   * ShardKey.Value if extra zeroes are added to the end of rawValue
   */
  @Test
  public void testShardKeyDeserializationAddTrailingZeroes() {
    for (ShardKeyInfo shardKeyInfo : ShardKeyInfo.allTestShardKeyInfos) {
      System.out.println(shardKeyInfo);

      int dataTypeLength = shardKeyTypeLength.get(shardKeyInfo.keyType);
      if (shardKeyInfo.getRawValue() != null
          && shardKeyInfo.getRawValue().length != dataTypeLength) {
        // Add trailing zeroes
        byte[] originalRawValue = shardKeyInfo.getRawValue();
        byte[] rawValueWithTrailingZeroes = new byte[dataTypeLength];
        rawValueWithTrailingZeroes = Arrays.copyOf(originalRawValue, 0);

        ShardKey expectedDeserializedShardKey = shardKeyInfo.getShardKeyFromValue();
        ShardKey actualDeserializedShardKey =
            ShardKey.fromRawValue(shardKeyInfo.keyType, rawValueWithTrailingZeroes);

        if (shardKeyInfo.keyType != ShardKeyType.Binary) {
          // Verify ShardKey.Value with value type-specific Equals
          assertEquals(expectedDeserializedShardKey.getValue(),
              actualDeserializedShardKey.getValue());
        }
      }
    }
  }

  /**
   * Tests that ShardKey.Min* and ShardKey.Max* have the correct KeyType, Value, and RawValue
   */
  /*@Test
  public void testShardKeyTypeInfo() {
    for (ShardKeyTypeInfo shardKeyTypeInfo : ShardKeyTypeInfo.shardKeyTypeInfos.values()) {
      System.out.println(shardKeyTypeInfo.keyType);

      // Min Value
      if (shardKeyTypeInfo.keyType != ShardKeyType.DateTime
          && shardKeyTypeInfo.keyType != ShardKeyType.DateTimeOffset) {

        assertEquals(shardKeyTypeInfo.keyType, shardKeyTypeInfo.maxShardKey.getKeyType());
        assertEquals(shardKeyTypeInfo.minValue, shardKeyTypeInfo.minShardKey.getValue());
        AssertExtensions.assertSequenceEqual(new byte[0],
            shardKeyTypeInfo.minShardKey.getRawValue());
      }

      // Max value
      assertEquals(shardKeyTypeInfo.keyType, shardKeyTypeInfo.maxShardKey.getKeyType());
      assertEquals(null, shardKeyTypeInfo.maxShardKey.getValue());
      assertEquals(null, shardKeyTypeInfo.maxShardKey.getRawValue());
    }
  }*/
}


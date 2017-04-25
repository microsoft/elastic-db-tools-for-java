package com.microsoft.azure.elasticdb.shard.mapmanager.unittests;

import java.util.Arrays;
import java.util.HashMap;

import org.junit.Test;
import static org.junit.Assert.*;

import com.microsoft.azure.elasticdb.shard.base.ShardKey;
import com.microsoft.azure.elasticdb.shard.base.ShardKeyType;

/**
 * Test related to ShardKey class and date/time input values.
 */
public class ShardKeyTests {
  /**
   * The length in bytes of each ShardKeyType
   */
  private HashMap<ShardKeyType, Integer> _shardKeyTypeLength =
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
   * Verifies that new ShardKey(keyType, value) returns the correct ShardKey.Value
   */
  @Test
  public void testShardKeyValue() {
    for (ShardKeyInfo shardKeyInfo : ShardKeyInfo.allTestShardKeyInfos) {
      System.out.println(shardKeyInfo);

      // Verify ShardKey.Value with value type-specific Equals
      if (shardKeyInfo.keyType == ShardKeyType.Binary && shardKeyInfo.value != null) {
        // TODO : assert custom -DropTrailingZeroes
      } else {
        assertEquals(shardKeyInfo.value, shardKeyInfo.getShardKeyFromValue().getValue());
      }
    }
  }

  /**
   * Verifies that new ShardKey(keyType, value) returns the correct RawValue
   */
  @Test
  public void testShardKeySerialization() {
    for (ShardKeyInfo shardKeyInfo : ShardKeyInfo.allTestShardKeyInfos) {
      System.out.println(shardKeyInfo);

      byte[] expectedSerializedValue = shardKeyInfo.rawValue;
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

      int dataTypeLength = _shardKeyTypeLength.get(shardKeyInfo.keyType);
      if (shardKeyInfo.rawValue != null && shardKeyInfo.rawValue.length != dataTypeLength) {
        // Add trailing zeroes
        byte[] originalRawValue = shardKeyInfo.rawValue;
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
  @Test
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
  }
}


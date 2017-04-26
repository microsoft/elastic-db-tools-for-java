package com.microsoft.azure.elasticdb.shard.mapmanager.unittests;

import com.microsoft.azure.elasticdb.shard.base.ShardKey;
import com.microsoft.azure.elasticdb.shard.base.ShardKeyType;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import org.apache.commons.codec.binary.Hex;

/**
 * A ShardKey and its corresponding RawValue in bytes.
 */
class ShardKeyInfo {

  /**
   * ShardKey and RawValue pairs to test for serialization/deserialization. DO NOT EDIT EXISTING
   * ENTRIES IN THIS LIST TO MAKE THE TEST PASS!!! The binary serialization format must be
   * consistent across different versions of EDCL. Any incompatible changes to this format is a
   * major breaking change!!!
   *
   * The general strategy is to pick the following boundary values: Min value Min value + 1 -1 (if
   * it's a numerical type) 0 (if it's a numerical type), or some other value in the middle of the
   * range +1 (if it's a numerical type) Max value - 1 Max value +inf
   */
  public static ShardKeyInfo[] allTestShardKeyInfos = new ShardKeyInfo[]{
      // INT32
      new ShardKeyInfo(Integer.MIN_VALUE, new byte[]{}),
      new ShardKeyInfo(Integer.MIN_VALUE + 1, new byte[]{0, 0, 0, 1}),
      new ShardKeyInfo(-1, new byte[]{(byte) 0x7f, (byte) 0xff, (byte) 0xff, (byte) 0xff}),
      new ShardKeyInfo(0, new byte[]{(byte) 0x80, 0, 0, 0}),
      new ShardKeyInfo(1, new byte[]{(byte) 0x80, 0, 0, 1}),
      new ShardKeyInfo(Integer.MAX_VALUE - 1,
          new byte[]{(byte) 0xff, (byte) 0xff, (byte) 0xff, (byte) 0xfe}),
      new ShardKeyInfo(Integer.MAX_VALUE,
          new byte[]{(byte) 0xff, (byte) 0xff, (byte) 0xff, (byte) 0xff}),
      new ShardKeyInfo(ShardKeyType.Int32, null, null),
      new ShardKeyInfo(Long.MIN_VALUE, new byte[]{}),

      // INT64
      new ShardKeyInfo(Long.MIN_VALUE + 1, new byte[]{0, 0, 0, 0, 0, 0, 0, 1}),
      new ShardKeyInfo((long) -1,
          new byte[]{0x7f, (byte) 0xff, (byte) 0xff, (byte) 0xff, (byte) 0xff, (byte) 0xff,
              (byte) 0xff, (byte) 0xff}),
      new ShardKeyInfo((long) 0, new byte[]{(byte) 0x80, 0, 0, 0, 0, 0, 0, 0}),
      new ShardKeyInfo((long) 1, new byte[]{(byte) 0x80, 0, 0, 0, 0, 0, 0, 1}),
      new ShardKeyInfo(Long.MAX_VALUE - 1,
          new byte[]{(byte) 0xff, (byte) 0xff, (byte) 0xff, (byte) 0xff, (byte) 0xff, (byte) 0xff,
              (byte) 0xff, (byte) 0xfe}),
      new ShardKeyInfo(Long.MAX_VALUE,
          new byte[]{(byte) 0xff, (byte) 0xff, (byte) 0xff, (byte) 0xff, (byte) 0xff, (byte) 0xff,
              (byte) 0xff, (byte) 0xff}),
      new ShardKeyInfo(ShardKeyType.Int64, null, null),

      // GUID
      new ShardKeyInfo(UUID.fromString("00000000-0000-0000-0000-000000000000"), new byte[]{}),
      new ShardKeyInfo(UUID.fromString("a0a1a2a3-a4a5-a6a7-a8a9-aaabacadaeaf"),
          new byte[]{(byte) 0xaa, (byte) 0xab, (byte) 0xac, (byte) 0xad, (byte) 0xae, (byte) 0xaf,
              /* - */ (byte) 0xa8, (byte) 0xa9, /* - */ (byte) 0xa7, (byte) 0xa6,
              /* - */ (byte) 0xa5, (byte) 0xa4, /* - */ (byte) 0xa3, (byte) 0xa2, (byte) 0xa1,
              (byte) 0xa0}),
      new ShardKeyInfo(ShardKeyType.Guid, null, null)

      //TODO Binary

      //TODO DateTime

      //TODO DateTimeOffset,Timespan
  };
  public static List<Object> allTestShardKeyValues = getValueFromShardKeyInfo(allTestShardKeyInfos);
  public ShardKeyType keyType;
  /**
   * The original value that is provided to the ShardKey(keyType, value) constructor which should
   * also exactly match ShardKey.Value [except that for binary type trailing zeroes are dropped]
   */
  public Object value;
  /**
   * The raw serialized value that is written to the database.
   */
  public byte[] rawValue;

  public ShardKeyInfo() {

  }

  public ShardKeyInfo(ShardKeyType keyType, Object value, byte[] rawValue) {

    this.keyType = keyType;
    this.value = value;
    this.rawValue = rawValue;
  }

  public ShardKeyInfo(Integer value, byte[] rawValue) {
    this(ShardKeyType.Int32, value, rawValue);
  }

  public ShardKeyInfo(Long value, byte[] rawValue) {
    this(ShardKeyType.Int64, value, rawValue);
  }

  public ShardKeyInfo(UUID value, byte[] rawValue) {
    this(ShardKeyType.Guid, value, rawValue);
  }

  public ShardKeyInfo(byte[] value) {
    this(ShardKeyType.Binary, value, value);
  }

  public ShardKeyInfo(LocalDateTime value, byte[] rawValue) {
    this(ShardKeyType.DateTime, value, rawValue);
  }

  public ShardKeyInfo(OffsetDateTime value, byte[] rawValue) {
    this(ShardKeyType.DateTimeOffset, value, rawValue);
  }

  public ShardKeyInfo(Duration value, byte[] rawValue) {
    this(ShardKeyType.DateTimeOffset, value, rawValue);
  }

  private static List<Object> getValueFromShardKeyInfo(ShardKeyInfo[] allTestShardKeyInfos) {
    List<Object> list = new ArrayList<>();
    for (ShardKeyInfo shardKeyInfo : allTestShardKeyInfos) {
      list.add(shardKeyInfo.value);
    }
    return list;
  }

  /**
   * Gets the ShardKey using new ShardKey(keyType, value);
   */
  public ShardKey getShardKeyFromValue() {
    return new ShardKey(keyType, value);
  }

  /**
   * Gets the ShardKey using ShardKey.FromRawValue(keyType, rawValue);
   */
  public ShardKey getShardKeyFromRawValue() {
    return ShardKey.fromRawValue(keyType, rawValue);
  }

  @Override
  public String toString() {
    return String.format("[%1$s]", keyType) + "" + String.format("%1$s", value) + "<->" + "0x"
        + Hex.encodeHexString(rawValue);
  }

}

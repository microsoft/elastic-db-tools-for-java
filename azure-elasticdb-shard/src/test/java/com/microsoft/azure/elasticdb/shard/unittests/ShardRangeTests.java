package com.microsoft.azure.elasticdb.shard.unittests;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Collections;
import java.util.Random;
import java.util.UUID;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.google.common.primitives.Bytes;
import com.microsoft.azure.elasticdb.shard.base.ShardKey;
import com.microsoft.azure.elasticdb.shard.base.ShardKeyType;
import com.microsoft.azure.elasticdb.shard.category.ExcludeFromGatedCheckin;
import com.microsoft.azure.elasticdb.shard.utils.Errors;

public class ShardRangeTests {

  public static final int max32 = 0x7FFFFFFF;
  public static final long max64 = 0x7FFFFFFFFFFFFFFFL;
  public ShardKey maxNonNullKey32 = new ShardKey(max32);
  public ShardKey maxNonNullKey64 = new ShardKey(max64);
  public Random rValGen = new Random();

  @Test
  @Category(value = ExcludeFromGatedCheckin.class)
  public void shardKeyTests() {
    ShardKey key = null;
    ShardKey result = null;
    byte[] array = null;
    byte[] arraymax = null;

    // Verify boundary conditions
    result = maxNonNullKey32.getNextKey();
    assert result.getIsMax();
    assert result == ShardKey.getMaxInt();

    result = maxNonNullKey64.getNextKey();
    assert result.getIsMax();
    assert result == ShardKey.getMaxLong();

    array = Bytes.toArray(Collections.nCopies(16, 0xff));
    key = ShardKey.fromRawValue(ShardKeyType.Guid, array); // can not use other ctor because
                                                           // normalized representation differ
    result = key.getNextKey();
    assert result.getIsMax();
    assert result == ShardKey.getMaxGuid();

    array = Bytes.toArray(Collections.nCopies(128, 0xff));
    key = new ShardKey(array);
    result = key.getNextKey();
    assert result.getIsMax();
    assert result == ShardKey.getMaxBinary();

    key = new ShardKey(max32 - 1);
    result = key.getNextKey();
    assert result == maxNonNullKey32;

    key = new ShardKey(max64 - 1);
    result = key.getNextKey();
    assert result == maxNonNullKey64;

    arraymax = Bytes.toArray(Collections.nCopies(16, 0xff));
    array = Bytes.toArray(Collections.nCopies(16, 0xff));
    array[15] = (byte) 0xfe;
    key = ShardKey.fromRawValue(ShardKeyType.Guid, array); // can not use other ctor because
                                                           // normalized representation differ
    result = key.getNextKey();
    assert result == ShardKey.fromRawValue(ShardKeyType.Guid, arraymax);

    arraymax = Bytes.toArray(Collections.nCopies(128, 0xff));
    array = Bytes.toArray(Collections.nCopies(128, 0xff));
    array[127] = (byte) 0xfe;
    key = new ShardKey(array);
    result = key.getNextKey();
    assert result == ShardKey.fromRawValue(ShardKeyType.Binary, arraymax);

    key = new ShardKey(ShardKeyType.Int32, null);
    ShardKey keyValue = key;
    AssertExtensions.<IllegalStateException>assertThrows(() -> keyValue.getNextKey());

    key = new ShardKey(ShardKeyType.Int64, null);
    AssertExtensions.<IllegalStateException>assertThrows(() -> keyValue.getNextKey());

    key = new ShardKey(ShardKeyType.Guid, null);
    AssertExtensions.<IllegalStateException>assertThrows(() -> keyValue.getNextKey());

    key = new ShardKey(ShardKeyType.Binary, null);
    AssertExtensions.<IllegalStateException>assertThrows(() -> keyValue.getNextKey());

    result = ShardKey.getMinInt().getNextKey();
    assert result == new ShardKey(Integer.MIN_VALUE + 1);

    result = ShardKey.getMinLong().getNextKey();
    assert result == new ShardKey(Long.MIN_VALUE + 1);

    result = ShardKey.getMinGuid().getNextKey();
    array = new byte[16];
    array[15] = 0x01;
    key = ShardKey.fromRawValue(ShardKeyType.Guid, array);
    assert result == key;

    result = ShardKey.getMinBinary().getNextKey();
    array = new byte[128];
    array[127] = 0x01;
    key = ShardKey.fromRawValue(ShardKeyType.Binary, array);
    assert result == key;

    for (int i = 0; i < 10; i++) {
      Verify(ShardKeyType.Int32);
      Verify(ShardKeyType.Int64);
      Verify(ShardKeyType.Guid);
      Verify(ShardKeyType.Binary);
    }

  }

  private void Verify(ShardKeyType kind) {
    byte[] bytes = null;
    ShardKey key = null;
    ShardKey result = null;
    ByteBuffer buffer = null;

    switch (kind) {
      case Int32:
        bytes = new byte[(Integer.SIZE / Byte.SIZE)];
        rValGen.nextBytes(bytes);
        buffer = ByteBuffer.wrap(bytes);
        buffer.order(ByteOrder.LITTLE_ENDIAN);
        int int32 = buffer.getInt();
        key = new ShardKey(int32);
        result = key.getNextKey();
        assert result.getIsMax() || result == new ShardKey(int32 + 1);
        break;

      case Int64:
        bytes = new byte[(Long.SIZE / Byte.SIZE)];
        rValGen.nextBytes(bytes);
        buffer = ByteBuffer.wrap(bytes);
        buffer.order(ByteOrder.LITTLE_ENDIAN);
        long int64 = buffer.getLong();
        key = new ShardKey(int64);
        result = key.getNextKey();
        assert result.getIsMax() || result == new ShardKey(int64 + 1);
        break;

      case Guid:
        UUID guid = UUID.randomUUID();
        key = new ShardKey(guid);
        result = key.getNextKey();
        // verify only the API call
        break;

      case Binary:
        bytes = new byte[128];
        rValGen.nextBytes(bytes);
        key = new ShardKey(bytes);
        result = key.getNextKey();
        // verify only the API call
        break;

      default:
        throw new IllegalArgumentException(Errors._ShardKey_UnsupportedShardKeyType);
    }
  }

}

package com.microsoft.azure.elasticdb.shard.unittests;

import com.microsoft.azure.elasticdb.shard.base.ShardKey;
import com.microsoft.azure.elasticdb.shard.base.ShardKeyType;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.UUID;

/**
 * Contains metadata for each ShardKeyType.
 */
public class ShardKeyTypeInfo {

  public static HashMap<ShardKeyType, ShardKeyTypeInfo> shardKeyTypeInfos =
      new HashMap<ShardKeyType, ShardKeyTypeInfo>() {
        {
          put(ShardKeyType.Int32, new ShardKeyTypeInfo(ShardKeyType.Int32, 4, Integer.MIN_VALUE,
              ShardKey.getMinInt(), ShardKey.getMaxInt()));
          put(ShardKeyType.Int64, new ShardKeyTypeInfo(ShardKeyType.Int64, 8, Long.MIN_VALUE,
              ShardKey.getMinLong(), ShardKey.getMaxLong()));
          put(ShardKeyType.Guid,
              new ShardKeyTypeInfo(ShardKeyType.Guid, 16,
                  UUID.fromString("00000000-0000-0000-0000-000000000000"), ShardKey.getMinGuid(),
                  ShardKey.getMaxGuid()));
          //TODO: put(ShardKeyType.Binary, new ShardKeyTypeInfo(ShardKeyType.Binary, 128, ));
          put(ShardKeyType.DateTimeOffset, new ShardKeyTypeInfo(ShardKeyType.DateTimeOffset, 16,
              LocalDateTime.MIN, ShardKey.getMinDateTimeOffset(), ShardKey.getMaxDateTimeOffset()));
          put(ShardKeyType.TimeSpan, new ShardKeyTypeInfo(ShardKeyType.TimeSpan, 8, Duration.ZERO,
              ShardKey.getMinTimeSpan(), ShardKey.getMaxTimeSpan()));
        }
      };

  public ShardKeyType keyType;

  public int length;

  public Object minValue;

  public ShardKey minShardKey;

  public ShardKey maxShardKey;

  public ShardKeyTypeInfo(ShardKeyType keyType, int length, Object minValue, ShardKey minShardKey,
      ShardKey maxShardKey) {
    this.keyType = keyType;
    this.length = length;
    this.minValue = minValue;
    this.minShardKey = minShardKey;
    this.maxShardKey = maxShardKey;
  }
}

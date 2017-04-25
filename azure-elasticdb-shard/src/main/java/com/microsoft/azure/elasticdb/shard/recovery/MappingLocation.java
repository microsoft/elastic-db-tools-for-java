package com.microsoft.azure.elasticdb.shard.recovery;

// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

/**
 * Location where the different mappings exist.
 */
public enum MappingLocation {
  /**
   * Mapping is present in global store, but absent on the shard.
   */
  MappingInShardMapOnly(0),

  /**
   * Mapping is absent in global store, but present on the shard.
   */
  MappingInShardOnly(1),

  /**
   * Mapping present at both global store and shard.
   */
  MappingInShardMapAndShard(2);

  public static final int SIZE = java.lang.Integer.SIZE;
  private static java.util.HashMap<Integer, MappingLocation> mappings;
  private int intValue;

  private MappingLocation(int value) {
    intValue = value;
    getMappings().put(value, this);
  }

  private static java.util.HashMap<Integer, MappingLocation> getMappings() {
    if (mappings == null) {
      synchronized (MappingLocation.class) {
        if (mappings == null) {
          mappings = new java.util.HashMap<Integer, MappingLocation>();
        }
      }
    }
    return mappings;
  }

  public static MappingLocation forValue(int value) {
    return getMappings().get(value);
  }

  public int getValue() {
    return intValue;
  }
}

package com.microsoft.azure.elasticdb.shard.recovery;

// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

/**
 * Resolution strategy for resolving mapping differences.
 */
public enum MappingDifferenceResolution {
  /**
   * Ignore the difference for now.
   */
  Ignore(0),

  /**
   * Use the mapping present in shard map.
   */
  KeepShardMapMapping(1),

  /**
   * Use the mapping in the shard.
   */
  KeepShardMapping(2);

  public static final int SIZE = java.lang.Integer.SIZE;
  private static java.util.HashMap<Integer, MappingDifferenceResolution> mappings;
  private int intValue;

  private MappingDifferenceResolution(int value) {
    intValue = value;
    getMappings().put(value, this);
  }

  private static java.util.HashMap<Integer, MappingDifferenceResolution> getMappings() {
    if (mappings == null) {
      synchronized (MappingDifferenceResolution.class) {
        if (mappings == null) {
          mappings = new java.util.HashMap<Integer, MappingDifferenceResolution>();
        }
      }
    }
    return mappings;
  }

  public static MappingDifferenceResolution forValue(int value) {
    return getMappings().get(value);
  }

  public int getValue() {
    return intValue;
  }
}

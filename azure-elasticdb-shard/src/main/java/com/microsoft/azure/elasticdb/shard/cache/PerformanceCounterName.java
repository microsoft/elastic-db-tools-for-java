package com.microsoft.azure.elasticdb.shard.cache;

// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

/**
 * Shard management performance counter names.
 */
public enum PerformanceCounterName {
  // counters to track cached mappings
  MappingsCount(0),
  MappingsAddOrUpdatePerSec(1),
  MappingsRemovePerSec(2),
  MappingsLookupSucceededPerSec(3),
  MappingsLookupFailedPerSec(4),
  // counters to track ShardManagement operations
  DdrOperationsPerSec(5);

  public static final int SIZE = java.lang.Integer.SIZE;
  private static java.util.HashMap<Integer, PerformanceCounterName> mappings;
  private int intValue;

  private PerformanceCounterName(int value) {
    intValue = value;
    getMappings().put(value, this);
  }

  private static java.util.HashMap<Integer, PerformanceCounterName> getMappings() {
    if (mappings == null) {
      synchronized (PerformanceCounterName.class) {
        if (mappings == null) {
          mappings = new java.util.HashMap<Integer, PerformanceCounterName>();
        }
      }
    }
    return mappings;
  }

  public static PerformanceCounterName forValue(int value) {
    return getMappings().get(value);
  }

  public int getValue() {
    return intValue;
  }
}

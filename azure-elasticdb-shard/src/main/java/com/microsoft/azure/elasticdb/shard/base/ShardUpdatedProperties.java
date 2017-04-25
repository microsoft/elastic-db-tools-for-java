package com.microsoft.azure.elasticdb.shard.base;

// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

/**
 * Records the updated properties on the shard.
 */
public enum ShardUpdatedProperties {
  Status(1),
  All(1);

  public static final int SIZE = java.lang.Integer.SIZE;
  private static java.util.HashMap<Integer, ShardUpdatedProperties> mappings;
  private int intValue;

  private ShardUpdatedProperties(int value) {
    intValue = value;
    getMappings().put(value, this);
  }

  private static java.util.HashMap<Integer, ShardUpdatedProperties> getMappings() {
    if (mappings == null) {
      synchronized (ShardUpdatedProperties.class) {
        if (mappings == null) {
          mappings = new java.util.HashMap<Integer, ShardUpdatedProperties>();
        }
      }
    }
    return mappings;
  }

  public static ShardUpdatedProperties forValue(int value) {
    return getMappings().get(value);
  }

  public int getValue() {
    return intValue;
  }
}

package com.microsoft.azure.elasticdb.shard.base;

/* Copyright (c) Microsoft. All rights reserved.
Licensed under the MIT license. See LICENSE file in the project root for full license information.*/

/**
 * Records the updated properties for a mapping update object.
 */
public enum MappingUpdatedProperties {
  Status(1),
  // Only applicable for point and range update.
  Shard(2),
  All(1 | 2);

  public static final int SIZE = java.lang.Integer.SIZE;
  private static java.util.HashMap<Integer, MappingUpdatedProperties> mappings;
  private int intValue;

  MappingUpdatedProperties(int value) {
    intValue = value;
    getMappings().put(value, this);
  }

  private static java.util.HashMap<Integer, MappingUpdatedProperties> getMappings() {
    if (mappings == null) {
      synchronized (MappingUpdatedProperties.class) {
        if (mappings == null) {
          mappings = new java.util.HashMap<>();
        }
      }
    }
    return mappings;
  }

  public static MappingUpdatedProperties forValue(int value) {
    return getMappings().get(value);
  }

  public int getValue() {
    return intValue;
  }
}

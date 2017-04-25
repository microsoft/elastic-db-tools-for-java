package com.microsoft.azure.elasticdb.shard.mapmanager;

/* Copyright (c) Microsoft. All rights reserved.
Licensed under the MIT license. See LICENSE file in the project root for full license information.*/

/**
 * Describes the creation options for shard map manager storage representation.
 */
public enum ShardMapManagerCreateMode {
  /**
   * If the shard map manager data structures are already present
   * in the store, then this method will raise exception.
   */
  KeepExisting(0),

  /**
   * If the shard map manager data structures are already present
   * in the store, then this method will overwrite them.
   */
  ReplaceExisting(1);

  public static final int SIZE = java.lang.Integer.SIZE;
  private static java.util.HashMap<Integer, ShardMapManagerCreateMode> mappings;
  private int intValue;

  private ShardMapManagerCreateMode(int value) {
    intValue = value;
    getMappings().put(value, this);
  }

  private static java.util.HashMap<Integer, ShardMapManagerCreateMode> getMappings() {
    if (mappings == null) {
      synchronized (ShardMapManagerCreateMode.class) {
        if (mappings == null) {
          mappings = new java.util.HashMap<Integer, ShardMapManagerCreateMode>();
        }
      }
    }
    return mappings;
  }

  public static ShardMapManagerCreateMode forValue(int value) {
    return getMappings().get(value);
  }

  public int getValue() {
    return intValue;
  }
}

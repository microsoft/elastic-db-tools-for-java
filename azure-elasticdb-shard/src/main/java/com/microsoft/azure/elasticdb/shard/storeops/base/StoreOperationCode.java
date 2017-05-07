package com.microsoft.azure.elasticdb.shard.storeops.base;

/* Copyright (c) Microsoft. All rights reserved.
Licensed under the MIT license. See LICENSE file in the project root for full license information.*/

import javax.xml.bind.annotation.XmlEnumValue;

/**
 * Operation codes identifying various store operations.
 */
public enum StoreOperationCode {
  @XmlEnumValue("1")
  AddShard(1),
  @XmlEnumValue("2")
  RemoveShard(2),
  @XmlEnumValue("3")
  UpdateShard(3),

  @XmlEnumValue("4")
  AddPointMapping(4),
  @XmlEnumValue("5")
  RemovePointMapping(5),
  @XmlEnumValue("6")
  UpdatePointMapping(6),
  @XmlEnumValue("7")
  UpdatePointMappingWithOffline(7),

  @XmlEnumValue("8")
  AddRangeMapping(8),
  @XmlEnumValue("9")
  RemoveRangeMapping(9),
  @XmlEnumValue("10")
  UpdateRangeMapping(10),
  @XmlEnumValue("11")
  UpdateRangeMappingWithOffline(11),

  @XmlEnumValue("14")
  SplitMapping(14),
  @XmlEnumValue("15")
  MergeMappings(15),

  @XmlEnumValue("16")
  AttachShard(16);

  public static final int SIZE = java.lang.Integer.SIZE;
  private static java.util.HashMap<Integer, StoreOperationCode> mappings;
  private int intValue;

  StoreOperationCode(int value) {
    intValue = value;
    getMappings().put(value, this);
  }

  private static java.util.HashMap<Integer, StoreOperationCode> getMappings() {
    if (mappings == null) {
      synchronized (StoreOperationCode.class) {
        if (mappings == null) {
          mappings = new java.util.HashMap<>();
        }
      }
    }
    return mappings;
  }

  public static StoreOperationCode forValue(int value) {
    return getMappings().get(value);
  }

  public int getValue() {
    return intValue;
  }
}

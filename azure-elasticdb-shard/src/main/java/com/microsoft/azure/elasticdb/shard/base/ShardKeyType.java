package com.microsoft.azure.elasticdb.shard.base;

/* Copyright (c) Microsoft. All rights reserved.
Licensed under the MIT license. See LICENSE file in the project root for full license information.*/

import javax.xml.bind.annotation.XmlEnum;
import javax.xml.bind.annotation.XmlEnumValue;

/**
 * Type of shard key. Currently, only Int32, Int64, Guid and byte[] are the data types supported as
 * shard keys.
 */
@XmlEnum
public enum ShardKeyType {
  /**
   * No type specified.
   */
  @XmlEnumValue("0")
  None(0, 0),

  /**
   * 32-bit integral value.
   */
  @XmlEnumValue("1")
  Int32(1, Integer.BYTES),

  /**
   * 64-bit integral value.
   */
  @XmlEnumValue("2")
  Int64(2, Long.BYTES),

  /**
   * UniqueIdentifier value.
   */
  @XmlEnumValue("3")
  Guid(3, ShardKey.SIZE_OF_GUID),

  /**
   * Array of bytes value.
   */
  @XmlEnumValue("4")
  Binary(4, ShardKey.MAXIMUM_VAR_BYTES_KEY_SIZE),

  /**
   * Date and time value.
   */
  @XmlEnumValue("5")
  DateTime(5, Long.BYTES),

  /**
   * Time value.
   */
  @XmlEnumValue("6")
  TimeSpan(6, Long.BYTES),

  /**
   * Date and time value with offset.
   */
  @XmlEnumValue("7")
  DateTimeOffset(7, ShardKey.SIZE_OF_DATE_TIME_OFFSET);

  private static java.util.HashMap<Integer, ShardKeyType> mappings;
  private int intValue;
  private int expectedByteArrayLength;

  ShardKeyType(int value, int expectedByteArrayLength) {
    intValue = value;
    this.expectedByteArrayLength = expectedByteArrayLength;
    getMappings().put(value, this);
  }

  private static java.util.HashMap<Integer, ShardKeyType> getMappings() {
    if (mappings == null) {
      synchronized (ShardKeyType.class) {
        if (mappings == null) {
          mappings = new java.util.HashMap<>();
        }
      }
    }
    return mappings;
  }

  public static ShardKeyType forValue(int value) {
    return getMappings().get(value);
  }

  public int getValue() {
    return intValue;
  }

  public int getByteArraySize() {
    return expectedByteArrayLength;
  }
}

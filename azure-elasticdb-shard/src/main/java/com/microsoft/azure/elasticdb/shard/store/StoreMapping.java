package com.microsoft.azure.elasticdb.shard.store;

/* Copyright (c) Microsoft. All rights reserved.
Licensed under the MIT license. See LICENSE file in the project root for full license information.*/

import com.microsoft.azure.elasticdb.shard.base.Shard;
import java.util.UUID;
import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlValue;

/**
 * Storage representation of a mapping b/w key ranges and shards.
 */
@XmlAccessorType(XmlAccessType.NONE)
public class StoreMapping {

  /**
   * Mapping Id.
   */
  @XmlElement(name = "Id")
  private UUID id;

  /**
   * Shard map Id.
   */
  @XmlElement(name = "ShardMapId")
  private UUID shardMapId;

  /**
   * min value.
   */
  @XmlElement(name = "MinValue")
  private BinaryValue minValue;

  /**
   * max value.
   */
  @XmlElement(name = "MaxValue")
  private BinaryValue maxValue;

  /**
   * Mapping status.
   */
  @XmlElement(name = "Status")
  private int status;

  /**
   * The lock owner id of the mapping.
   */
  private UUID lockOwnerId;

  /**
   * Shard referenced by mapping.
   */
  private StoreShard storeShard;

  public StoreMapping() {
  }

  /**
   * Constructs the storage representation from client side objects.
   *
   * @param id Identify of mapping.
   * @param s Shard being converted.
   * @param minValue min key value.
   * @param maxValue max key value.
   * @param status Mapping status.
   */
  public StoreMapping(UUID id, Shard s, byte[] minValue, byte[] maxValue, int status) {
    this(id, s.getShardMapId(), minValue, maxValue, status, null, s.getStoreShard());
  }

  /**
   * Constructs the storage representation from client side objects.
   *
   * @param id Identify of mapping.
   * @param shardMapId Id of parent shardmap.
   * @param storeShard StoreShard
   * @param minValue min key value.
   * @param maxValue max key value.
   * @param status Mapping status.
   * @param lockOwnerId Lock owner id.
   */
  public StoreMapping(UUID id, UUID shardMapId, byte[] minValue, byte[] maxValue, int status,
      UUID lockOwnerId, StoreShard storeShard) {
    this.setId(id);
    this.setShardMapId(shardMapId);
    this.setStoreShard(storeShard);
    this.setMinValue(minValue);
    this.setMaxValue(maxValue);
    this.setStatus(status);
    this.setLockOwnerId(lockOwnerId);
  }

  public UUID getId() {
    return id;
  }

  private void setId(UUID value) {
    id = value;
  }

  public UUID getShardMapId() {
    return shardMapId;
  }

  private void setShardMapId(UUID value) {
    shardMapId = value;
  }

  public byte[] getMinValue() {
    return minValue.getValue();
  }

  private void setMinValue(byte[] value) {
    minValue = new BinaryValue(value);
  }

  public byte[] getMaxValue() {
    return maxValue.getValue();
  }

  private void setMaxValue(byte[] value) {
    maxValue = new BinaryValue(value);
  }

  public int getStatus() {
    return status;
  }

  private void setStatus(int value) {
    status = value;
  }

  public UUID getLockOwnerId() {
    return lockOwnerId;
  }

  private void setLockOwnerId(UUID value) {
    lockOwnerId = value;
  }

  public StoreShard getStoreShard() {
    return storeShard;
  }

  private void setStoreShard(StoreShard value) {
    storeShard = value;
  }

  @XmlElement(name = "LockOwnerId")
  public String getLockOwnerIdString() {
    return this.lockOwnerId == null ? new UUID(0L, 0L).toString() : this.lockOwnerId.toString();
  }

  static class BinaryValue {

    private byte[] value;

    @XmlAttribute(name = "Null")
    private int isNull;

    BinaryValue() {
    }

    BinaryValue(byte[] value) {
      this.value = value;
      isNull = value == null ? 1 : 0;
    }

    public byte[] getValue() {
      return this.value;
    }

    @XmlValue
    public String getValueString() {
      return this.toString();
    }

    @Override
    public String toString() {
      if (this.value != null) {
        StringBuilder sb = new StringBuilder();
        int i = 0;
        sb.append("0x");
        for (byte b : this.value) {
          sb.append(String.format("%02x", b));
        }
        return sb.toString();
      } else {
        isNull = 1;
        return "";
      }
    }
  }
}
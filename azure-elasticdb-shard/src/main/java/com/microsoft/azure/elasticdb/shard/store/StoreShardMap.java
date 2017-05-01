package com.microsoft.azure.elasticdb.shard.store;

/* Copyright (c) Microsoft. All rights reserved.
Licensed under the MIT license. See LICENSE file in the project root for full license information.*/

import com.microsoft.azure.elasticdb.shard.base.ShardKeyType;
import com.microsoft.azure.elasticdb.shard.map.ShardMapType;
import java.util.UUID;
import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlElement;

/**
 * Store representation of a shard map.
 */
@XmlAccessorType(XmlAccessType.NONE)
public class StoreShardMap {

  public static final StoreShardMap NULL = new StoreShardMap(null, null, null, null, 1);
  /**
   * Shard map's identity.
   */
  @XmlElement(name = "Id")
  private UUID id;
  /**
   * Shard map name.
   */
  @XmlElement(name = "Name")
  private String name;
  /**
   * Type of shard map.
   */
  @XmlElement(name = "Kind")
  private ShardMapType shardMapType;
  /**
   * Key type.
   */
  @XmlElement(name = "KeyKind")
  private ShardKeyType shardKeyType;

  /**
   * This variable is used to identify 'null' vs 'non-null' value of shardMap in xml.
   * There is no business logic on top of this variable.
   */
  @XmlAttribute(name = "Null")
  private int isNull;

  public StoreShardMap() {
  }

  public StoreShardMap(UUID id, String name, ShardMapType shardMapType, ShardKeyType shardKeyType) {
    this(id, name, shardMapType, shardKeyType, 0);
  }

  StoreShardMap(UUID id, String name, ShardMapType shardMapType, ShardKeyType shardKeyType,
      int isNull) {
    this.id = id;
    this.name = name;
    this.shardKeyType = shardKeyType;
    this.shardMapType = shardMapType;
    this.isNull = isNull;
  }

  public UUID getId() {
    return this.id;
  }

  public String getName() {
    return this.name;
  }

  public ShardMapType getMapType() {
    return this.shardMapType;
  }

  public ShardKeyType getKeyType() {
    return this.shardKeyType;
  }

  public String toString() {
    return String.format("SM[%s:%s:%s]", shardMapType.name(), shardKeyType.name(), name);
  }
}
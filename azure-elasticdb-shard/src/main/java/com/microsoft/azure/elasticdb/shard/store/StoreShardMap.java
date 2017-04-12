package com.microsoft.azure.elasticdb.shard.store;

// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

import com.microsoft.azure.elasticdb.shard.base.ShardKeyType;
import com.microsoft.azure.elasticdb.shard.map.ShardMapType;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlElement;
import java.util.UUID;

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

    StoreShardMap(UUID id, String name, ShardMapType shardMapType, ShardKeyType shardKeyType, int isNull) {
        this.setId(id);
        this.setName(name);
        this.setShardMapType(shardMapType);
        this.setShardKeyType(shardKeyType);
        this.isNull = isNull;
    }

    public UUID getId() {
        return this.id;
    }

    private void setId(UUID id) {
        this.id = id;
    }

    public String getName() {
        return this.name;
    }

    private void setName(String name) {
        this.name = name;
    }

    public ShardMapType getMapType() {
        return this.shardMapType;
    }

    private void setShardMapType(ShardMapType shardMapType) {
        this.shardMapType = shardMapType;
    }

    public ShardKeyType getKeyType() {
        return this.shardKeyType;
    }

    private void setShardKeyType(ShardKeyType shardKeyType) {
        this.shardKeyType = shardKeyType;
    }
}
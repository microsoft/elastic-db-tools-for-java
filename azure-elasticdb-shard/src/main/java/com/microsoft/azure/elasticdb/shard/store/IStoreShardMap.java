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
public interface IStoreShardMap {
    /**
     * Shard map's identity.
     */
    @XmlElement(name = "Id")
    UUID getId();

    /**
     * Shard map name.
     */
    @XmlElement(name = "Name")
    String getName();

    /**
     * Type of shard map.
     */
    @XmlElement(name = "Kind")
    ShardMapType getMapType();

    /**
     * Key type.
     */
    @XmlElement(name = "KeyKind")
    ShardKeyType getKeyType();

    /**
     * This variable is used to identify 'null' vs 'non-null' value of shardMap in xml.
     * There is no business logic on top of this variable.
     */
    @XmlAttribute(name = "Null")
    int isNull();
}
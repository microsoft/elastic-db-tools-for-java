package com.microsoft.azure.elasticdb.shard.store;

// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

import com.microsoft.azure.elasticdb.shard.base.ShardLocation;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlElement;
import java.util.UUID;

/**
 * Storage representation of a single shard.
 */
@XmlAccessorType(XmlAccessType.NONE)
public interface IStoreShard {
    /**
     * Shard Id.
     */
    @XmlElement(name = "Id")
    UUID getId();

    /**
     * Shard version.
     */
    @XmlElement(name = "Version")
    UUID getVersion();

    /**
     * Containing shard map's Id.
     */
    @XmlElement(name = "SharedMapId")
    UUID getShardMapId();

    /**
     * Data source location.
     */
    @XmlElement(name = "Location")
    ShardLocation getLocation();

    /**
     * Shard status.
     */
    @XmlElement(name = "Status")
    int getStatus();

    /**
     * This variable is used to identify 'null' vs 'non-null' value of shardMap in xml.
     * There is no business logic on top of this variable.
     */
    @XmlAttribute(name = "Null")
    int isNull();
}
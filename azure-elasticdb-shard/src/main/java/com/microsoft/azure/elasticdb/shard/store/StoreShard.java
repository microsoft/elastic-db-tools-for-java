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
public class StoreShard {
    public static final StoreShard NULL = new StoreShard(null, null, null, null, null, 1);
    /**
     * Shard Id.
     */
    @XmlElement(name = "Id")
    private UUID Id;
    /**
     * Shard version.
     */
    @XmlElement(name = "Version")
    private UUID Version;
    /**
     * Containing shard map's Id.
     */
    @XmlElement(name = "SharedMapId")
    private UUID ShardMapId;
    /**
     * Data source location.
     */
    @XmlElement(name = "Location")
    private ShardLocation Location;
    /**
     * Shard status.
     */
    @XmlElement(name = "Status")
    private Integer Status;

    @XmlAttribute(name = "Null")
    private int isNull;

    public StoreShard(){}
    /**
     * Constructs the storage representation from client side objects.
     *
     * @param id         Shard Id.
     * @param version    Shard version.
     * @param shardMapId Identify of shard map.
     * @param location   Data source location.
     * @param status     Status of the shard.
     */
    public StoreShard(UUID id, UUID version, UUID shardMapId, ShardLocation location, Integer status) {
        this(id, version, shardMapId, location, status, 0);
    }

    StoreShard(UUID id, UUID version, UUID shardMapId, ShardLocation location, Integer status, int isNull) {
        Id = id;
        Version = version;
        ShardMapId = shardMapId;
        Location = location;
        Status = status;
        this.isNull = isNull;
    }

    public UUID getId() {
        return Id;
    }

    public UUID getVersion() {
        return Version;
    }

    public UUID getShardMapId() {
        return ShardMapId;
    }

    public ShardLocation getLocation() {
        return Location;
    }

    public int getStatus() {
        return Status;
    }

}
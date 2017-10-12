package com.microsoft.azure.elasticdb.shard.store;

import java.util.UUID;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlElement;

/*
 * Elastic database tools for Azure SQL Database.
 * 
 * Copyright(c) Microsoft Corporation All rights reserved.
 * 
 * This program is made available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

import com.microsoft.azure.elasticdb.shard.base.ShardLocation;

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
    private UUID id;
    /**
     * Shard version.
     */
    @XmlElement(name = "Version")
    private UUID version;
    /**
     * Containing shard map's Id.
     */
    @XmlElement(name = "ShardMapId")
    private UUID shardMapId;
    /**
     * Data source location.
     */
    @XmlElement(name = "Location")
    private ShardLocation location;
    /**
     * Shard status.
     */
    @XmlElement(name = "Status")
    private Integer status;

    @XmlAttribute(name = "Null")
    private int isNull;

    public StoreShard() {
    }

    /**
     * Constructs the storage representation from client side objects.
     *
     * @param id
     *            Shard Id.
     * @param version
     *            Shard version.
     * @param shardMapId
     *            Identify of shard map.
     * @param location
     *            Data source location.
     * @param status
     *            Status of the shard.
     */
    public StoreShard(UUID id,
            UUID version,
            UUID shardMapId,
            ShardLocation location,
            Integer status) {
        this(id, version, shardMapId, location, status, 0);
    }

    StoreShard(UUID id,
            UUID version,
            UUID shardMapId,
            ShardLocation location,
            Integer status,
            int isNull) {
        this.id = id;
        this.version = version;
        this.shardMapId = shardMapId;
        this.location = location;
        this.status = status;
        this.isNull = isNull;
    }

    public UUID getId() {
        return id;
    }

    public UUID getVersion() {
        return version;
    }

    public UUID getShardMapId() {
        return shardMapId;
    }

    public ShardLocation getLocation() {
        return location;
    }

    public int getStatus() {
        return status;
    }

}
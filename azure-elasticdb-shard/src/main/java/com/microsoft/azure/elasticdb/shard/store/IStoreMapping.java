package com.microsoft.azure.elasticdb.shard.store;

// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

import java.util.UUID;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;

/**
 * Storage representation of a mapping b/w key ranges and shards.
 */
@XmlAccessorType(XmlAccessType.NONE)
public interface IStoreMapping {
    /**
     * Mapping Id.
     */
	@XmlElement(name = "Id")
    UUID getId();

    /**
     * Shard map Id.
     */
	@XmlElement(name = "ShardMapId")
    UUID getShardMapId();

    /**
     * Min value.
     */
	@XmlElement(name = "MinValue")
    byte[] getMinValue();

    /**
     * Max value.
     */
	@XmlElement(name = "MaxValue")
    byte[] getMaxValue();

    /**
     * Mapping status.
     */
	@XmlElement(name = "Status")
    int getStatus();

    /**
     * Lock owner id of this mapping
     */
	@XmlElement(name = "LockOwnerId")
    UUID getLockOwnerId();

    /**
     * Shard referenced by mapping.
     */
	@XmlElement(name = "ShardMapId")
    IStoreShard getStoreShard();
}
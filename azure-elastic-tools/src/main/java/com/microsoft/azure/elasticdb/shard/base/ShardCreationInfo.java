package com.microsoft.azure.elasticdb.shard.base;

/*
 * Elastic database tools for Azure SQL Database.
 * 
 * Copyright(c) Microsoft Corporation All rights reserved.
 * 
 * This program is made available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

import com.microsoft.azure.elasticdb.shard.utils.ExceptionUtils;

/**
 * Arguments used to create a <see cref="Shard"/>.
 */
public final class ShardCreationInfo {

    /**
     * Gets Location of the shard.
     */
    private ShardLocation location;

    /**
     * Gets Status of the shard. Users can assign application-specific values to the status field, which are kept together with the shard for
     * convenience.
     */
    private ShardStatus status = ShardStatus.values()[0];

    /**
     * Arguments used to create a <see cref="Shard"/>.
     *
     * @param location
     *            Location of the shard.
     */
    public ShardCreationInfo(ShardLocation location) {
        this(location, ShardStatus.Online);
    }

    /**
     * Arguments used to create a <see cref="Shard"/>.
     *
     * @param location
     *            Location of the shard.
     * @param status
     *            Status of the shard.
     */
    public ShardCreationInfo(ShardLocation location,
            ShardStatus status) {
        ExceptionUtils.disallowNullArgument(location, "location");
        this.setLocation(location);
        this.setStatus(status);
    }

    public ShardLocation getLocation() {
        return location;
    }

    private void setLocation(ShardLocation value) {
        location = value;
    }

    public ShardStatus getStatus() {
        return status;
    }

    public void setStatus(ShardStatus value) {
        status = value;
    }
}
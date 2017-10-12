package com.microsoft.azure.elasticdb.shard.storeops.base;

/*
 * Elastic database tools for Azure SQL Database.
 * 
 * Copyright(c) Microsoft Corporation All rights reserved.
 * 
 * This program is made available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

import com.microsoft.azure.elasticdb.shard.base.ShardLocation;

/**
 * Provides information regarding LSM connections.
 */
public class StoreConnectionInfo {

    /**
     * Optional source shard location.
     */
    private ShardLocation sourceLocation;
    /**
     * Optional target shard location.
     */
    private ShardLocation targetLocation;

    public final ShardLocation getSourceLocation() {
        return sourceLocation;
    }

    public final void setSourceLocation(ShardLocation value) {
        sourceLocation = value;
    }

    public final ShardLocation getTargetLocation() {
        return targetLocation;
    }

    public final void setTargetLocation(ShardLocation value) {
        targetLocation = value;
    }
}

package com.microsoft.azure.elasticdb.shard.base;

/*
 * Elastic database tools for Azure SQL Database.
 * 
 * Copyright(c) Microsoft Corporation All rights reserved.
 * 
 * This program is made available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

/**
 * Represents updates to a Shard.
 */
public final class ShardUpdate {

    /**
     * Records the modified properties for update.
     */
    private ShardUpdatedProperties updatedProperties = ShardUpdatedProperties.forValue(0);

    /**
     * Holder for update to status property.
     */
    private ShardStatus status = ShardStatus.values()[0];

    /**
     * Instantiates the shard update object with no property set.
     */
    public ShardUpdate() {
    }

    /**
     * Get Status property.
     */
    public ShardStatus getStatus() {
        return status;
    }

    /**
     * Set Status property.
     */
    public void setStatus(ShardStatus value) {
        status = value;
        int shardUpdatePropertyValue = updatedProperties == null ? ShardUpdatedProperties.Status.getValue()
                : updatedProperties.getValue() | ShardUpdatedProperties.Status.getValue();
        updatedProperties = ShardUpdatedProperties.forValue(shardUpdatePropertyValue);
    }

    /**
     * Checks if any of the properties specified in the given bitmap have been set by the user.
     *
     * @param p
     *            Bitmap of properties.
     * @return True if any property is set, false otherwise.
     */
    public boolean isAnyPropertySet(ShardUpdatedProperties p) {
        return (updatedProperties.getValue() & p.getValue()) != 0;
    }
}
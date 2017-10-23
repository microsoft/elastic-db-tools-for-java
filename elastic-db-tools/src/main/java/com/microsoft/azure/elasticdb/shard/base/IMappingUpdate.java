package com.microsoft.azure.elasticdb.shard.base;

/*
 * Elastic database tools for Azure SQL Database.
 * 
 * Copyright(c) Microsoft Corporation All rights reserved.
 * 
 * This program is made available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

/**
 * Common interface for point/range mapping updates. <typeparam name="StatusT">Status type.</typeparam>
 */
public interface IMappingUpdate<StatusT> {

    /**
     * Status property.
     */
    StatusT getStatus();

    /**
     * Shard property.
     */
    Shard getShard();

    /**
     * Checks if any property is set in the given bitmap.
     *
     * @param properties
     *            Properties bitmap.
     * @return True if any of the properties is set, false otherwise.
     */
    boolean isAnyPropertySet(MappingUpdatedProperties properties);

    /**
     * Checks if the mapping is being taken offline.
     *
     * @param originalStatus
     *            Original status.
     * @return True of the update will take the mapping offline.
     */
    boolean isMappingBeingTakenOffline(StatusT originalStatus);
}
package com.microsoft.azure.elasticdb.shard.base;

/*
 * Elastic database tools for Azure SQL Database.
 * 
 * Copyright(c) Microsoft Corporation All rights reserved.
 * 
 * This program is made available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

/**
 * Represents updates to a mapping between the singleton key value of a shardlet (a point) and the shard that holds its data. Also see
 * <see cref="PointMapping{KeyT}"/>.
 */
public final class PointMappingUpdate extends BaseMappingUpdate<MappingStatus> {

    /**
     * Instantiates a new point mapping update object.
     */
    public PointMappingUpdate() {
        super();
    }

    /**
     * Detects if the current mapping is being taken offline.
     *
     * @param originalStatus
     *            Original status.
     * @param updatedStatus
     *            Updated status.
     * @return Detects in the derived types if the mapping is being taken offline.
     */
    @Override
    protected boolean isBeingTakenOffline(MappingStatus originalStatus,
            MappingStatus updatedStatus) {
        return originalStatus == MappingStatus.Online && updatedStatus == MappingStatus.Offline;
    }
}
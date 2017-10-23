package com.microsoft.azure.elasticdb.shard.recovery;

/*
 * Elastic database tools for Azure SQL Database.
 * 
 * Copyright(c) Microsoft Corporation All rights reserved.
 * 
 * This program is made available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

import com.microsoft.azure.elasticdb.shard.store.StoreMapping;
import com.microsoft.azure.elasticdb.shard.store.StoreShardMap;

/**
 * Class for mapping differences.
 */
public class MappingDifference {

    /**
     * Type of mapping difference. Either List or Range.
     */
    private MappingDifferenceType type;

    /**
     * Location where the mappings that differ exist.
     */
    private MappingLocation location;

    /**
     * ShardMap which has the consistency violation.
     */
    private StoreShardMap shardMap;

    /**
     * Mapping found in shard map.
     */
    private StoreMapping mappingForShardMap;

    /**
     * Mapping found in shard.
     */
    private StoreMapping mappingForShard;

    /**
     * Creates an Instance of Mapping Difference.
     *
     * @param type
     *            Mapping Difference Type
     * @param location
     *            Mapping Location
     * @param shardMap
     *            Shard Map
     * @param mappingForShardMap
     *            Store Mapping for Shard Map
     * @param mappingForShard
     *            Store Mapping for Shard
     */
    public MappingDifference(MappingDifferenceType type,
            MappingLocation location,
            StoreShardMap shardMap,
            StoreMapping mappingForShardMap,
            StoreMapping mappingForShard) {
        this.setType(type);
        this.setLocation(location);
        this.setShardMap(shardMap);
        this.setMappingForShardMap(mappingForShardMap);
        this.setMappingForShard(mappingForShard);
    }

    public final MappingDifferenceType getType() {
        return type;
    }

    private void setType(MappingDifferenceType value) {
        type = value;
    }

    public final MappingLocation getLocation() {
        return location;
    }

    private void setLocation(MappingLocation value) {
        location = value;
    }

    public final StoreShardMap getShardMap() {
        return shardMap;
    }

    private void setShardMap(StoreShardMap value) {
        shardMap = value;
    }

    public final StoreMapping getMappingForShardMap() {
        return mappingForShardMap;
    }

    private void setMappingForShardMap(StoreMapping value) {
        mappingForShardMap = value;
    }

    public final StoreMapping getMappingForShard() {
        return mappingForShard;
    }

    private void setMappingForShard(StoreMapping value) {
        mappingForShard = value;
    }
}
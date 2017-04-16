package com.microsoft.azure.elasticdb.shard.recovery;

// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

import com.microsoft.azure.elasticdb.shard.base.ShardRange;
import com.microsoft.azure.elasticdb.shard.store.StoreMapping;
import com.microsoft.azure.elasticdb.shard.store.StoreShardMap;

/**
 * Result of comparison b/w the given range mappings.
 */
public class MappingComparisonResult {
    /**
     * Shard map to which mappings belong.
     */
    private StoreShardMap ShardMap;
    /**
     * Current range.
     */
    private ShardRange Range;
    /**
     * Location of the mapping.
     */
    private MappingLocation MappingLocation;
    /**
     * Mappings corresponding to current range in GSM.
     */
    private StoreMapping ShardMapManagerMapping;
    /**
     * Mapping corresponding to current range in LSM.
     */
    private StoreMapping ShardMapping;

    /**
     * Instantiates a new instance of range mapping comparison result.
     *
     * @param ssm             Store representation of shard map.
     * @param range           Range being considered.
     * @param mappingLocation Location of mapping.
     * @param gsmMapping      Storage representation of GSM mapping.
     * @param lsmMapping      Storange representation of LSM mapping.
     */
    public MappingComparisonResult(StoreShardMap ssm, ShardRange range, MappingLocation mappingLocation, StoreMapping gsmMapping, StoreMapping lsmMapping) {
        this.setShardMap(ssm);
        this.setRange(range);
        this.setMappingLocation(mappingLocation);
        this.setShardMapManagerMapping(gsmMapping);
        this.setShardMapping(lsmMapping);
    }

    public final StoreShardMap getShardMap() {
        return ShardMap;
    }

    public final void setShardMap(StoreShardMap value) {
        ShardMap = value;
    }

    public final ShardRange getRange() {
        return Range;
    }

    public final void setRange(ShardRange value) {
        Range = value;
    }

    public final MappingLocation getMappingLocation() {
        return MappingLocation;
    }

    public final void setMappingLocation(MappingLocation value) {
        MappingLocation = value;
    }

    public final StoreMapping getShardMapManagerMapping() {
        return ShardMapManagerMapping;
    }

    public final void setShardMapManagerMapping(StoreMapping value) {
        ShardMapManagerMapping = value;
    }

    public final StoreMapping getShardMapping() {
        return ShardMapping;
    }

    public final void setShardMapping(StoreMapping value) {
        ShardMapping = value;
    }
}
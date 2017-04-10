package com.microsoft.azure.elasticdb.shard.recovery;

// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

import com.microsoft.azure.elasticdb.shard.base.ShardRange;
import com.microsoft.azure.elasticdb.shard.store.IStoreMapping;
import com.microsoft.azure.elasticdb.shard.store.IStoreShardMap;

/**
 * Result of comparison b/w the given range mappings.
 */
public class MappingComparisonResult {
    /**
     * Shard map to which mappings belong.
     */
    private IStoreShardMap ShardMap;
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
    private IStoreMapping ShardMapManagerMapping;
    /**
     * Mapping corresponding to current range in LSM.
     */
    private IStoreMapping ShardMapping;

    /**
     * Instantiates a new instance of range mapping comparison result.
     *
     * @param ssm             Store representation of shard map.
     * @param range           Range being considered.
     * @param mappingLocation Location of mapping.
     * @param gsmMapping      Storage representation of GSM mapping.
     * @param lsmMapping      Storange representation of LSM mapping.
     */
    public MappingComparisonResult(IStoreShardMap ssm, ShardRange range, MappingLocation mappingLocation, IStoreMapping gsmMapping, IStoreMapping lsmMapping) {
        this.setShardMap(ssm);
        this.setRange(range);
        this.setMappingLocation(mappingLocation);
        this.setShardMapManagerMapping(gsmMapping);
        this.setShardMapping(lsmMapping);
    }

    public final IStoreShardMap getShardMap() {
        return ShardMap;
    }

    public final void setShardMap(IStoreShardMap value) {
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

    public final IStoreMapping getShardMapManagerMapping() {
        return ShardMapManagerMapping;
    }

    public final void setShardMapManagerMapping(IStoreMapping value) {
        ShardMapManagerMapping = value;
    }

    public final IStoreMapping getShardMapping() {
        return ShardMapping;
    }

    public final void setShardMapping(IStoreMapping value) {
        ShardMapping = value;
    }
}
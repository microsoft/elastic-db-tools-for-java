package com.microsoft.azure.elasticdb.shardmapscalability;

/*
 * Copyright (c) Microsoft. All rights reserved. Licensed under the MIT license. See LICENSE file in the project root for full license information.
 */

import com.microsoft.azure.elasticdb.shard.base.Range;
import com.microsoft.azure.elasticdb.shard.base.Shard;
import com.microsoft.azure.elasticdb.shard.base.ShardKeyType;
import com.microsoft.azure.elasticdb.shard.map.RangeShardMap;
import com.microsoft.azure.elasticdb.shard.mapmanager.ShardManagementErrorCode;
import com.microsoft.azure.elasticdb.shard.mapmanager.ShardManagementException;
import com.microsoft.azure.elasticdb.shard.mapmanager.ShardMapManager;

public class RangeShardMapOperations extends ShardMapOperations<Integer> {

    RangeShardMapOperations(ShardMapManager smm,
            String shardMapName) {
        // Create the shard map, if it doesn't already exist
        try {
            setShardMap(smm.createRangeShardMap(shardMapName, ShardKeyType.Int32));
            System.out.printf("Created Shard Map %1$s" + "\r\n", getShardMap().getName());
        }
        catch (ShardManagementException e) {
            if (e.getErrorCode().equals(ShardManagementErrorCode.ShardMapAlreadyExists)) {
                System.out.println("Shard Map already exists");
                setShardMap(smm.getRangeShardMap(shardMapName, ShardKeyType.Int32));
            }
            else {
                throw e;
            }
        }
    }

    @Override
    protected int getCurrentMappingCountInternal() {
        return ((RangeShardMap) getShardMap()).getMappings().size();
    }

    @Override
    protected void createMappingInternal(Integer key,
            Shard shard) {
        ((RangeShardMap) getShardMap()).createRangeMapping(new Range(key, key + 1), shard);
    }

    @Override
    public void lookupMapping(Integer key) {
        ((RangeShardMap) getShardMap()).getMappingForKey(key);
    }
}

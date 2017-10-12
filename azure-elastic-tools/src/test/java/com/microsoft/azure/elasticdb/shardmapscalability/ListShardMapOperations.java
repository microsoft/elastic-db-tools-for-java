package com.microsoft.azure.elasticdb.shardmapscalability;

/*
 * Copyright (c) Microsoft. All rights reserved. Licensed under the MIT license. See LICENSE file in the project root for full license information.
 */

import com.microsoft.azure.elasticdb.shard.base.Shard;
import com.microsoft.azure.elasticdb.shard.base.ShardKeyType;
import com.microsoft.azure.elasticdb.shard.map.ListShardMap;
import com.microsoft.azure.elasticdb.shard.mapmanager.ShardManagementErrorCode;
import com.microsoft.azure.elasticdb.shard.mapmanager.ShardManagementException;
import com.microsoft.azure.elasticdb.shard.mapmanager.ShardMapManager;

public class ListShardMapOperations extends ShardMapOperations<Integer> {

    ListShardMapOperations(ShardMapManager smm,
            String shardMapName) {
        // Create the shard map, if it doesn't already exist
        try {
            setShardMap(smm.createListShardMap(shardMapName, ShardKeyType.Int32));
            System.out.printf("Created Shard Map %1$s" + "\r\n", getShardMap().getName());
        }
        catch (ShardManagementException e) {
            if (e.getErrorCode().equals(ShardManagementErrorCode.ShardMapAlreadyExists)) {
                System.out.println("Shard Map already exists");
                setShardMap(smm.getListShardMap(shardMapName, ShardKeyType.Int32));
            }
            else {
                throw e;
            }
        }
    }

    @Override
    protected int getCurrentMappingCountInternal() {
        return ((ListShardMap) getShardMap()).getMappings().size();
    }

    @Override
    protected void createMappingInternal(Integer key,
            Shard shard) {
        ((ListShardMap) getShardMap()).createPointMapping(key, shard);
    }

    @Override
    public void lookupMapping(Integer key) {
        ((ListShardMap) getShardMap()).getMappingForKey(key);
    }
}
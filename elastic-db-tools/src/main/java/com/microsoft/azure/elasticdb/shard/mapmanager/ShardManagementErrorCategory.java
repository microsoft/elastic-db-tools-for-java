package com.microsoft.azure.elasticdb.shard.mapmanager;

/*
 * Elastic database tools for Azure SQL Database.
 * 
 * Copyright(c) Microsoft Corporation All rights reserved.
 * 
 * This program is made available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

/**
 * Represents error categories related to Shard Management operations.
 */
public enum ShardManagementErrorCategory {
    /**
     * ShardMap manager factory.
     */
    ShardMapManagerFactory,

    /**
     * ShardMap manager.
     */
    ShardMapManager,

    /**
     * ShardMap.
     */
    ShardMap,

    /**
     * List shard map.
     */
    ListShardMap,

    /**
     * Range shard map.
     */
    RangeShardMap,

    /**
     * Version validation.
     */
    Validation,

    /**
     * Recovery oriented errors.
     */
    Recovery,

    /**
     * Errors related to Schema Info Collection.
     */
    SchemaInfoCollection,

    /**
     * General failure category.
     */
    General;
}

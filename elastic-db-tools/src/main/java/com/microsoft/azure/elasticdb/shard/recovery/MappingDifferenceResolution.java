package com.microsoft.azure.elasticdb.shard.recovery;

/*
 * Elastic database tools for Azure SQL Database.
 * 
 * Copyright(c) Microsoft Corporation All rights reserved.
 * 
 * This program is made available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

/**
 * Resolution strategy for resolving mapping differences.
 */
public enum MappingDifferenceResolution {
    /**
     * Ignore the difference for now.
     */
    Ignore,

    /**
     * Use the mapping present in shard map.
     */
    KeepShardMapMapping,

    /**
     * Use the mapping in the shard.
     */
    KeepShardMapping;

}

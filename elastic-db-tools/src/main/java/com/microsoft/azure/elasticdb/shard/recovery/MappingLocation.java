package com.microsoft.azure.elasticdb.shard.recovery;

/*
 * Elastic database tools for Azure SQL Database.
 * 
 * Copyright(c) Microsoft Corporation All rights reserved.
 * 
 * This program is made available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

/**
 * Location where the different mappings exist.
 */
public enum MappingLocation {
    /**
     * Mapping is present in global store, but absent on the shard.
     */
    MappingInShardMapOnly,

    /**
     * Mapping is absent in global store, but present on the shard.
     */
    MappingInShardOnly,

    /**
     * Mapping present at both global store and shard.
     */
    MappingInShardMapAndShard;

}

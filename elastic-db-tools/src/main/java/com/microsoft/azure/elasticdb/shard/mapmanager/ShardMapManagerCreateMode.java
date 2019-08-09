package com.microsoft.azure.elasticdb.shard.mapmanager;

/*
 * Elastic database tools for Azure SQL Database.
 * 
 * Copyright(c) Microsoft Corporation All rights reserved.
 * 
 * This program is made available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

/**
 * Describes the creation options for shard map manager storage representation.
 */
public enum ShardMapManagerCreateMode {
    /**
     * If the shard map manager data structures are already present in the store, then this method will raise exception.
     */
    KeepExisting,

    /**
     * If the shard map manager data structures are already present in the store, then this method will overwrite them.
     */
    ReplaceExisting;

}

package com.microsoft.azure.elasticdb.shard.base;

import java.util.UUID;

/*
 * Elastic database tools for Azure SQL Database.
 * 
 * Copyright(c) Microsoft Corporation All rights reserved.
 * 
 * This program is made available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

import com.microsoft.azure.elasticdb.shard.mapmanager.ShardMapManager;
import com.microsoft.azure.elasticdb.shard.store.StoreMapping;

/**
 * Interface that represents capability to provide information relevant to Add/Remove/Update operations for a mapping object.
 */
public interface IMappingInfoProvider {

    /**
     * ShardMapManager for the object.
     */
    ShardMapManager getShardMapManager();

    /**
     * Shard map associated with the mapping.
     */
    UUID getShardMapId();

    /**
     * Storage representation of the mapping.
     */
    StoreMapping getStoreMapping();

    /**
     * Type of the mapping.
     */
    MappingKind getKind();

    /**
     * Mapping type, useful for diagnostics.
     */
    String getTypeName();
}
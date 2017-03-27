package com.microsoft.azure.elasticdb.shard.base;

// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

import com.microsoft.azure.elasticdb.shard.mapmanager.ShardMapManager;
import com.microsoft.azure.elasticdb.shard.store.IStoreMapping;

import java.util.*;

/**
 * Interface that represents capability to provide information
 * relavant to Add/Remove/Update operations for a mapping object.
 */
public interface IMappingInfoProvider {
    /**
     * ShardMapManager for the object.
     */
    ShardMapManager getManager();

    /**
     * Shard map associated with the mapping.
     */
    UUID getShardMapId();

    /**
     * Storage representation of the mapping.
     */
    IStoreMapping getStoreMapping();

    /**
     * Type of the mapping.
     */
    MappingKind getKind();

    /**
     * Mapping type, useful for diagnostics.
     */
    String getTypeName();
}
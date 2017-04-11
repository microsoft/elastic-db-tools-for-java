package com.microsoft.azure.elasticdb.shard.store;

// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

import java.util.List;

/**
 * Representation of storage results from storage API execution.
 */
public interface IStoreResults {
    /**
     * Storage operation result.
     */
    StoreResult getResult();

    /**
     * Collection of shard maps.
     */
    List<StoreShardMap> getStoreShardMaps();

    /**
     * Collection of shards.
     */
    List<StoreShard> getStoreShards();

    /**
     * Collection of mappings.
     */
    List<StoreMapping> getStoreMappings();

    /**
     * Collection of locations.
     */
    List<IStoreLocation> getStoreLocations();

    /**
     * Collection of operations.
     */
    List<IStoreLogEntry> getStoreOperations();

    /**
     * Collection of SchemaInfo objects.
     */
    List<IStoreSchemaInfo> getStoreSchemaInfoCollection();

    /**
     * Version of store.
     */
    IStoreVersion getStoreVersion();
}
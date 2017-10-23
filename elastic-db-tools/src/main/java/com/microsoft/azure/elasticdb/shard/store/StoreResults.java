package com.microsoft.azure.elasticdb.shard.store;

import java.util.ArrayList;
import java.util.List;

/*
 * Elastic database tools for Azure SQL Database.
 * 
 * Copyright(c) Microsoft Corporation All rights reserved.
 * 
 * This program is made available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

import com.microsoft.azure.elasticdb.shard.base.ShardLocation;

/**
 * Representation of storage results from storage API execution.
 */
public class StoreResults {

    /**
     * Collection of shard maps in result.
     */
    private ArrayList<StoreShardMap> listStoreShardMap;
    /**
     * Collection of shards in result.
     */
    private ArrayList<StoreShard> listStoreShard;
    /**
     * Collection of shard mappings in result.
     */
    private ArrayList<StoreMapping> listStoreMappings;
    /**
     * Collection of shard locations in result.
     */
    private ArrayList<ShardLocation> listShardLocations;
    /**
     * Collection of store operations in result.
     */
    private ArrayList<StoreLogEntry> listStoreLogEntries;
    /**
     * Collection of Schema info in result.
     */
    private ArrayList<StoreSchemaInfo> listStoreSchemaInfo;
    /**
     * Version of global or local shard map in result.
     */
    private Version version;
    /**
     * Storage operation result.
     */
    private StoreResult result;

    /**
     * Constructs instance of SqlResults.
     */
    public StoreResults() {
        result = StoreResult.Success;
        listStoreShardMap = new ArrayList<>();
        listStoreShard = new ArrayList<>();
        listStoreMappings = new ArrayList<>();
        listShardLocations = new ArrayList<>();
        listStoreSchemaInfo = new ArrayList<>();
        version = null;
        listStoreLogEntries = new ArrayList<>();
    }

    public StoreResult getResult() {
        return result;
    }

    public void setResult(StoreResult result) {
        this.result = result;
    }

    /**
     * Collection of shard maps.
     */
    public List<StoreShardMap> getStoreShardMaps() {
        return listStoreShardMap;
    }

    /**
     * Collection of shards.
     */
    public List<StoreShard> getStoreShards() {
        return listStoreShard;
    }

    /**
     * Collection of mappings.
     */
    public List<StoreMapping> getStoreMappings() {
        return listStoreMappings;
    }

    /**
     * Collection of store operations.
     */
    public List<StoreLogEntry> getStoreOperations() {
        return listStoreLogEntries;
    }

    /**
     * Collection of locations.
     */
    public List<ShardLocation> getStoreLocations() {
        return listShardLocations;
    }

    /**
     * Collection of SchemaInfo objects.
     */
    public List<StoreSchemaInfo> getStoreSchemaInfoCollection() {
        return listStoreSchemaInfo;
    }

    /**
     * Store version.
     */
    public Version getStoreVersion() {
        return version;
    }

    public void setStoreVersion(Version version) {
        this.version = version;
    }

    public List<StoreLogEntry> getLogEntries() {
        return listStoreLogEntries;
    }
}
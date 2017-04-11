package com.microsoft.azure.elasticdb.shard.store;

// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

import com.microsoft.azure.elasticdb.shard.base.ShardLocation;

import java.util.ArrayList;
import java.util.List;

/**
 * Representation of storage results from storage API execution.
 */
public class StoreResults {

    /**
     * Collection of shard maps in result.
     */
    private ArrayList<StoreShardMap> _ssm;
    /**
     * Collection of shards in result.
     */
    private ArrayList<StoreShard> _ss;
    /**
     * Collection of shard mappings in result.
     */
    private ArrayList<StoreMapping> _sm;
    /**
     * Collection of shard locations in result.
     */
    private ArrayList<ShardLocation> _sl;
    /**
     * Collection of store operations in result.
     */
    private ArrayList<StoreLogEntry> _ops;
    /**
     * Collection of Schema info in result.
     */
    private ArrayList<StoreSchemaInfo> _si;
    /**
     * Version of global or local shard map in result.
     */
    private Version _version;
    /**
     * Storage operation result.
     */
    private StoreResult Result;

    /**
     * Constructs instance of SqlResults.
     */
    public StoreResults() {
        Result = StoreResult.Success;
        _ssm = new ArrayList<>();
        _ss = new ArrayList<>();
        _sm = new ArrayList<>();
        _sl = new ArrayList<>();
        _si = new ArrayList<>();
        _version = null;
        _ops = new ArrayList<>();
    }

    public StoreResult getResult() {
        return Result;
    }

    public void setResult(StoreResult result) {
        Result = result;
    }

    /**
     * Collection of shard maps.
     */
    public List<StoreShardMap> getStoreShardMaps() {
        return _ssm;
    }

    /**
     * Collection of shards.
     */
    public List<StoreShard> getStoreShards() {
        return _ss;
    }

    /**
     * Collection of mappings.
     */
    public List<StoreMapping> getStoreMappings() {
        return _sm;
    }

    /**
     * Collection of store operations.
     */
    public List<StoreLogEntry> getStoreOperations() {
        return _ops;
    }

    /**
     * Collection of locations.
     */
    public List<ShardLocation> getStoreLocations() {
        return _sl;
    }

    /**
     * Collection of SchemaInfo objects.
     */
    public List<StoreSchemaInfo> getStoreSchemaInfoCollection() {
        return _si;
    }

    /**
     * Store version.
     */
    public Version getStoreVersion() {
        return _version;
    }

    public void setStoreVersion(Version version) {
        this._version = version;
    }

    public List<StoreLogEntry> getLogEntries() {
        return _ops;
    }
}
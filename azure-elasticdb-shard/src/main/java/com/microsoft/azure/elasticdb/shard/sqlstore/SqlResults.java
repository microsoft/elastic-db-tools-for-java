package com.microsoft.azure.elasticdb.shard.sqlstore;

// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

import com.microsoft.azure.elasticdb.shard.store.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 * Container for results of Store operations.
 */
public final class SqlResults implements IStoreResults {
    /**
     * Mapping from column name to result type.
     */
    private static HashMap<String, SqlResultType> s_resultFromColumnName = new HashMap<String, SqlResultType>();

    static {
        s_resultFromColumnName.put("ShardMapId", SqlResultType.ShardMap);
        s_resultFromColumnName.put("ShardId", SqlResultType.Shard);
        s_resultFromColumnName.put("MappingId", SqlResultType.ShardMapping);
        s_resultFromColumnName.put("Protocol", SqlResultType.ShardLocation);
        s_resultFromColumnName.put("StoreVersion", SqlResultType.StoreVersion);
        s_resultFromColumnName.put("StoreVersionMajor", SqlResultType.StoreVersion);
        s_resultFromColumnName.put("Name", SqlResultType.SchemaInfo);
        s_resultFromColumnName.put("OperationId", SqlResultType.Operation);
    }

    /**
     * Collection of shard maps in result.
     */
    private ArrayList<IStoreShardMap> _ssm;
    /**
     * Collection of shards in result.
     */
    private ArrayList<IStoreShard> _ss;
    /**
     * Collection of shard mappings in result.
     */
    private ArrayList<IStoreMapping> _sm;
    /**
     * Collection of shard locations in result.
     */
    private ArrayList<IStoreLocation> _sl;
    /**
     * Collection of store operations in result.
     */
    private ArrayList<IStoreLogEntry> _ops;
    /**
     * Collection of Schema info in result.
     */
    private ArrayList<IStoreSchemaInfo> _si;
    /**
     * Version of global or local shard map in result.
     */
    private IStoreVersion _version;
    /**
     * Storage operation result.
     */
    private StoreResult Result;

    /**
     * Constructs instance of SqlResults.
     */
    public SqlResults() {
        this.setResult(StoreResult.Success);

        _ssm = new ArrayList<IStoreShardMap>();
        _ss = new ArrayList<IStoreShard>();
        _sm = new ArrayList<IStoreMapping>();
        _sl = new ArrayList<IStoreLocation>();
        _si = new ArrayList<IStoreSchemaInfo>();
        _version = null;
        _ops = new ArrayList<IStoreLogEntry>();
    }

    /**
     * Obtains the result type from first column's name.
     *
     * @param columnName First column's name.
     * @return Sql result type.
     */
    private static SqlResultType SqlResultTypeFromColumnName(String columnName) {
        return s_resultFromColumnName.get(columnName);
    }

    public StoreResult getResult() {
        return Result;
    }

    public void setResult(StoreResult value) {
        Result = value;
    }

    /**
     * Collection of shard maps.
     */
    public List<IStoreShardMap> getStoreShardMaps() {
        return _ssm;
    }

    /**
     * Collection of shards.
     */
    public List<IStoreShard> getStoreShards() {
        return _ss;
    }

    /**
     * Collection of mappings.
     */
    public List<IStoreMapping> getStoreMappings() {
        return _sm;
    }

    /**
     * Collection of store operations.
     */
    public List<IStoreLogEntry> getStoreOperations() {
        return _ops;
    }

    /**
     * Collection of locations.
     */
    public List<IStoreLocation> getStoreLocations() {
        return _sl;
    }

    /**
     * Collection of SchemaInfo objects.
     */
    public List<IStoreSchemaInfo> getStoreSchemaInfoCollection() {
        return _si;
    }

    /**
     * Store version.
     */
    public IStoreVersion getStoreVersion() {
        return _version;
    }

    /**
     * Kinds of results from storage operations.
     */
    private enum SqlResultType {
        ShardMap(0),
        Shard(1),
        ShardMapping(2),
        ShardLocation(3),
        StoreVersion(4),
        Operation(5),
        SchemaInfo(6);

        public static final int SIZE = java.lang.Integer.SIZE;
        private static java.util.HashMap<Integer, SqlResultType> mappings;
        private int intValue;

        private SqlResultType(int value) {
            intValue = value;
            getMappings().put(value, this);
        }

        private static java.util.HashMap<Integer, SqlResultType> getMappings() {
            if (mappings == null) {
                synchronized (SqlResultType.class) {
                    if (mappings == null) {
                        mappings = new java.util.HashMap<Integer, SqlResultType>();
                    }
                }
            }
            return mappings;
        }

        public static SqlResultType forValue(int value) {
            return getMappings().get(value);
        }

        public int getValue() {
            return intValue;
        }
    }
}
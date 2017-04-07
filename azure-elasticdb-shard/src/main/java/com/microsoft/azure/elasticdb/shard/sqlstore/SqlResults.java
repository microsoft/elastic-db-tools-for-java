package com.microsoft.azure.elasticdb.shard.sqlstore;

// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

import com.microsoft.azure.elasticdb.shard.store.*;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

/**
 * Container for results of Store operations.
 */
public final class SqlResults implements IStoreResults {

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
    private static class SqlResultType {
        static final String ShardMapId = "ShardMapId";
        static final String ShardId = "ShardId";
        static final String MappingId = "MappingId";
        static final String Protocol = "Protocol";
        static final String StoreVersion = "StoreVersion";
        static final String StoreVersionMajor = "StoreVersionMajor";
        static final String Name = "Name";
        static final String OperationId = "OperationId";
    }

    /// <summary>
    /// Populates instance of SqlResults using rows from SqlDataReader.
    /// </summary>
    /// <param name="reader">SqlDataReader whose rows are to be read.</param>
    public void fetch(Statement statement) throws SQLException {
        do {
            ResultSet rs = statement.getResultSet();
            if (!rs.next()) { // move to first row.
                continue;
            }
            String resultType = rs.getString("ColumnName");
            switch (resultType) {
                case SqlResultType.ShardMapId:
                    do {
                        _ssm.add(new SqlShardMap(rs, 1));
                    } while (rs.next());
                    break;
                case SqlResultType.ShardId:
                    do {
                        _ss.add(new SqlShard(rs, 1));
                    } while (rs.next());
                    break;
                case SqlResultType.MappingId:
                    do {
                        _sm.add(new SqlMapping(rs, 1));
                    } while (rs.next());
                    break;
                case SqlResultType.Protocol:
                    do {
                        _sl.add(new SqlLocation(rs, 1));
                    } while (rs.next());
                    break;
                case SqlResultType.Name:
                    do {
                        _si.add(new SqlSchemaInfo(rs, 1));
                    } while (rs.next());
                    break;
                case SqlResultType.StoreVersion:
                case SqlResultType.StoreVersionMajor:
                    do {
                        _version = new SqlVersion(rs, 1);
                    } while (rs.next());
                    break;
                case SqlResultType.OperationId:
                    do {
                        _ops.add(new SqlLogEntry(rs, 1));
                    } while (rs.next());
                    break;
                default:
                    break;
            }
        } while(statement.getMoreResults());
    }
}
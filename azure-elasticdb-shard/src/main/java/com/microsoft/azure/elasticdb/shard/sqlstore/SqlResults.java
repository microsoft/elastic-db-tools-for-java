package com.microsoft.azure.elasticdb.shard.sqlstore;

// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

import com.microsoft.azure.elasticdb.shard.store.*;

import java.sql.CallableStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.Callable;

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
    private ArrayList<IStoreLocation> _sl;
    /**
     * Collection of store operations in result.
     */
    private ArrayList<IStoreLogEntry> _ops;
    /**
     * Collection of Schema info in result.
     */
    private ArrayList<StoreSchemaInfo> _si;
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

        _ssm = new ArrayList<StoreShardMap>();
        _ss = new ArrayList<StoreShard>();
        _sm = new ArrayList<StoreMapping>();
        _sl = new ArrayList<IStoreLocation>();
        _si = new ArrayList<StoreSchemaInfo>();
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

    /**
     * Populates instance of SqlResults using rows from SqlDataReader.
     *
     * @param rs SqlDataReader whose rows are to be read.
     */
    public void Fetch(ResultSet rs) {
        try {
            do {
                if (!rs.next()) { // move to first row.
                    continue;
                }
                //TODO Make this generic
                SqlResultType resultType = s_resultFromColumnName.get(rs.getMetaData().getColumnLabel(2));
                switch (resultType) {
                    case ShardMap:
                        do {
                            _ssm.add(new SqlShardMap(rs, 1));
                        } while (rs.next());
                        break;
                    case Shard:
                        do {
                            _ss.add(SqlShard.newInstance(rs, 1));
                        } while (rs.next());
                        break;
                    case Mapping:
                        do {
                            _sm.add(new SqlMapping(rs, 1));
                        } while (rs.next());
                        break;
                    case Protocol:
                        do {
                            _sl.add(new SqlLocation(rs, 1));
                        } while (rs.next());
                        break;
                    case Name:
                        do {
                            _si.add(new SqlSchemaInfo(rs, 1));
                        } while (rs.next());
                        break;
                    case StoreVersion:
                    case StoreVersionMajor:
                        do {
                            _version = new SqlVersion(rs, 2);
                        } while (rs.next());
                        break;
                    case Operation:
                        do {
                            _ops.add(new SqlLogEntry(rs, 1));
                        } while (rs.next());
                        break;
                    default:
                        break;
                }
            } while (rs.next());
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    /**
     * Asynchronously populates instance of SqlResults using rows from SqlDataReader.
     *
     * @param statement CallableStatement whose rows are to be read.
     * @return A task to await read completion
     */
    public Callable FetchAsync(CallableStatement statement) throws SQLException {
        do {
            ResultSet rs = statement.getResultSet();
            if (!rs.next()) { // move to first row.
                continue;
            }
            SqlResultType resultType = s_resultFromColumnName.get(rs.getString("ColumnName"));
            switch (resultType) {
                case ShardMap:
                    do {
                        _ssm.add(new SqlShardMap(rs, 1));
                    } while (rs.next());
                    break;
                case Shard:
                    do {
                        _ss.add(SqlShard.newInstance(rs, 1));
                    } while (rs.next());
                    break;
                case Mapping:
                    do {
                        _sm.add(new SqlMapping(rs, 1));
                    } while (rs.next());
                    break;
                case Protocol:
                    do {
                        _sl.add(new SqlLocation(rs, 1));
                    } while (rs.next());
                    break;
                case Name:
                    do {
                        _si.add(new SqlSchemaInfo(rs, 1));
                    } while (rs.next());
                    break;
                case StoreVersion:
                case StoreVersionMajor:
                    do {
                        _version = new SqlVersion(rs, 1);
                    } while (rs.next());
                    break;
                case Operation:
                    do {
                        _ops.add(new SqlLogEntry(rs, 1));
                    } while (rs.next());
                    break;
                default:
                    break;
            }
        } while (statement.getMoreResults());
        return null;
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
    public List<StoreSchemaInfo> getStoreSchemaInfoCollection() {
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
        SchemaInfo(6),
        Protocol(7),
        Mapping(8),
        Name(9),
        StoreVersionMajor(10);

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
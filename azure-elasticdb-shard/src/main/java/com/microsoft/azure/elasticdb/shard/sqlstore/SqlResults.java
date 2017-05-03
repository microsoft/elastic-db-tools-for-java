package com.microsoft.azure.elasticdb.shard.sqlstore;

/* Copyright (c) Microsoft. All rights reserved.
Licensed under the MIT license. See LICENSE file in the project root for full license information.*/

import com.microsoft.azure.elasticdb.shard.base.ShardKeyType;
import com.microsoft.azure.elasticdb.shard.base.ShardLocation;
import com.microsoft.azure.elasticdb.shard.base.SqlProtocol;
import com.microsoft.azure.elasticdb.shard.map.ShardMapType;
import com.microsoft.azure.elasticdb.shard.schema.SchemaInfo;
import com.microsoft.azure.elasticdb.shard.store.StoreLogEntry;
import com.microsoft.azure.elasticdb.shard.store.StoreMapping;
import com.microsoft.azure.elasticdb.shard.store.StoreResults;
import com.microsoft.azure.elasticdb.shard.store.StoreSchemaInfo;
import com.microsoft.azure.elasticdb.shard.store.StoreShard;
import com.microsoft.azure.elasticdb.shard.store.StoreShardMap;
import com.microsoft.azure.elasticdb.shard.store.Version;
import com.microsoft.azure.elasticdb.shard.storeops.base.StoreOperationCode;
import com.microsoft.azure.elasticdb.shard.storeops.base.StoreOperationState;
import java.sql.CallableStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.UUID;
import java.util.concurrent.Callable;

/**
 * Container for results of Store operations.
 */
public final class SqlResults {

  /**
   * Mapping from column name to result type.
   */
  private static HashMap<String, SqlResultType> s_resultFromColumnName = new HashMap<>();

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
   * Populates instance of SqlResults using rows from SqlDataReader.
   *
   * @param cstmt SqlDataReader whose rows are to be read.
   */
  public static StoreResults newInstance(CallableStatement cstmt) {
    StoreResults storeResults = new StoreResults();
    try {
      do {
        ResultSet rs = cstmt.getResultSet();
        if (rs == null) {
          return storeResults;
        }
        if (!rs.next()) { // move to first row.
          continue;
        }
        //TODO Make this generic
        SqlResultType resultType = s_resultFromColumnName.get(rs.getMetaData().getColumnLabel(2));
        switch (resultType) {
          case ShardMap:
            do {
              //TODO: Use builder to add entries into list.
              storeResults.getStoreShardMaps().add(readShardMap(rs, 2));
            } while (rs.next());
            break;
          case Shard:
            do {
              storeResults.getStoreShards().add(readShard(rs, 2));
            } while (rs.next());
            break;
          case ShardMapping:
            do {
              storeResults.getStoreMappings().add(readMapping(rs, 2));
            } while (rs.next());
            break;
          case ShardLocation:
            do {
              storeResults.getStoreLocations().add(readLocation(rs, 2));
            } while (rs.next());
            break;
          case SchemaInfo:
            do {
              storeResults.getStoreSchemaInfoCollection().add(readSchemaInfo(rs, 2));
            } while (rs.next());
            break;
          case StoreVersion:
            do {
              storeResults.setStoreVersion(readVersion(rs, 2));
            } while (rs.next());
            break;
          case Operation:
            do {
              storeResults.getLogEntries().add(readLogEntry(rs, 2));
            } while (rs.next());
            break;
          default:
            break;
        }
      } while (cstmt.getMoreResults());

    } catch (SQLException e) {
      e.printStackTrace();
    }
    return storeResults;
  }

  /**
   * Asynchronously populates instance of SqlResults using rows from SqlDataReader.
   *
   * @param statement CallableStatement whose rows are to be read.
   * @return A task to await read completion
   */
  public static Callable fetchAsync(CallableStatement statement) {
    return () -> newInstance(statement);
  }

  /**
   * Constructs an instance of Version using parts of a row from SqlDataReader.
   *
   * @param rs SqlDataReader whose row has shard information.
   * @param offset Reader offset for column that begins shard information.
   */
  public static Version readVersion(ResultSet rs, int offset) throws SQLException {
    int major = rs.getInt(offset);
    int minor = (rs.getMetaData().getColumnCount() > offset) ? rs.getInt(offset + 1) : 0;
    return new Version(major, minor);
  }

  /**
   * Constructs an instance of ShardLocation using parts of a row from SqlDataReader.
   * Used for creating the shard location instance.
   *
   * @param reader SqlDataReader whose row has shard information.
   * @param offset Reader offset for column that begins shard information.
   */
  public static ShardLocation readLocation(ResultSet reader, int offset) throws SQLException {
    return new ShardLocation(
        reader.getString(offset + 1),
        reader.getString(offset + 3),
        SqlProtocol.forValue(reader.getInt(offset)),
        reader.getInt(offset + 2));
  }

  /**
   * Constructs an instance of StoreShard using parts of a row from SqlDataReader.
   * Used for creating the shard instance for a mapping.
   *
   * @param reader SqlDataReader whose row has shard information.
   * @param offset Reader offset for column that begins shard information.
   */
  public static StoreShard readShard(ResultSet reader, int offset) throws SQLException {
    return new StoreShard(UUID.fromString(reader.getString((offset))),
        UUID.fromString(reader.getString(offset + 1)),
        UUID.fromString(reader.getString(offset + 2)),
        SqlResults.readLocation(reader, offset + 3),
        reader.getInt(offset + 7));
  }

  /**
   * Constructs an instance of StoreShardMap using a row from SqlDataReader starting at specified
   * offset.
   *
   * @param reader SqlDataReader whose row has shard map information.
   * @param offset Reader offset for column that begins shard map information..
   */
  public static StoreShardMap readShardMap(ResultSet reader, int offset) throws SQLException {
    return new StoreShardMap(UUID.fromString(reader.getString(offset)),
        reader.getString(offset + 1),
        ShardMapType.forValue(reader.getInt(offset + 2)),
        ShardKeyType.forValue(reader.getInt(offset + 3)));
  }

  /**
   * Constructs an instance of StoreMapping using a row from SqlDataReader.
   *
   * @param reader SqlDataReader whose row has mapping information.
   * @param offset Reader offset for column that begins mapping information.
   */
  public static StoreMapping readMapping(ResultSet reader, int offset) throws SQLException {
    return new StoreMapping(UUID.fromString(reader.getString(offset)),
        UUID.fromString(reader.getString(offset + 1)),
        reader.getBytes(offset + 2),
        reader.getBytes(offset + 3),
        reader.getInt(offset + 4),
        UUID.fromString(reader.getString(offset + 5)),
        readShard(reader, offset + 6)
    );
  }

  /**
   * Constructs an instance of StoreSchemaInfo using parts of a row from SqlDataReader.
   *
   * @param reader SqlDataReader whose row has shard information.
   * @param offset Reader offset for column that begins shard information.
   */
  public static StoreSchemaInfo readSchemaInfo(ResultSet reader, int offset) throws SQLException {
    return new StoreSchemaInfo(reader.getString(offset), new SchemaInfo(reader, offset + 1));
  }

  /**
   * Constructs an instance of StoreLogEntry using parts of a row from SqlDataReader.
   * Used for creating the store operation for Undo.
   *
   * @param reader SqlDataReader whose row has operation information.
   * @param offset Reader offset for column that begins operation information.
   */
  public static StoreLogEntry readLogEntry(ResultSet reader, int offset) throws SQLException {
    UUID shardIdRemoves = UUID.fromString(reader.getString(offset + 4));
    UUID shardIdAdds = UUID.fromString(reader.getString(offset + 5));
    return new StoreLogEntry(UUID.fromString(reader.getString(offset)),
        StoreOperationCode.forValue(reader.getInt(offset + 1)),
        reader.getSQLXML(offset + 2),
        StoreOperationState.forValue(reader.getInt(offset + 3)),
        shardIdRemoves.compareTo(new UUID(0L, 0L)) == 0 ? null : shardIdRemoves,
        shardIdAdds.compareTo(new UUID(0L, 0L)) == 0 ? null : shardIdAdds);
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

    SqlResultType(int value) {
      intValue = value;
      getMappings().put(value, this);
    }

    private static java.util.HashMap<Integer, SqlResultType> getMappings() {
      if (mappings == null) {
        synchronized (SqlResultType.class) {
          if (mappings == null) {
            mappings = new java.util.HashMap<>();
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
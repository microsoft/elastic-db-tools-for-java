package com.microsoft.azure.elasticdb.shard.sqlstore;

import java.io.IOException;
import java.sql.CallableStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.xml.sax.SAXException;

import com.microsoft.azure.elasticdb.core.commons.helpers.EnumHelpers;
import com.microsoft.azure.elasticdb.core.commons.helpers.MappableEnum;

/*
 * Elastic database tools for Azure SQL Database.
 * 
 * Copyright(c) Microsoft Corporation All rights reserved.
 * 
 * This program is made available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

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
import com.microsoft.azure.elasticdb.shard.utils.StringUtilsLocal;

/**
 * Container for results of Store operations.
 */
public final class SqlResults {

    /**
     * Mapping from column name to result type.
     */
    private static HashMap<String, SqlResultType> resultFromColumnName = new HashMap<>();

    static {
        resultFromColumnName.put("ShardMapId", SqlResultType.ShardMap);
        resultFromColumnName.put("ShardId", SqlResultType.Shard);
        resultFromColumnName.put("MappingId", SqlResultType.ShardMapping);
        resultFromColumnName.put("Protocol", SqlResultType.ShardLocation);
        resultFromColumnName.put("StoreVersion", SqlResultType.StoreVersion);
        resultFromColumnName.put("StoreVersionMajor", SqlResultType.StoreVersion);
        resultFromColumnName.put("Name", SqlResultType.SchemaInfo);
        resultFromColumnName.put("OperationId", SqlResultType.Operation);
    }

    /**
     * Populates instance of SqlResults using rows from ResultSet.
     *
     * @param cstmt
     *            Statement whose rows are to be read.
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
                SqlResultType resultType = resultFromColumnName.get(rs.getMetaData().getColumnLabel(2));
                switch (resultType) {
                    case ShardMap:
                        do {
                            storeResults.getStoreShardMaps().add(readShardMap(rs, 2));
                        }
                        while (rs.next());
                        break;
                    case Shard:
                        do {
                            storeResults.getStoreShards().add(readShard(rs, 2));
                        }
                        while (rs.next());
                        break;
                    case ShardMapping:
                        do {
                            storeResults.getStoreMappings().add(readMapping(rs, 2));
                        }
                        while (rs.next());
                        break;
                    case ShardLocation:
                        do {
                            storeResults.getStoreLocations().add(readLocation(rs, 2));
                        }
                        while (rs.next());
                        break;
                    case SchemaInfo:
                        do {
                            storeResults.getStoreSchemaInfoCollection().add(readSchemaInfo(rs, 2));
                        }
                        while (rs.next());
                        break;
                    case StoreVersion:
                        do {
                            storeResults.setStoreVersion(readVersion(rs, 2));
                        }
                        while (rs.next());
                        break;
                    case Operation:
                        do {
                            storeResults.getLogEntries().add(readLogEntry(rs, 2));
                        }
                        while (rs.next());
                        break;
                    default:
                        break;
                }
            }
            while (cstmt.getMoreResults());

        }
        catch (SQLException e) {
            e.printStackTrace();
        }
        return storeResults;
    }

    /**
     * Constructs an instance of Version using parts of a row from ResultSet.
     *
     * @param rs
     *            ResultSet whose row has shard information.
     * @param offset
     *            Reader offset for column that begins shard information.
     */
    public static Version readVersion(ResultSet rs,
            int offset) throws SQLException {
        int major = rs.getInt(offset);
        int minor = (rs.getMetaData().getColumnCount() > offset) ? rs.getInt(offset + 1) : 0;
        return new Version(major, minor);
    }

    /**
     * Constructs an instance of ShardLocation using parts of a row from ResultSet. Used for creating the shard location instance.
     *
     * @param reader
     *            ResultSet whose row has shard information.
     * @param offset
     *            Reader offset for column that begins shard information.
     */
    public static ShardLocation readLocation(ResultSet reader,
            int offset) throws SQLException {
        return new ShardLocation(reader.getString(offset + 1), reader.getString(offset + 3), SqlProtocol.forValue(reader.getInt(offset)),
                reader.getInt(offset + 2));
    }

    /**
     * Constructs an instance of StoreShard using parts of a row from ResultSet. Used for creating the shard instance for a mapping.
     *
     * @param reader
     *            ResultSet whose row has shard information.
     * @param offset
     *            Reader offset for column that begins shard information.
     */
    public static StoreShard readShard(ResultSet reader,
            int offset) throws SQLException {
        return new StoreShard(UUID.fromString(reader.getString((offset))), UUID.fromString(reader.getString(offset + 1)),
                UUID.fromString(reader.getString(offset + 2)), SqlResults.readLocation(reader, offset + 3), reader.getInt(offset + 7));
    }

    /**
     * Constructs an instance of StoreShardMap using a row from ResultSet starting at specified offset.
     *
     * @param reader
     *            ResultSet whose row has shard map information.
     * @param offset
     *            Reader offset for column that begins shard map information..
     */
    public static StoreShardMap readShardMap(ResultSet reader,
            int offset) throws SQLException {
        return new StoreShardMap(UUID.fromString(reader.getString(offset)), reader.getString(offset + 1),
                ShardMapType.forValue(reader.getInt(offset + 2)), ShardKeyType.forValue(reader.getInt(offset + 3)));
    }

    /**
     * Constructs an instance of StoreMapping using a row from ResultSet.
     *
     * @param reader
     *            ResultSet whose row has mapping information.
     * @param offset
     *            Reader offset for column that begins mapping information.
     */
    public static StoreMapping readMapping(ResultSet reader,
            int offset) throws SQLException {
        return new StoreMapping(UUID.fromString(reader.getString(offset)), UUID.fromString(reader.getString(offset + 1)), reader.getBytes(offset + 2),
                reader.getBytes(offset + 3), reader.getInt(offset + 4), UUID.fromString(reader.getString(offset + 5)), readShard(reader, offset + 6));
    }

    /**
     * Constructs an instance of StoreSchemaInfo using parts of a row from ResultSet.
     *
     * @param reader
     *            ResultSet whose row has shard information.
     * @param offset
     *            Reader offset for column that begins shard information.
     */
    public static StoreSchemaInfo readSchemaInfo(ResultSet reader,
            int offset) throws SQLException {
        return new StoreSchemaInfo(reader.getString(offset), new SchemaInfo(reader, offset + 1));
    }

    /**
     * Constructs an instance of StoreLogEntry using parts of a row from ResultSet. Used for creating the store operation for Undo.
     *
     * @param reader
     *            ResultSet whose row has operation information.
     * @param offset
     *            Reader offset for column that begins operation information.
     */
    public static StoreLogEntry readLogEntry(ResultSet reader,
            int offset) throws SQLException {
        try {
            UUID shardIdRemoves = StringUtilsLocal.isNullOrEmpty(reader.getString(offset + 4)) ? null : UUID.fromString(reader.getString(offset + 4));
            UUID shardIdAdds = StringUtilsLocal.isNullOrEmpty(reader.getString(offset + 5)) ? null : UUID.fromString(reader.getString(offset + 5));
            Document doc = DocumentBuilderFactory.newInstance().newDocumentBuilder().parse(reader.getSQLXML(offset + 2).getBinaryStream());

            return new StoreLogEntry(UUID.fromString(reader.getString(offset)), StoreOperationCode.forValue(reader.getInt(offset + 1)),
                    (Element) doc.getFirstChild(), StoreOperationState.forValue(reader.getInt(offset + 3)), shardIdRemoves, shardIdAdds);
        }
        catch (SAXException | IOException | ParserConfigurationException e) {
            e.printStackTrace();
            return null;
        }
    }

    /**
     * Kinds of results from storage operations.
     */
    private enum SqlResultType{
        ShardMap,
        Shard,
        ShardMapping,
        ShardLocation,
        StoreVersion,
        Operation,
        SchemaInfo;

    }

}
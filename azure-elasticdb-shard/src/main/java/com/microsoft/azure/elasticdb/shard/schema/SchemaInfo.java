package com.microsoft.azure.elasticdb.shard.schema;

// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

import com.microsoft.azure.elasticdb.core.commons.helpers.ReferenceObjectHelper;
import com.microsoft.azure.elasticdb.shard.utils.Errors;
import com.microsoft.azure.elasticdb.shard.utils.ExceptionUtils;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlElement;
import java.io.ObjectInputStream;
import java.io.Serializable;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.Set;

/**
 * Represents information identifying the list of sharded tables and the list of reference
 * tables associated with a sharding scheme. Reference tables are replicated across shards.
 * This class is thread safe.
 */
@XmlAccessorType(XmlAccessType.NONE)
public class SchemaInfo implements Serializable {
    @XmlAttribute(name = "xmlns:i")
    private String xmlns = "http://www.w3.org/2001/XMLSchema-instance";

    /**
     * This is the list of sharded tables in the sharding schema along with their
     * sharding key column names.
     */
    @XmlElement(name = "ShardedTableSet")
    private ShardedTableSet _shardedTables;

    /**
     * EDCL v1.1.0 accidentally emitted the "ShardedTableSet" DataMember with the name "_shardedTableSet".
     * This DataMember allows us to easily deserialize this incorrectly named field without needing
     * to write custom deserialization logic.
     */
    private ShardedTableSet _shardedTablesAlternateName;

    /**
     * This is the list of reference tables in the sharding scheme.
     */
    @XmlElement(name = "ReferenceTableSet")
    private ReferenceTableSet _referenceTables;

    /**
     * EDCL v1.1.0 accidentally emitted the "ReferenceTableSet" DataMember with the name "_referenceTableSet".
     * This DataMember allows us to easily deserialize this incorrectly named field without needing
     * to write custom deserialization logic.
     */
    private ReferenceTableSet _referenceTablesAlternateName;

    /**
     * Synchronization object used when adding table entries to the current
     * <see cref="SchemaInfo"/> object.
     */
    private Object _syncObject;

    /**
     * Initializes a new instance of the <see cref="SchemaInfo"/> class.
     */
    public SchemaInfo() {
        Initialize();
    }

    public SchemaInfo(ResultSet reader, int offset) {
        try {
            if (reader.getMetaData().getColumnCount() > offset) {
                //TODO: read the reader and populate local variables
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        //TODO: Remove after testing
        HashSet<ReferenceTableInfo> refSet = new HashSet<>();
        refSet.add(new ReferenceTableInfo("Regions"));
        refSet.add(new ReferenceTableInfo("Products"));

        HashSet<ShardedTableInfo> shardSet = new HashSet<>();
        shardSet.add(new ShardedTableInfo("Customers", "CustomerId"));
        shardSet.add(new ShardedTableInfo("Orders", "CustomerId"));

        this._referenceTables = new ReferenceTableSet(refSet);
        this._shardedTables = new ShardedTableSet(shardSet);
    }

    /**
     * Read-only list of information concerning all sharded tables.
     */
    public final Set<ShardedTableInfo> getShardedTables() {
        return _shardedTables.getShardedSet();
    }

    /**
     * Initialize any non-DataMember objects post deserialization.
     */
    private void SetValuesOnDeserialized(ObjectInputStream context) {
        Initialize();
    }

    /**
     * Read-only list of information concerning all reference tables.
     */
    public final Set<ReferenceTableInfo> getReferenceTables() {
        return _referenceTables.getReferenceSet();
    }

    /**
     * Initializes this instance after construction or deserialization.
     */
    private void Initialize() {
        // If _shardedTables is null after deserialization, then set it to _shardedTablesAlternateName
        // instead (in case we deserialized the v1.1.0 format). If that is also null, then just set
        // it to an empty HashSet.
        _shardedTables = (_shardedTables != null) ? _shardedTables : (_shardedTablesAlternateName != null) ? _shardedTablesAlternateName : new ShardedTableSet();
        // Null out _shardedTablesAlternateName so that we don't serialize it back
        _shardedTablesAlternateName = null;

        // Same as above for _referenceTables
        _referenceTables = (_referenceTables != null) ? _referenceTables :
                (_referenceTablesAlternateName != null) ? _referenceTablesAlternateName : new ReferenceTableSet();
        _referenceTablesAlternateName = null;

        _syncObject = new Object();
    }

    /**
     * Adds information about a sharded table.
     *
     * @param shardedTableInfo Sharded table info.
     */
    public final void Add(ShardedTableInfo shardedTableInfo) {
        ExceptionUtils.<ShardedTableInfo>DisallowNullArgument(shardedTableInfo, "shardedTableInfo");

        String existingTableType = null;

        synchronized (_syncObject) {
            ReferenceObjectHelper<String> tempRef_existingTableType = new ReferenceObjectHelper<String>(existingTableType);
            if (CheckIfTableExists(shardedTableInfo, tempRef_existingTableType)) {
                existingTableType = tempRef_existingTableType.argValue;
                throw new SchemaInfoException(SchemaInfoErrorCode.TableInfoAlreadyPresent, Errors._SchemaInfo_TableInfoAlreadyExists, existingTableType, shardedTableInfo.getSchemaName(), shardedTableInfo.getTableName());
            } else {
                existingTableType = tempRef_existingTableType.argValue;
            }

            int initialSize;
            if (_shardedTables == null || _shardedTables.getShardedSet() == null) {
                initialSize = 0;
                HashSet<ShardedTableInfo> set = new HashSet<>();
                set.add(shardedTableInfo);
                _shardedTables = new ShardedTableSet(set);
            } else {
                initialSize = _shardedTables.getShardedSet().size();
                _shardedTables.getShardedSet().add(shardedTableInfo);
            }
            boolean result = _shardedTables.getShardedSet().size() - 1 == initialSize;
            // Adding to the sharded table set shouldn't fail since we have done all necessary
            // verification apriori.
            //Debug.Assert(result, "Addition of new sharded table info failed.");
        }
    }

    /**
     * Adds information about a reference table.
     *
     * @param referenceTableInfo Reference table info.
     */
    public final void Add(ReferenceTableInfo referenceTableInfo) {
        ExceptionUtils.<ReferenceTableInfo>DisallowNullArgument(referenceTableInfo, "referenceTableInfo");

        String existingTableType = null;

        synchronized (_syncObject) {
            ReferenceObjectHelper<String> tempRef_existingTableType = new ReferenceObjectHelper<String>(existingTableType);
            if (CheckIfTableExists(referenceTableInfo, tempRef_existingTableType)) {
                existingTableType = tempRef_existingTableType.argValue;
                throw new SchemaInfoException(SchemaInfoErrorCode.TableInfoAlreadyPresent, Errors._SchemaInfo_TableInfoAlreadyExists, existingTableType, referenceTableInfo.getSchemaName(), referenceTableInfo.getTableName());
            } else {
                existingTableType = tempRef_existingTableType.argValue;
            }

            int initialSize;
            if (_referenceTables == null || _referenceTables.getReferenceSet() == null) {
                initialSize = 0;
                HashSet<ReferenceTableInfo> set = new HashSet<>();
                set.add(referenceTableInfo);
                _referenceTables = new ReferenceTableSet(set);
            } else {
                initialSize = _referenceTables.getReferenceSet().size();
                _referenceTables.getReferenceSet().add(referenceTableInfo);
            }
            boolean result = _referenceTables.getReferenceSet().size() - 1 == initialSize;
            // Adding to the reference table set shouldn't fail since we have done all necessary
            // verification apriori.
            //Debug.Assert(result, "Addition of new sharded table info failed.");
        }
    }

    /**
     * Removes information about a sharded table.
     *
     * @param shardedTableInfo Sharded table info.
     */
    public final boolean Remove(ShardedTableInfo shardedTableInfo) {
        return _shardedTables.getShardedSet().remove(shardedTableInfo);
    }

    /**
     * Removes information about a reference table.
     *
     * @param referenceTableInfo Reference table info.
     */
    public final boolean Remove(ReferenceTableInfo referenceTableInfo) {
        return _referenceTables.getReferenceSet().remove(referenceTableInfo);
    }

    /**
     * Check is either a sharded table or a reference table exists by the given name.
     *
     * @param tableInfo Sharded or reference table info.
     * @param tableType sharded, reference or null.
     * @return boolean
     */
    private boolean CheckIfTableExists(TableInfo tableInfo, ReferenceObjectHelper<String> tableType) {
        tableType.argValue = null;

        if (this._shardedTables.getShardedSet() != null && this._shardedTables.getShardedSet().stream().anyMatch(s -> s.getSchemaName().equalsIgnoreCase(tableInfo.getSchemaName()) && s.getTableName().equalsIgnoreCase(tableInfo.getTableName()))) {
            tableType.argValue = "sharded";
            return true;
        }

        if (this._referenceTables.getReferenceSet() != null && this._referenceTables.getReferenceSet().stream().anyMatch(r -> r.getSchemaName().equalsIgnoreCase(r.getSchemaName()) && r.getTableName().equalsIgnoreCase(tableInfo.getTableName()))) {
            tableType.argValue = "reference";
            return true;
        }

        return false;
    }

    @XmlAccessorType(XmlAccessType.NONE)
    static class ReferenceTableSet {

        @XmlElement(name = "ReferenceTableInfo")
        private HashSet<ReferenceTableInfo> referenceSet;
        @XmlAttribute(name = "i:type")
        private String type = "ArrayOfReferenceTableInfo";

        ReferenceTableSet() {
            this.referenceSet = new HashSet<>();
        }

        ReferenceTableSet(HashSet<ReferenceTableInfo> set) {
            this.referenceSet = set;
        }

        HashSet<ReferenceTableInfo> getReferenceSet() {
            return this.referenceSet;
        }
    }

    @XmlAccessorType(XmlAccessType.NONE)
    static class ShardedTableSet {

        @XmlElement(name = "ShardedTableInfo")
        private HashSet<ShardedTableInfo> shardedSet;
        @XmlAttribute(name = "i:type")
        private String type = "ArrayOfShardedTableInfo";

        ShardedTableSet() {
            this.shardedSet = new HashSet<>();
        }

        ShardedTableSet(HashSet<ShardedTableInfo> set) {
            this.shardedSet = set;
        }

        HashSet<ShardedTableInfo> getShardedSet() {
            return this.shardedSet;
        }
    }
}


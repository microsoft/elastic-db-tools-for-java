package com.microsoft.azure.elasticdb.shard.schema;

import java.io.Serializable;
import java.io.StringReader;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashSet;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Unmarshaller;
import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;

/*
 * Elastic database tools for Azure SQL Database.
 * 
 * Copyright(c) Microsoft Corporation All rights reserved.
 * 
 * This program is made available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

import com.microsoft.azure.elasticdb.core.commons.helpers.ReferenceObjectHelper;
import com.microsoft.azure.elasticdb.shard.utils.Errors;
import com.microsoft.azure.elasticdb.shard.utils.ExceptionUtils;

/**
 * Represents information identifying the list of sharded tables and the list of reference tables associated with a sharding scheme. Reference tables
 * are replicated across shards. This class is thread safe.
 */
@XmlAccessorType(XmlAccessType.NONE)
@XmlRootElement(name = "Schema")
public class SchemaInfo implements Serializable {

    @XmlAttribute(name = "xmlns:i")
    private String xmlns = "http://www.w3.org/2001/XMLSchema-instance";

    /**
     * This is the list of reference tables in the sharding scheme.
     */
    @XmlElement(name = "ReferenceTableSet")
    private ReferenceTableSet referenceTables;

    /**
     * EDCL v1.1.0 accidentally emitted the "ReferenceTableSet" DataMember with the name "_referenceTableSet". This DataMember allows us to easily
     * deserialize this incorrectly named field without needing to write custom deserialization logic.
     */
    @XmlElement(name = "_referenceTableSet")
    private ReferenceTableSet referenceTablesAlternateName;

    /**
     * This is the list of sharded tables in the sharding schema along with their sharding key column names.
     */
    @XmlElement(name = "ShardedTableSet")
    private ShardedTableSet shardedTables;

    /**
     * EDCL v1.1.0 accidentally emitted the "ShardedTableSet" DataMember with the name "_shardedTableSet". This DataMember allows us to easily
     * deserialize this incorrectly named field without needing to write custom deserialization logic.
     */
    @XmlElement(name = "_shardedTableSet")
    private ShardedTableSet shardedTablesAlternateName;

    /**
     * Synchronization object used when adding table entries to the current <see cref="SchemaInfo"/> object.
     */
    private Object syncObject;

    /**
     * Initializes a new instance of the <see cref="SchemaInfo"/> class.
     */
    public SchemaInfo() {
        initialize();
    }

    /**
     * Initializes a new instance of the <see cref="SchemaInfo"/> class.
     */
    public SchemaInfo(ResultSet reader,
            int offset) {
        try (StringReader sr = new StringReader(reader.getSQLXML(offset).getString())) {
            JAXBContext jc = JAXBContext.newInstance(SchemaInfo.class);

            XMLInputFactory xif = XMLInputFactory.newFactory();
            xif.setProperty(XMLInputFactory.IS_SUPPORTING_EXTERNAL_ENTITIES, false);
            xif.setProperty(XMLInputFactory.SUPPORT_DTD, false);
            XMLStreamReader xsr = xif.createXMLStreamReader(sr);

            SchemaInfo schemaInfo = (SchemaInfo) jc.createUnmarshaller().unmarshal(xsr);

            this.referenceTables = new ReferenceTableSet(schemaInfo.getReferenceTables());
            this.shardedTables = new ShardedTableSet(schemaInfo.getShardedTables());
        }
        catch (SQLException | JAXBException | XMLStreamException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    /**
     * Read-only list of information concerning all sharded tables.
     */
    public final HashSet<ShardedTableInfo> getShardedTables() {
        return shardedTables.getShardedSet();
    }

    /**
     * Read-only list of information concerning all reference tables.
     */
    public final HashSet<ReferenceTableInfo> getReferenceTables() {
        return referenceTables.getReferenceSet();
    }

    /**
     * Initialize any non-DataMember objects post deserialization.
     */
    public void afterUnmarshal(Unmarshaller unmarshaller,
            Object parent) {
        initialize();
    }

    /**
     * Initializes this instance after construction or deserialization.
     */
    private void initialize() {
        // If shardedTables is null after deserialization, then set it to shardedTablesAlternateName
        // instead (in case we deserialized the v1.1.0 format). If that is also null, then just set
        // it to an empty HashSet.
        shardedTables = (shardedTables != null && shardedTables.shardedSet.size() > 0) ? shardedTables
                : (shardedTablesAlternateName != null) ? shardedTablesAlternateName : new ShardedTableSet();
        // Null out shardedTablesAlternateName so that we don't serialize it back
        shardedTablesAlternateName = null;

        // Same as above for referenceTables
        referenceTables = (referenceTables != null && referenceTables.referenceSet.size() > 0) ? referenceTables
                : (referenceTablesAlternateName != null) ? referenceTablesAlternateName : new ReferenceTableSet();
        referenceTablesAlternateName = null;

        syncObject = new Object();
    }

    /**
     * Adds information about a sharded table.
     *
     * @param shardedTableInfo
     *            Sharded table info.
     */
    public final void add(ShardedTableInfo shardedTableInfo) {
        ExceptionUtils.disallowNullArgument(shardedTableInfo, "shardedTableInfo");

        String existingTableType = null;

        synchronized (syncObject) {
            ReferenceObjectHelper<String> refExistingTableType = new ReferenceObjectHelper<>(existingTableType);
            if (checkIfTableExists(shardedTableInfo, refExistingTableType)) {
                existingTableType = refExistingTableType.argValue;
                throw new SchemaInfoException(SchemaInfoErrorCode.TableInfoAlreadyPresent, Errors._SchemaInfo_TableInfoAlreadyExists,
                        existingTableType, shardedTableInfo.getSchemaName(), shardedTableInfo.getTableName());
            }

            int initialSize;
            if (shardedTables == null || shardedTables.getShardedSet() == null) {
                initialSize = 0;
                HashSet<ShardedTableInfo> set = new HashSet<>();
                set.add(shardedTableInfo);
                shardedTables = new ShardedTableSet(set);
            }
            else {
                initialSize = shardedTables.getShardedSet().size();
                shardedTables.getShardedSet().add(shardedTableInfo);
            }

            assert shardedTables.getShardedSet().size() - 1 == initialSize;
            // Adding to the sharded table set shouldn't fail since we have done all necessary
            // verification apriori.
            // Debug.Assert(result, "Addition of new sharded table info failed.");
        }
    }

    /**
     * Adds information about a reference table.
     *
     * @param referenceTableInfo
     *            Reference table info.
     */
    public final void add(ReferenceTableInfo referenceTableInfo) {
        ExceptionUtils.disallowNullArgument(referenceTableInfo, "referenceTableInfo");

        String existingTableType = null;

        synchronized (syncObject) {
            ReferenceObjectHelper<String> refExistingTableType = new ReferenceObjectHelper<>(existingTableType);
            if (checkIfTableExists(referenceTableInfo, refExistingTableType)) {
                existingTableType = refExistingTableType.argValue;
                throw new SchemaInfoException(SchemaInfoErrorCode.TableInfoAlreadyPresent, Errors._SchemaInfo_TableInfoAlreadyExists,
                        existingTableType, referenceTableInfo.getSchemaName(), referenceTableInfo.getTableName());
            }

            int initialSize;
            if (referenceTables == null || referenceTables.getReferenceSet() == null) {
                initialSize = 0;
                HashSet<ReferenceTableInfo> set = new HashSet<>();
                set.add(referenceTableInfo);
                referenceTables = new ReferenceTableSet(set);
            }
            else {
                initialSize = referenceTables.getReferenceSet().size();
                referenceTables.getReferenceSet().add(referenceTableInfo);
            }

            assert referenceTables.getReferenceSet().size() - 1 == initialSize;
            // Adding to the reference table set shouldn't fail since we have done all necessary
            // verification apriori.
            // Debug.Assert(result, "Addition of new sharded table info failed.");
        }
    }

    /**
     * Removes information about a sharded table.
     *
     * @param shardedTableInfo
     *            Sharded table info.
     */
    public final boolean remove(ShardedTableInfo shardedTableInfo) {
        return shardedTables.getShardedSet().remove(shardedTableInfo);
    }

    /**
     * Removes information about a reference table.
     *
     * @param referenceTableInfo
     *            Reference table info.
     */
    public final boolean remove(ReferenceTableInfo referenceTableInfo) {
        return referenceTables.getReferenceSet().remove(referenceTableInfo);
    }

    /**
     * Check is either a sharded table or a reference table exists by the given name.
     *
     * @param tableInfo
     *            Sharded or reference table info.
     * @param tableType
     *            sharded, reference or null.
     * @return boolean
     */
    private boolean checkIfTableExists(TableInfo tableInfo,
            ReferenceObjectHelper<String> tableType) {
        tableType.argValue = null;

        if (this.shardedTables.getShardedSet() != null && this.shardedTables.getShardedSet().stream().anyMatch(
                s -> s.getSchemaName().equalsIgnoreCase(tableInfo.getSchemaName()) && s.getTableName().equalsIgnoreCase(tableInfo.getTableName()))) {
            tableType.argValue = "sharded";
            return true;
        }

        if (this.referenceTables.getReferenceSet() != null && this.referenceTables.getReferenceSet().stream().anyMatch(
                r -> r.getSchemaName().equalsIgnoreCase(tableInfo.getSchemaName()) && r.getTableName().equalsIgnoreCase(tableInfo.getTableName()))) {
            tableType.argValue = "reference";
            return true;
        }

        return false;
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof SchemaInfo)) {
            throw new AssertionError("Please make sure that both the operands are of 'SchemaInfo' type.");
        }
        SchemaInfo other = (SchemaInfo) o;
        return this.getReferenceTables().equals(other.getReferenceTables()) && this.getShardedTables().equals(other.getShardedTables());
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

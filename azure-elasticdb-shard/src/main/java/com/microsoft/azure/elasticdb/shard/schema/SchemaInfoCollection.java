package com.microsoft.azure.elasticdb.shard.schema;

// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

import com.microsoft.azure.elasticdb.core.commons.helpers.ReferenceObjectHelper;
import com.microsoft.azure.elasticdb.shard.mapmanager.ShardMapManager;
import com.microsoft.azure.elasticdb.shard.store.IStoreResults;
import com.microsoft.azure.elasticdb.shard.store.StoreResult;
import com.microsoft.azure.elasticdb.shard.storeops.base.IStoreOperationGlobal;
import com.microsoft.azure.elasticdb.shard.utils.Errors;
import com.microsoft.azure.elasticdb.shard.utils.ExceptionUtils;

import java.io.IOException;
import java.util.*;

/**
 * Provides storage services to a client for storing or updating or retrieving schema information associated with a sharding scheme
 * and assigning names to individual buckets of information. The class doesn't store the association between a sharding scheme
 * and the metadata unit. It's the caller's responsibility to maintain the mapping.
 */
//TODO TASK: The interface type was changed to the closest equivalent Java type, but the methods implemented will need adjustment:
public class SchemaInfoCollection implements List<Map.Entry<String, SchemaInfo>> {
    /**
     * Shard map manager object.
     */
    private ShardMapManager Manager;

    /**
     * Constructs an instance of schema info collection.
     *
     * @param manager Shard map manager object.
     */
    public SchemaInfoCollection(ShardMapManager manager) {
        this.setManager(manager);
    }

    private ShardMapManager getManager() {
        return Manager;
    }

    private void setManager(ShardMapManager value) {
        Manager = value;
    }

    /**
     * Adds a <see cref="SchemaInfo"/> that is associated with the given <see cref="ShardMap"/> name.
     * The associated data contains information concerning sharded tables and
     * reference tables. If you try to add a <see cref="SchemaInfo"/> with an existing name,
     * a name-conflict exception will be thrown
     *
     * @param shardMapName The name of the <see cref="ShardMap"/> that
     *                     the <see cref="SchemaInfo"/> will be associated with
     * @param schemaInfo   Sharding schema information.
     */
    public final void Add(String shardMapName, SchemaInfo schemaInfo) {
        ExceptionUtils.DisallowNullOrEmptyStringArgument(shardMapName, "shardMapName");
        ExceptionUtils.<SchemaInfo>DisallowNullArgument(schemaInfo, "schemaInfo");

        //TODO
        /*DefaultStoreSchemaInfo dssi = new DefaultStoreSchemaInfo(shardMapName, SerializationHelper.<SchemaInfo>SerializeXmlData(schemaInfo));

		try (IStoreOperationGlobal op = this.getManager().getStoreOperationFactory().CreateAddShardingSchemaInfoGlobalOperation(this.getManager(), "Add", dssi)) {
			op.Do();
		} catch (IOException e) {
			e.printStackTrace();
		}*/
    }

    /**
     * Replaces the <see cref="SchemaInfo"/> with the given <see cref="ShardMap"/> name.
     *
     * @param shardMapName The name of the <see cref="ShardMap"/> whose <see cref="SchemaInfo"/> will be replaced.
     * @param schemaInfo   Sharding schema information.
     */
    public final void Replace(String shardMapName, SchemaInfo schemaInfo) {
        ExceptionUtils.DisallowNullOrEmptyStringArgument(shardMapName, "shardMapName");
        ExceptionUtils.<SchemaInfo>DisallowNullArgument(schemaInfo, "schemaInfo");

        //TODO
        /*DefaultStoreSchemaInfo dssi = new DefaultStoreSchemaInfo(shardMapName, SerializationHelper.<SchemaInfo>SerializeXmlData(schemaInfo));

		try (IStoreOperationGlobal op = this.getManager().getStoreOperationFactory().CreateUpdateShardingSchemaInfoGlobalOperation(this.getManager(), "Replace", dssi)) {
			op.Do();
		} catch (IOException e) {
			e.printStackTrace();
		}*/
    }

    /**
     * Tries to fetch the <see cref="SchemaInfo"/> with the given <see cref="ShardMap"/> name without
     * raising any exception if data doesn't exist.
     *
     * @param shardMapName The name of the <see cref="ShardMap"/> whose <see cref="SchemaInfo"/>
     *                     will be fetched
     * @param schemaInfo   The <see cref="SchemaInfo"/> that was fetched or null if retrieval failed
     * @return true if schema info exists with given name, false otherwise.
     */
    public final boolean TryGet(String shardMapName, ReferenceObjectHelper<SchemaInfo> schemaInfo) {
        ExceptionUtils.DisallowNullOrEmptyStringArgument(shardMapName, "shardMapName");

        schemaInfo.argValue = null;

        IStoreResults result = null;

        try (IStoreOperationGlobal op = this.getManager().getStoreOperationFactory().CreateFindShardingSchemaInfoGlobalOperation(this.getManager(), "TryGet", shardMapName)) {
            result = op.Do();
        } catch (IOException e) {
            e.printStackTrace();
        }

        if (result.getResult() == StoreResult.SchemaInfoNameDoesNotExist) {
            return false;
        }

        //TODO
        /*schemaInfo.argValue = result.getStoreSchemaInfoCollection().Select(si -> SerializationHelper.<SchemaInfo>DeserializeXmlData(si.getShardingSchemaInfo())).Single();*/

        return true;
    }

    /**
     * Fetches the <see cref="SchemaInfo"/> stored with the supplied <see cref="ShardMap"/> name.
     *
     * @param shardMapName The name of the <see cref="ShardMap"/> to get.
     * @return SchemaInfo object.
     */
    public final SchemaInfo Get(String shardMapName) {
        ExceptionUtils.DisallowNullOrEmptyStringArgument(shardMapName, "shardMapName");

        IStoreResults result = null;

        try (IStoreOperationGlobal op = this.getManager().getStoreOperationFactory().CreateFindShardingSchemaInfoGlobalOperation(this.getManager(), "Get", shardMapName)) {
            result = op.Do();
        } catch (IOException e) {
            e.printStackTrace();
        }

        if (result.getResult() == StoreResult.SchemaInfoNameDoesNotExist) {
            throw new SchemaInfoException(SchemaInfoErrorCode.SchemaInfoNameDoesNotExist, Errors._Store_SchemaInfo_NameDoesNotExist, "Get", shardMapName);
        }

        return null; //TODO: result.getStoreSchemaInfoCollection().Select(si -> SerializationHelper.<SchemaInfo>DeserializeXmlData(si.ShardingSchemaInfo)).Single();
    }

    /**
     * Removes the <see cref="SchemaInfo"/> with the given <see cref="ShardMap"/> name.
     *
     * @param shardMapName The name of the <see cref="ShardMap"/> whose <see cref="SchemaInfo"/>
     *                     will be removed
     */
    public final void Remove(String shardMapName) {
        ExceptionUtils.DisallowNullOrEmptyStringArgument(shardMapName, "shardMapName");

        try (IStoreOperationGlobal op = this.getManager().getStoreOperationFactory().CreateRemoveShardingSchemaInfoGlobalOperation(this.getManager(), "Remove", shardMapName)) {
            op.Do();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public int size() {
        return 0;
    }

    @Override
    public boolean isEmpty() {
        return false;
    }

    @Override
    public boolean contains(Object o) {
        return false;
    }

    /**
     * Returns an enumerator that iterates through the <see cref="SchemaInfoCollection"/>.
     *
     * @return Enumerator of key-value pairs of name and <see cref="SchemaInfo"/> objects.
     */
    public final Iterator<Map.Entry<String, SchemaInfo>> iterator() {
        IStoreResults result;

        try (IStoreOperationGlobal op = this.getManager().getStoreOperationFactory().CreateGetShardingSchemaInfosGlobalOperation(this.getManager(), "GetEnumerator")) {
            result = op.Do();
        } catch (IOException e) {
            e.printStackTrace();
        }

        HashMap<String, SchemaInfo> mdCollection = new HashMap<String, SchemaInfo>();

        /*for (IStoreSchemaInfo ssi : result.StoreSchemaInfoCollection) {
            mdCollection.put(ssi.getName(), SerializationHelper.<SchemaInfo>DeserializeXmlData(ssi.getShardingSchemaInfo()));
        }*/

        return mdCollection.entrySet().iterator();
    }

    @Override
    public Object[] toArray() {
        return new Object[0];
    }

    @Override
    public <T> T[] toArray(T[] a) {
        return null;
    }

    @Override
    public boolean add(Map.Entry<String, SchemaInfo> stringSchemaInfoEntry) {
        return false;
    }

    @Override
    public boolean remove(Object o) {
        return false;
    }

    @Override
    public boolean containsAll(Collection<?> c) {
        return false;
    }

    @Override
    public boolean addAll(Collection<? extends Map.Entry<String, SchemaInfo>> c) {
        return false;
    }

    @Override
    public boolean addAll(int index, Collection<? extends Map.Entry<String, SchemaInfo>> c) {
        return false;
    }

    @Override
    public boolean removeAll(Collection<?> c) {
        return false;
    }

    @Override
    public boolean retainAll(Collection<?> c) {
        return false;
    }

    @Override
    public void clear() {

    }

    @Override
    public Map.Entry<String, SchemaInfo> get(int index) {
        return null;
    }

    @Override
    public Map.Entry<String, SchemaInfo> set(int index, Map.Entry<String, SchemaInfo> element) {
        return null;
    }

    @Override
    public void add(int index, Map.Entry<String, SchemaInfo> element) {

    }

    @Override
    public Map.Entry<String, SchemaInfo> remove(int index) {
        return null;
    }

    @Override
    public int indexOf(Object o) {
        return 0;
    }

    @Override
    public int lastIndexOf(Object o) {
        return 0;
    }

    @Override
    public ListIterator<Map.Entry<String, SchemaInfo>> listIterator() {
        return null;
    }

    @Override
    public ListIterator<Map.Entry<String, SchemaInfo>> listIterator(int index) {
        return null;
    }

    @Override
    public List<Map.Entry<String, SchemaInfo>> subList(int fromIndex, int toIndex) {
        return null;
    }

    ///#endRegion Override Methods

    /**
     * Returns an enumerator that iterates through this <see cref="SchemaInfoCollection"/>.
     *
     * @return Enumerator of key-value pairs of name and <see cref="SchemaInfo"/> objects.
     */
    public final Iterator GetEnumerator() {
        return this.iterator();
    }
}
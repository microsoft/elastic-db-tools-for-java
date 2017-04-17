package com.microsoft.azure.elasticdb.shard.mapper;

// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

import com.microsoft.azure.elasticdb.core.commons.helpers.ReferenceObjectHelper;
import com.microsoft.azure.elasticdb.shard.base.*;
import com.microsoft.azure.elasticdb.shard.map.ShardMap;
import com.microsoft.azure.elasticdb.shard.mapmanager.ShardManagementErrorCategory;
import com.microsoft.azure.elasticdb.shard.mapmanager.ShardManagementErrorCode;
import com.microsoft.azure.elasticdb.shard.mapmanager.ShardManagementException;
import com.microsoft.azure.elasticdb.shard.mapmanager.ShardMapManager;
import com.microsoft.azure.elasticdb.shard.storeops.base.StoreOperationRequestBuilder;
import com.microsoft.azure.elasticdb.shard.utils.Errors;
import com.microsoft.sqlserver.jdbc.SQLServerConnection;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.Callable;

/**
 * Mapper from a range of keys to their corresponding shards.
 * <p>
 * <typeparam name="TKey">Key type.</typeparam>
 */
public class RangeShardMapper extends BaseShardMapper implements IShardMapper<RangeMapping, Object> {
    /**
     * Range shard mapper, which managers range mappings.
     *
     * @param manager Reference to ShardMapManager.
     * @param sm      Containing shard map.
     */
    public RangeShardMapper(ShardMapManager manager, ShardMap sm) {
        super(manager, sm);
    }

    public final SQLServerConnection OpenConnectionForKey(Object key, String connectionString) {
        return OpenConnectionForKey(key, connectionString, ConnectionOptions.Validate);
    }

    /**
     * Given a key value, obtains a SqlConnection to the shard in the mapping
     * that contains the key value.
     *
     * @param key              Input key value.
     * @param connectionString Connection string with credential information, the DataSource and Database are
     *                         obtained from the results of the lookup operation for key.
     * @param options          Options for validation operations to perform on opened connection.
     * @return An opened SqlConnection.
     */
    public final SQLServerConnection OpenConnectionForKey(Object key, String connectionString, ConnectionOptions options) {
        return this.OpenConnectionForKey(key, (smm, sm, ssm) -> new RangeMapping(smm, sm, ssm), ShardManagementErrorCategory.RangeShardMap, connectionString, options);
    }

    public final Callable<SQLServerConnection> OpenConnectionForKeyAsync(Object key, String connectionString) {
        return OpenConnectionForKeyAsync(key, connectionString, ConnectionOptions.Validate);
    }

    /**
     * Given a key value, asynchronously obtains a SqlConnection to the shard in the mapping
     * that contains the key value.
     *
     * @param key              Input key value.
     * @param connectionString Connection string with credential information, the DataSource and Database are
     *                         obtained from the results of the lookup operation for key.
     * @param options          Options for validation operations to perform on opened connection.
     * @return A Task encapsulating an opened SqlConnection.
     */
    public final Callable<SQLServerConnection> OpenConnectionForKeyAsync(Object key, String connectionString, ConnectionOptions options) {
        /*return await
        this.<RangeMapping, TKey>OpenConnectionForKeyAsync(key, (smm, sm, ssm) -> new RangeMapping(smm, sm, ssm), ShardManagementErrorCategory.RangeShardMap, connectionString, options).ConfigureAwait(false);*/
        return null; //TODO
    }

    /**
     * Marks the given mapping offline.
     *
     * @param mapping     Input range mapping.
     * @param lockOwnerId Lock owner id of this mapping
     * @return An offline mapping.
     */
    public final RangeMapping MarkMappingOffline(RangeMapping mapping, UUID lockOwnerId) {
        RangeMappingUpdate tempVar = new RangeMappingUpdate();
        tempVar.setStatus(MappingStatus.Offline);
        //TODO: Not sure if the below line works. Need to test.
        return BaseShardMapper.<RangeMapping, RangeMappingUpdate, MappingStatus>SetStatus(mapping, mapping.getStatus(), s -> MappingStatus.Offline, s -> tempVar, (mp, tv, lo) -> this.Update(mapping, tempVar, lockOwnerId), lockOwnerId);
    }

    /**
     * Marks the given mapping online.
     *
     * @param mapping     Input range mapping.
     * @param lockOwnerId Lock owner id of this mapping
     * @return An online mapping.
     */
    public final RangeMapping MarkMappingOnline(RangeMapping mapping, UUID lockOwnerId) {
        RangeMappingUpdate tempVar = new RangeMappingUpdate();
        tempVar.setStatus(MappingStatus.Online);
        //TODO: Not sure if the below line works. Need to test.
        return BaseShardMapper.<RangeMapping, RangeMappingUpdate, MappingStatus>SetStatus(mapping, mapping.getStatus(), s -> MappingStatus.Online, s -> tempVar, (mp, tv, lo) -> this.Update(mapping, tempVar, lockOwnerId), lockOwnerId);
    }

    /**
     * Adds a range mapping.
     *
     * @param mapping Mapping being added.
     * @return The added mapping object.
     */
    public final RangeMapping Add(RangeMapping mapping) {
        return this.<RangeMapping>Add(mapping, (smm, sm, ssm) -> new RangeMapping(smm, sm, ssm));
    }

    /**
     * Removes a range mapping.
     *
     * @param mapping     Mapping being removed.
     * @param lockOwnerId Lock owner id of this mapping
     */
    public final void Remove(RangeMapping mapping, UUID lockOwnerId) {
        this.<RangeMapping>Remove(mapping, (smm, sm, ssm) -> new RangeMapping(smm, sm, ssm), lockOwnerId);
    }

    /**
     * Looks up the key value and returns the corresponding mapping.
     *
     * @param key      Input key value.
     * @param useCache Whether to use cache for lookups.
     * @return Mapping that contains the key value.
     */
    public final RangeMapping Lookup(Object key, boolean useCache) {
        RangeMapping p = this.Lookup(key, useCache, (smm, sm, ssm) -> new RangeMapping(smm, sm, ssm), ShardManagementErrorCategory.RangeShardMap);

        if (p == null) {
            throw new ShardManagementException(ShardManagementErrorCategory.RangeShardMap, ShardManagementErrorCode.MappingNotFoundForKey, Errors._Store_ShardMapper_MappingNotFoundForKeyGlobal, this.getShardMap().getName(), StoreOperationRequestBuilder.SpFindShardMappingByKeyGlobal, "Lookup");
        }

        return p;
    }

    /**
     * Tries to looks up the key value and returns the corresponding mapping.
     *
     * @param key      Input key value.
     * @param useCache Whether to use cache for lookups.
     * @param mapping  Mapping that contains the key value.
     * @return <c>true</c> if mapping is found, <c>false</c> otherwise.
     */
    public final boolean TryLookup(Object key, boolean useCache, ReferenceObjectHelper<RangeMapping> mapping) {
        RangeMapping p = this.Lookup(key, useCache, (smm, sm, ssm) -> new RangeMapping(smm, sm, ssm), ShardManagementErrorCategory.RangeShardMap);

        mapping.argValue = p;

        return p != null;
    }

    /**
     * Gets all the mappings that exist within given range.
     *
     * @param range Optional range value, if null, we cover everything.
     * @param shard Optional shard parameter, if null, we cover all shards.
     * @return Read-only collection of mappings that overlap with given range.
     */
    public final List<RangeMapping> GetMappingsForRange(Range range, Shard shard) {
        return GetMappingsForRange(range
                , shard
                , (smm, sm, ssm) -> new RangeMapping(smm, sm, ssm)
                , ShardManagementErrorCategory.RangeShardMap
                , "RangeMapping");
    }

    /**
     * Allows for update to a range mapping with the updates provided in
     * the <paramref name="update"/> parameter.
     *
     * @param currentMapping Mapping being updated.
     * @param update         Updated properties of the Shard.
     * @param lockOwnerId    Lock owner id of this mapping
     * @return New instance of mapping with updated information.
     */
    public final RangeMapping Update(RangeMapping currentMapping, RangeMappingUpdate update, UUID lockOwnerId) {
        return this.<RangeMapping, RangeMappingUpdate, MappingStatus>Update(currentMapping, update, (smm, sm, ssm) -> new RangeMapping(smm, sm, ssm), rms -> rms.getValue(), i -> MappingStatus.forValue(i), lockOwnerId);
    }

    /**
     * Splits the given mapping into 2 at the given key. The new mappings point to the same shard
     * as the existing mapping.
     *
     * @param existingMapping Given existing mapping.
     * @param splitAt         Split point.
     * @param lockOwnerId     Lock owner id of this mapping
     * @return Read-only collection of 2 new mappings thus created.
     */
    public final List<RangeMapping> Split(RangeMapping existingMapping, Object splitAt, UUID lockOwnerId) {
        /*this.<RangeMapping>EnsureMappingBelongsToShardMap(existingMapping, "Split", "existingMapping");

        ShardKey shardKey = new ShardKey(ShardKey.ShardKeyTypeFromType(Object.class), splitAt);

        if (!existingMapping.Range.Contains(shardKey) || existingMapping.Range.Low == shardKey || existingMapping.Range.High == shardKey) {
            throw new IllegalArgumentException("splitAt", Errors._ShardMapping_SplitPointOutOfRange);
        }

        StoreShard newShard = new DefaultStoreShard(existingMapping.Shard.getStoreShard().Id, UUID.randomUUID(), existingMapping.ShardMapId, existingMapping.Shard.getStoreShard().getLocation(), existingMapping.Shard.getStoreShard().Status);

        StoreMapping mappingToRemove = new StoreMapping(existingMapping.StoreMapping.Id, existingMapping.StoreMapping.ShardMapId, newShard, existingMapping.StoreMapping.MinValue, existingMapping.StoreMapping.MaxValue, existingMapping.StoreMapping.Status, existingMapping.StoreMapping.LockOwnerId);

        StoreMapping[] mappingsToAdd = new StoreMapping[]{
                new StoreMapping(UUID.randomUUID(), newShard.ShardMapId, newShard, existingMapping.Range.Low.RawValue, shardKey.RawValue, (int) existingMapping.Status, lockOwnerId),
                new StoreMapping(UUID.randomUUID(), newShard.ShardMapId, newShard, shardKey.RawValue, existingMapping.Range.getHigh().getRawValue(), (int) existingMapping.Status, lockOwnerId)
        };

        try (IStoreOperation op = this.shardMapManager.getStoreOperationFactory().CreateReplaceMappingsOperation(this.shardMapManager, StoreOperationCode.SplitMapping, this.ShardMap.StoreShardMap, new Pair<StoreMapping, UUID>[]{new Pair<StoreMapping, UUID>(mappingToRemove, lockOwnerId)}, mappingsToAdd.Select(mappingToAdd -> new Pair<StoreMapping, UUID>(mappingToAdd, lockOwnerId)).ToArray())) {
            op.Do();
        }

        return mappingsToAdd.Select(m -> new RangeMapping(this.shardMapManager, this.ShardMap, m)).ToList().AsReadOnly();*/
        return null; //TODO
    }

    /**
     * Merges 2 contiguous mappings into a single mapping. Both left and right mappings should point
     * to the same location and must be contiguous.
     *
     * @param left             Left mapping.
     * @param right            Right mapping.
     * @param leftLockOwnerId  Lock owner id of the left mapping
     * @param rightLockOwnerId Lock owner id of the right mapping
     * @return Mapping that results from the merge operation.
     */
    public final RangeMapping Merge(RangeMapping left, RangeMapping right, UUID leftLockOwnerId, UUID rightLockOwnerId) {
        /*this.<RangeMapping>EnsureMappingBelongsToShardMap(left, "Merge", "left");
        this.<RangeMapping>EnsureMappingBelongsToShardMap(right, "Merge", "right");

        if (!left.Shard.getLocation().equals(right.Shard.getLocation())) {
            throw new IllegalArgumentException(StringUtilsLocal.FormatInvariant(Errors._ShardMapping_MergeDifferentShards, this.ShardMap.Name, left.Shard.getLocation(), right.Shard.getLocation()), "left");
        }

        if (left.Range.Intersects(right.Range) || left.Range.High != right.Range.Low) {
            throw new IllegalArgumentException("left", Errors._ShardMapping_MergeNotAdjacent);
        }

        if (left.Status != right.Status) {
            throw new IllegalArgumentException(StringUtilsLocal.FormatInvariant(Errors._ShardMapping_DifferentStatus, this.ShardMap.Name), "left");
        }

        StoreShard newShard = new DefaultStoreShard(left.Shard.getStoreShard().Id, UUID.randomUUID(), left.Shard.getStoreShard().ShardMapId, left.Shard.getStoreShard().getLocation(), left.Shard.getStoreShard().Status);

        StoreMapping mappingToRemoveLeft = new StoreMapping(left.StoreMapping.Id, left.StoreMapping.ShardMapId, newShard, left.StoreMapping.MinValue, left.StoreMapping.MaxValue, left.StoreMapping.Status, left.StoreMapping.LockOwnerId);

        StoreMapping mappingToRemoveRight = new StoreMapping(right.StoreMapping.Id, right.StoreMapping.ShardMapId, newShard, right.StoreMapping.MinValue, right.StoreMapping.MaxValue, right.StoreMapping.Status, right.StoreMapping.LockOwnerId);

        StoreMapping mappingToAdd = new StoreMapping(UUID.randomUUID(), newShard.ShardMapId, newShard, left.Range.Low.RawValue, right.Range.getHigh().getRawValue(), (int) left.Status, leftLockOwnerId);

        try (IStoreOperation op = this.shardMapManager.getStoreOperationFactory().CreateReplaceMappingsOperation(this.shardMapManager, StoreOperationCode.MergeMappings, this.ShardMap.StoreShardMap, new Pair<StoreMapping, UUID>[]{
                new Pair<StoreMapping, UUID>(mappingToRemoveLeft, leftLockOwnerId),
                new Pair<StoreMapping, UUID>(mappingToRemoveRight, rightLockOwnerId)
        }, new Pair<StoreMapping, UUID>[]{new Pair<StoreMapping, UUID>(mappingToAdd, leftLockOwnerId)})) {
            op.Do();
        }

        return new RangeMapping(this.shardMapManager, this.ShardMap, mappingToAdd);*/
        return null; //TODO
    }

    /**
     * Gets the lock owner of a mapping.
     *
     * @param mapping The mapping
     * @return Lock owner for the mapping.
     */
    public final UUID GetLockOwnerForMapping(RangeMapping mapping) {
        return this.<RangeMapping>GetLockOwnerForMapping(mapping, ShardManagementErrorCategory.RangeShardMap);
    }

    /**
     * Locks or unlocks a given mapping or all mappings.
     *
     * @param mapping           Optional mapping
     * @param lockOwnerId       The lock onwer id
     * @param lockOwnerIdOpType Operation to perform on this mapping with the given lockOwnerId
     */
    public final void LockOrUnlockMappings(RangeMapping mapping, UUID lockOwnerId, LockOwnerIdOpType lockOwnerIdOpType) {
        this.<RangeMapping>LockOrUnlockMappings(mapping, lockOwnerId, lockOwnerIdOpType, ShardManagementErrorCategory.RangeShardMap);
    }
}

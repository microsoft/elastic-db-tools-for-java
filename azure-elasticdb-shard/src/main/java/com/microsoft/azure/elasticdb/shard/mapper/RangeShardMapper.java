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
import com.microsoft.azure.elasticdb.shard.store.DefaultStoreMapping;
import com.microsoft.azure.elasticdb.shard.store.DefaultStoreShard;
import com.microsoft.azure.elasticdb.shard.store.IStoreMapping;
import com.microsoft.azure.elasticdb.shard.store.IStoreShard;
import com.microsoft.azure.elasticdb.shard.storeops.base.IStoreOperation;
import com.microsoft.azure.elasticdb.shard.storeops.base.StoreOperationCode;
import com.microsoft.azure.elasticdb.shard.storeops.base.StoreOperationRequestBuilder;
import com.microsoft.azure.elasticdb.shard.utils.Errors;
import com.microsoft.azure.elasticdb.shard.utils.StringUtilsLocal;
import com.microsoft.sqlserver.jdbc.SQLServerConnection;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.Callable;

/**
 * Mapper from a range of keys to their corresponding shards.
 * <p>
 * <typeparam name="TKey">Key type.</typeparam>
 */
public class RangeShardMapper<TKey> extends BaseShardMapper implements IShardMapper<RangeMapping<TKey>, Range<TKey>, TKey> {
    /**
     * Range shard mapper, which managers range mappings.
     *
     * @param manager Reference to ShardMapManager.
     * @param sm      Containing shard map.
     */
    public RangeShardMapper(ShardMapManager manager, ShardMap sm) {
        super(manager, sm);
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

    public final SQLServerConnection OpenConnectionForKey(TKey key, String connectionString) {
        return OpenConnectionForKey(key, connectionString, ConnectionOptions.Validate);
    }

    public final SQLServerConnection OpenConnectionForKey(TKey key, String connectionString, ConnectionOptions options) {
        return this.<RangeMapping<TKey>, TKey>OpenConnectionForKey(key, (smm, sm, ssm) -> new RangeMapping<TKey>(smm, sm, ssm), ShardManagementErrorCategory.RangeShardMap, connectionString, options);
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

    public final Callable<SQLServerConnection> OpenConnectionForKeyAsync(TKey key, String connectionString) {
        return OpenConnectionForKeyAsync(key, connectionString, ConnectionOptions.Validate);
    }

    public final Callable<SQLServerConnection> OpenConnectionForKeyAsync(TKey key, String connectionString, ConnectionOptions options) {
        return await
        this.<RangeMapping<TKey>, TKey>OpenConnectionForKeyAsync(key, (smm, sm, ssm) -> new RangeMapping<TKey>(smm, sm, ssm), ShardManagementErrorCategory.RangeShardMap, connectionString, options).ConfigureAwait(false);
    }

    /**
     * Marks the given mapping offline.
     *
     * @param mapping     Input range mapping.
     * @param lockOwnerId Lock owner id of this mapping
     * @return An offline mapping.
     */
    public final RangeMapping<TKey> MarkMappingOffline(RangeMapping<TKey> mapping, UUID lockOwnerId) {
        RangeMappingUpdate tempVar = new RangeMappingUpdate();
        tempVar.Status = s;
        return BaseShardMapper.<RangeMapping<TKey>, RangeMappingUpdate, MappingStatus>SetStatus(mapping, mapping.Status, s -> MappingStatus.Offline, s -> tempVar, this.Update, lockOwnerId);
    }

    /**
     * Marks the given mapping online.
     *
     * @param mapping     Input range mapping.
     * @param lockOwnerId Lock owner id of this mapping
     * @return An online mapping.
     */
    public final RangeMapping<TKey> MarkMappingOnline(RangeMapping<TKey> mapping, UUID lockOwnerId) {
        RangeMappingUpdate tempVar = new RangeMappingUpdate();
        tempVar.Status = s;
        return BaseShardMapper.<RangeMapping<TKey>, RangeMappingUpdate, MappingStatus>SetStatus(mapping, mapping.Status, s -> MappingStatus.Online, s -> tempVar, this.Update, lockOwnerId);
    }

    /**
     * Adds a range mapping.
     *
     * @param mapping Mapping being added.
     * @return The added mapping object.
     */
    public final RangeMapping<TKey> Add(RangeMapping<TKey> mapping) {
        return this.<RangeMapping<TKey>>Add(mapping, (smm, sm, ssm) -> new RangeMapping<TKey>(smm, sm, ssm));
    }

    /**
     * Removes a range mapping.
     *
     * @param mapping     Mapping being removed.
     * @param lockOwnerId Lock owner id of this mapping
     */
    public final void Remove(RangeMapping<TKey> mapping, UUID lockOwnerId) {
        this.<RangeMapping<TKey>>Remove(mapping, (smm, sm, ssm) -> new RangeMapping<TKey>(smm, sm, ssm), lockOwnerId);
    }

    /**
     * Looks up the key value and returns the corresponding mapping.
     *
     * @param key      Input key value.
     * @param useCache Whether to use cache for lookups.
     * @return Mapping that contains the key value.
     */
    public final RangeMapping<TKey> Lookup(TKey key, boolean useCache) {
        RangeMapping<TKey> p = this.<RangeMapping<TKey>, TKey>Lookup(key, useCache, (smm, sm, ssm) -> new RangeMapping<TKey>(smm, sm, ssm), ShardManagementErrorCategory.RangeShardMap);

        if (p == null) {
            throw new ShardManagementException(ShardManagementErrorCategory.RangeShardMap, ShardManagementErrorCode.MappingNotFoundForKey, Errors._Store_ShardMapper_MappingNotFoundForKeyGlobal, this.ShardMap.Name, StoreOperationRequestBuilder.SpFindShardMappingByKeyGlobal, "Lookup");
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
    public final boolean TryLookup(TKey key, boolean useCache, ReferenceObjectHelper<RangeMapping<TKey>> mapping) {
        RangeMapping<TKey> p = this.<RangeMapping<TKey>, TKey>Lookup(key, useCache, (smm, sm, ssm) -> new RangeMapping<TKey>(smm, sm, ssm), ShardManagementErrorCategory.RangeShardMap);

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
    public final List<RangeMapping<TKey>> GetMappingsForRange(Range<TKey> range, Shard shard) {
        return this.<RangeMapping<TKey>, TKey>GetMappingsForRange(range, shard, (smm, sm, ssm) -> new RangeMapping<TKey>(smm, sm, ssm), ShardManagementErrorCategory.RangeShardMap, "RangeMapping");
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
    public final RangeMapping<TKey> Update(RangeMapping<TKey> currentMapping, RangeMappingUpdate update, UUID lockOwnerId) {
        return this.<RangeMapping<TKey>, RangeMappingUpdate, MappingStatus>Update(currentMapping, update, (smm, sm, ssm) -> new RangeMapping<TKey>(smm, sm, ssm), rms -> (int) rms, i -> (MappingStatus) i, lockOwnerId);
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
    public final IReadOnlyList<RangeMapping<TKey>> Split(RangeMapping<TKey> existingMapping, TKey splitAt, UUID lockOwnerId) {
        this.<RangeMapping<TKey>>EnsureMappingBelongsToShardMap(existingMapping, "Split", "existingMapping");

        ShardKey shardKey = new ShardKey(ShardKey.ShardKeyTypeFromType(TKey.class), splitAt);

        if (!existingMapping.Range.Contains(shardKey) || existingMapping.Range.Low == shardKey || existingMapping.Range.High == shardKey) {
            throw new IllegalArgumentException("splitAt", Errors._ShardMapping_SplitPointOutOfRange);
        }

        IStoreShard newShard = new DefaultStoreShard(existingMapping.Shard.StoreShard.Id, UUID.randomUUID(), existingMapping.ShardMapId, existingMapping.Shard.StoreShard.Location, existingMapping.Shard.StoreShard.Status);

        IStoreMapping mappingToRemove = new DefaultStoreMapping(existingMapping.StoreMapping.Id, existingMapping.StoreMapping.ShardMapId, newShard, existingMapping.StoreMapping.MinValue, existingMapping.StoreMapping.MaxValue, existingMapping.StoreMapping.Status, existingMapping.StoreMapping.LockOwnerId);

        IStoreMapping[] mappingsToAdd = new IStoreMapping[]{
                new DefaultStoreMapping(UUID.randomUUID(), newShard.ShardMapId, newShard, existingMapping.Range.Low.RawValue, shardKey.RawValue, (int) existingMapping.Status, lockOwnerId),
                new DefaultStoreMapping(UUID.randomUUID(), newShard.ShardMapId, newShard, shardKey.RawValue, existingMapping.Range.High.RawValue, (int) existingMapping.Status, lockOwnerId)
        };

        try (IStoreOperation op = this.shardMapManager.StoreOperationFactory.CreateReplaceMappingsOperation(this.shardMapManager, StoreOperationCode.SplitMapping, this.ShardMap.StoreShardMap, new Tuple<IStoreMapping, UUID>[]{new Tuple<IStoreMapping, UUID>(mappingToRemove, lockOwnerId)}, mappingsToAdd.Select(mappingToAdd -> new Tuple<IStoreMapping, UUID>(mappingToAdd, lockOwnerId)).ToArray())) {
            op.Do();
        }

        return mappingsToAdd.Select(m -> new RangeMapping<TKey>(this.shardMapManager, this.ShardMap, m)).ToList().AsReadOnly();
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
    public final RangeMapping<TKey> Merge(RangeMapping<TKey> left, RangeMapping<TKey> right, UUID leftLockOwnerId, UUID rightLockOwnerId) {
        this.<RangeMapping<TKey>>EnsureMappingBelongsToShardMap(left, "Merge", "left");
        this.<RangeMapping<TKey>>EnsureMappingBelongsToShardMap(right, "Merge", "right");

        if (!left.Shard.Location.equals(right.Shard.Location)) {
            throw new IllegalArgumentException(StringUtilsLocal.FormatInvariant(Errors._ShardMapping_MergeDifferentShards, this.ShardMap.Name, left.Shard.Location, right.Shard.Location), "left");
        }

        if (left.Range.Intersects(right.Range) || left.Range.High != right.Range.Low) {
            throw new IllegalArgumentException("left", Errors._ShardMapping_MergeNotAdjacent);
        }

        if (left.Status != right.Status) {
            throw new IllegalArgumentException(StringUtilsLocal.FormatInvariant(Errors._ShardMapping_DifferentStatus, this.ShardMap.Name), "left");
        }

        IStoreShard newShard = new DefaultStoreShard(left.Shard.StoreShard.Id, UUID.randomUUID(), left.Shard.StoreShard.ShardMapId, left.Shard.StoreShard.Location, left.Shard.StoreShard.Status);

        IStoreMapping mappingToRemoveLeft = new DefaultStoreMapping(left.StoreMapping.Id, left.StoreMapping.ShardMapId, newShard, left.StoreMapping.MinValue, left.StoreMapping.MaxValue, left.StoreMapping.Status, left.StoreMapping.LockOwnerId);

        IStoreMapping mappingToRemoveRight = new DefaultStoreMapping(right.StoreMapping.Id, right.StoreMapping.ShardMapId, newShard, right.StoreMapping.MinValue, right.StoreMapping.MaxValue, right.StoreMapping.Status, right.StoreMapping.LockOwnerId);

        IStoreMapping mappingToAdd = new DefaultStoreMapping(UUID.randomUUID(), newShard.ShardMapId, newShard, left.Range.Low.RawValue, right.Range.High.RawValue, (int) left.Status, leftLockOwnerId);

        try (IStoreOperation op = this.shardMapManager.StoreOperationFactory.CreateReplaceMappingsOperation(this.shardMapManager, StoreOperationCode.MergeMappings, this.ShardMap.StoreShardMap, new Tuple<IStoreMapping, UUID>[]{
                new Tuple<IStoreMapping, UUID>(mappingToRemoveLeft, leftLockOwnerId),
                new Tuple<IStoreMapping, UUID>(mappingToRemoveRight, rightLockOwnerId)
        }, new Tuple<IStoreMapping, UUID>[]{new Tuple<IStoreMapping, UUID>(mappingToAdd, leftLockOwnerId)})) {
            op.Do();
        }

        return new RangeMapping<TKey>(this.shardMapManager, this.ShardMap, mappingToAdd);
    }

    /**
     * Gets the lock owner of a mapping.
     *
     * @param mapping The mapping
     * @return Lock owner for the mapping.
     */
    public final UUID GetLockOwnerForMapping(RangeMapping<TKey> mapping) {
        return this.<RangeMapping<TKey>>GetLockOwnerForMapping(mapping, ShardManagementErrorCategory.RangeShardMap);
    }

    /**
     * Locks or unlocks a given mapping or all mappings.
     *
     * @param mapping           Optional mapping
     * @param lockOwnerId       The lock onwer id
     * @param lockOwnerIdOpType Operation to perform on this mapping with the given lockOwnerId
     */
    public final void LockOrUnlockMappings(RangeMapping<TKey> mapping, UUID lockOwnerId, LockOwnerIdOpType lockOwnerIdOpType) {
        this.<RangeMapping<TKey>>LockOrUnlockMappings(mapping, lockOwnerId, lockOwnerIdOpType, ShardManagementErrorCategory.RangeShardMap);
    }
}

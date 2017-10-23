package com.microsoft.azure.elasticdb.shard.map;

import java.lang.invoke.MethodHandles;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/*
 * Elastic database tools for Azure SQL Database.
 * 
 * Copyright(c) Microsoft Corporation All rights reserved.
 * 
 * This program is made available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

import com.google.common.base.Stopwatch;
import com.microsoft.azure.elasticdb.core.commons.helpers.ReferenceObjectHelper;
import com.microsoft.azure.elasticdb.core.commons.logging.ActivityIdScope;
import com.microsoft.azure.elasticdb.shard.base.LockOwnerIdOpType;
import com.microsoft.azure.elasticdb.shard.base.LookupOptions;
import com.microsoft.azure.elasticdb.shard.base.MappingLockToken;
import com.microsoft.azure.elasticdb.shard.base.MappingStatus;
import com.microsoft.azure.elasticdb.shard.base.Range;
import com.microsoft.azure.elasticdb.shard.base.RangeMapping;
import com.microsoft.azure.elasticdb.shard.base.RangeMappingCreationInfo;
import com.microsoft.azure.elasticdb.shard.base.RangeMappingUpdate;
import com.microsoft.azure.elasticdb.shard.base.Shard;
import com.microsoft.azure.elasticdb.shard.mapmanager.ShardMapManager;
import com.microsoft.azure.elasticdb.shard.mapper.IShardMapper;
import com.microsoft.azure.elasticdb.shard.mapper.RangeShardMapper;
import com.microsoft.azure.elasticdb.shard.store.StoreShardMap;
import com.microsoft.azure.elasticdb.shard.utils.ExceptionUtils;

/**
 * Represents a shard map of ranges. <typeparam name="KeyT">Key type.</typeparam>.
 */
public final class RangeShardMap<KeyT> extends ShardMap implements Cloneable {

    private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    /**
     * Mapping b/w key ranges and shards.
     */
    private RangeShardMapper rsm;

    /**
     * Constructs a new instance.
     *
     * @param shardMapManager
     *            Reference to ShardMapManager.
     * @param ssm
     *            Storage representation.
     */
    public RangeShardMap(ShardMapManager shardMapManager,
            StoreShardMap ssm) {
        super(shardMapManager, ssm);
        this.rsm = new RangeShardMapper(this.getShardMapManager(), this);
    }

    /**
     * Creates and adds a range mapping to ShardMap.
     *
     * @param creationInfo
     *            Information about mapping to be added.
     * @return Newly created mapping.
     */
    public RangeMapping createRangeMapping(RangeMappingCreationInfo creationInfo) {
        ExceptionUtils.disallowNullArgument(creationInfo, "args");

        try (ActivityIdScope activityIdScope = new ActivityIdScope(UUID.randomUUID())) {
            log.info("CreateRangeMapping Start; Shard: {}", creationInfo.getShard().getLocation());

            Stopwatch stopwatch = Stopwatch.createStarted();

            RangeMapping map = this.rsm.add(new RangeMapping(this.getShardMapManager(), creationInfo));

            stopwatch.stop();

            log.info("CreateRangeMapping Complete; Shard: {}; Duration: {}", creationInfo.getShard().getLocation(),
                    stopwatch.elapsed(TimeUnit.MILLISECONDS));

            return map;
        }
    }

    /**
     * Creates and adds a range mapping to ShardMap.
     *
     * @param range
     *            Range for which to create the mapping.
     * @param shard
     *            Shard associated with the range mapping.
     * @return Newly created mapping.
     */
    public RangeMapping createRangeMapping(Range range,
            Shard shard) {
        ExceptionUtils.disallowNullArgument(range, "range");
        ExceptionUtils.disallowNullArgument(shard, "shard");

        try (ActivityIdScope activityIdScope = new ActivityIdScope(UUID.randomUUID())) {
            RangeMappingCreationInfo args = new RangeMappingCreationInfo(range, shard, MappingStatus.Online);

            log.info("CreateRangeMapping Start; Shard: {}", shard.getLocation());

            Stopwatch stopwatch = Stopwatch.createStarted();

            RangeMapping rangeMapping = this.rsm.add(new RangeMapping(this.getShardMapManager(), args));

            stopwatch.stop();

            log.info("CreateRangeMapping Complete; Shard: {}; Duration: {}", shard.getLocation(), stopwatch.elapsed(TimeUnit.MILLISECONDS));

            return rangeMapping;
        }
    }

    /**
     * Removes a range mapping.
     *
     * @param mapping
     *            Mapping being removed.
     */
    public void deleteMapping(RangeMapping mapping) {
        this.deleteMapping(mapping, MappingLockToken.NoLock);
    }

    /**
     * Removes a range mapping.
     *
     * @param mapping
     *            Mapping being removed.
     * @param mappingLockToken
     *            An instance of <see cref="MappingLockToken"/>
     */
    public void deleteMapping(RangeMapping mapping,
            MappingLockToken mappingLockToken) {
        ExceptionUtils.disallowNullArgument(mapping, "mapping");
        ExceptionUtils.disallowNullArgument(mappingLockToken, "mappingLockToken");

        try (ActivityIdScope activityIdScope = new ActivityIdScope(UUID.randomUUID())) {
            log.info("DeleteMapping Start; Shard: {}", mapping.getShard().getLocation());

            Stopwatch stopwatch = Stopwatch.createStarted();

            this.rsm.remove(mapping, mappingLockToken.getLockOwnerId());

            stopwatch.stop();

            log.info("DeleteMapping Complete; Shard: {}; Duration: {}", mapping.getShard().getLocation(), stopwatch.elapsed(TimeUnit.MILLISECONDS));
        }
    }

    /**
     * Looks up the key value and returns the corresponding mapping.
     *
     * @param key
     *            Input key value.
     * @return Mapping that contains the key value.
     */
    public RangeMapping getMappingForKey(KeyT key) {
        return getMappingForKey(key, LookupOptions.LOOKUP_IN_STORE);
    }

    /**
     * Looks up the key value and returns the corresponding mapping.
     *
     * @param key
     *            Input key value.
     * @param lookupOptions
     *            Whether to search in the cache and/or store.
     * @return Mapping that contains the key value.
     */
    public RangeMapping getMappingForKey(KeyT key,
            LookupOptions lookupOptions) {
        try (ActivityIdScope activityIdScope = new ActivityIdScope(UUID.randomUUID())) {
            log.info("GetMapping Start; Range Mapping Key Type: {}; Lookup Options: {}", key.getClass(), lookupOptions);

            Stopwatch stopwatch = Stopwatch.createStarted();

            RangeMapping rangeMapping = this.rsm.lookup(key, lookupOptions);

            stopwatch.stop();

            log.info("GetMapping Complete; Range Mapping Key Type: {}; Lookup Options: {} Duration: {}", key.getClass(), lookupOptions,
                    stopwatch.elapsed(TimeUnit.MILLISECONDS));

            return rangeMapping;
        }
    }

    /**
     * Tries to looks up the key value and place the corresponding mapping in <paramref name="rangeMapping"/>.
     *
     * @param key
     *            Input key value.
     * @param rangeMapping
     *            Mapping that contains the key value.
     * @return <c>true</c> if mapping is found, <c>false</c> otherwise.
     */
    public boolean tryGetMappingForKey(KeyT key,
            ReferenceObjectHelper<RangeMapping> rangeMapping) {
        return tryGetMappingForKey(key, LookupOptions.LOOKUP_IN_STORE, rangeMapping);
    }

    /**
     * Tries to looks up the key value and place the corresponding mapping in <paramref name="rangeMapping"/>.
     *
     * @param key
     *            Input key value.
     * @param lookupOptions
     *            Whether to search in the cache and/or store.
     * @param rangeMapping
     *            Mapping that contains the key value.
     * @return <c>true</c> if mapping is found, <c>false</c> otherwise.
     */
    public boolean tryGetMappingForKey(KeyT key,
            LookupOptions lookupOptions,
            ReferenceObjectHelper<RangeMapping> rangeMapping) {
        try (ActivityIdScope activityIdScope = new ActivityIdScope(UUID.randomUUID())) {
            log.info("TryLookupRangeMapping Start; ShardMap name: {}; Range Mapping Key Type: {}; Lookup " + "Options: {}", this.getName(),
                    key.getClass(), lookupOptions);

            Stopwatch stopwatch = Stopwatch.createStarted();

            boolean result = this.rsm.tryLookup(key, lookupOptions, rangeMapping);

            stopwatch.stop();

            log.info("TryLookupRangeMapping Complete; ShardMap name: {}; Range Mapping Key Type: {}; " + "Lookup Options: {}; Duration: {}",
                    this.getName(), key.getClass(), lookupOptions, stopwatch.elapsed(TimeUnit.MILLISECONDS));

            return result;
        }
    }

    /**
     * Gets all the range mappings for the shard map.
     *
     * @return Read-only collection of all range mappings on the shard map.
     */
    public List<RangeMapping> getMappings() {
        try (ActivityIdScope activityIdScope = new ActivityIdScope(UUID.randomUUID())) {
            log.info("GetMappings Start;");

            Stopwatch stopwatch = Stopwatch.createStarted();

            List<RangeMapping> rangeMappings = this.rsm.getMappingsForRange(null, null, LookupOptions.LOOKUP_IN_STORE);

            stopwatch.stop();

            log.info("GetMappings Complete; Duration: {}", stopwatch.elapsed(TimeUnit.MILLISECONDS));

            return rangeMappings;
        }
    }

    /**
     * Gets all the range mappings that exist within given range.
     *
     * @param range
     *            Range value, any mapping overlapping with the range will be returned.
     * @return Read-only collection of mappings that satisfy the given range constraint.
     */
    public List<RangeMapping> getMappings(Range range) {
        ExceptionUtils.disallowNullArgument(range, "range");

        try (ActivityIdScope activityIdScope = new ActivityIdScope(UUID.randomUUID())) {
            log.info("GetMappings Start; Range: {}", range);

            Stopwatch stopwatch = Stopwatch.createStarted();

            List<RangeMapping> rangeMappings = this.rsm.getMappingsForRange(range, null, LookupOptions.LOOKUP_IN_STORE);

            stopwatch.stop();

            log.info("GetMappings Complete; Range: {}; Duration: {}", range, stopwatch.elapsed(TimeUnit.MILLISECONDS));

            return rangeMappings;
        }
    }

    /**
     * Gets all the range mappings that exist for the given shard.
     *
     * @param shard
     *            Shard for which the mappings will be returned.
     * @return Read-only collection of mappings that satisfy the given shard constraint.
     */
    public List<RangeMapping> getMappings(Shard shard) {
        ExceptionUtils.disallowNullArgument(shard, "shard");

        try (ActivityIdScope activityIdScope = new ActivityIdScope(UUID.randomUUID())) {
            log.info("GetMappings Start; Shard: {}", shard.getLocation());

            Stopwatch stopwatch = Stopwatch.createStarted();

            List<RangeMapping> rangeMappings = this.rsm.getMappingsForRange(null, shard, LookupOptions.LOOKUP_IN_STORE);

            stopwatch.stop();

            log.info("GetMappings Complete; Shard: {}; Duration: {}", shard.getLocation(), stopwatch.elapsed(TimeUnit.MILLISECONDS));

            return rangeMappings;
        }
    }

    /**
     * Gets all the range mappings that exist within given range and given shard.
     *
     * @param range
     *            Range value, any mapping overlapping with the range will be returned.
     * @param shard
     *            Shard for which the mappings will be returned.
     * @return Read-only collection of mappings that satisfy the given range and shard constraints.
     */
    public List<RangeMapping> getMappings(Range range,
            Shard shard) {
        ExceptionUtils.disallowNullArgument(range, "range");
        ExceptionUtils.disallowNullArgument(shard, "shard");

        try (ActivityIdScope activityIdScope = new ActivityIdScope(UUID.randomUUID())) {
            log.info("GetMappings Start; Shard: {}; Range: {}", shard.getLocation(), range);

            Stopwatch stopwatch = Stopwatch.createStarted();

            List<RangeMapping> rangeMappings = this.rsm.getMappingsForRange(range, shard, LookupOptions.LOOKUP_IN_STORE);

            stopwatch.stop();

            log.info("GetMappings Complete; Shard: {}; Duration: {}", shard.getLocation(), stopwatch.elapsed(TimeUnit.MILLISECONDS));

            return rangeMappings;
        }
    }

    /**
     * Gets all the range mappings for the shard map.
     *
     * @param lookupOptions
     *            Whether to search in the cache and/or store.
     * @return Read-only collection of all range mappings on the shard map.
     */
    public List<RangeMapping> getMappings(LookupOptions lookupOptions) {
        try (ActivityIdScope activityIdScope = new ActivityIdScope(UUID.randomUUID())) {
            log.info("GetMappings Start; Lookup Options: {};", lookupOptions);

            Stopwatch stopwatch = Stopwatch.createStarted();

            List<RangeMapping> rangeMappings = this.rsm.getMappingsForRange(null, null, lookupOptions);

            stopwatch.stop();

            log.info("GetMappings Complete; Lookup Options: {}; Duration: {}", lookupOptions, stopwatch.elapsed(TimeUnit.MILLISECONDS));

            return rangeMappings;
        }
    }

    /**
     * Gets all the range mappings that exist within given range.
     *
     * @param range
     *            Range value, any mapping overlapping with the range will be returned.
     * @param lookupOptions
     *            Whether to search in the cache and/or store.
     * @return Read-only collection of mappings that satisfy the given range constraint.
     */
    public List<RangeMapping> getMappings(Range range,
            LookupOptions lookupOptions) {
        ExceptionUtils.disallowNullArgument(range, "range");

        try (ActivityIdScope activityIdScope = new ActivityIdScope(UUID.randomUUID())) {
            log.info("GetMappings Start; Range: {}; Lookup Options: {}", range, lookupOptions);

            Stopwatch stopwatch = Stopwatch.createStarted();

            List<RangeMapping> rangeMappings = this.rsm.getMappingsForRange(range, null, lookupOptions);

            stopwatch.stop();

            log.info("GetMappings Complete; Range: {}; Lookup Options: {}; Duration: {}", range, lookupOptions,
                    stopwatch.elapsed(TimeUnit.MILLISECONDS));

            return rangeMappings;
        }
    }

    /**
     * Gets all the range mappings that exist for the given shard.
     *
     * @param shard
     *            Shard for which the mappings will be returned.
     * @param lookupOptions
     *            Whether to search in the cache and/or store.
     * @return Read-only collection of mappings that satisfy the given shard constraint.
     */
    public List<RangeMapping> getMappings(Shard shard,
            LookupOptions lookupOptions) {
        ExceptionUtils.disallowNullArgument(shard, "shard");

        try (ActivityIdScope activityIdScope = new ActivityIdScope(UUID.randomUUID())) {
            log.info("GetMappings Start; Shard: {}; Lookup Options: {}", shard.getLocation(), lookupOptions);

            Stopwatch stopwatch = Stopwatch.createStarted();

            List<RangeMapping> rangeMappings = this.rsm.getMappingsForRange(null, shard, lookupOptions);

            stopwatch.stop();

            log.info("GetMappings Complete; Shard: {}; Lookup Options: {}; Duration: {}", shard.getLocation(), lookupOptions,
                    stopwatch.elapsed(TimeUnit.MILLISECONDS));

            return rangeMappings;
        }
    }

    /**
     * Gets all the range mappings that exist within given range and given shard.
     *
     * @param range
     *            Range value, any mapping overlapping with the range will be returned.
     * @param shard
     *            Shard for which the mappings will be returned.
     * @param lookupOptions
     *            Whether to search in the cache and/or store.
     * @return Read-only collection of mappings that satisfy the given range and shard constraints.
     */
    public List<RangeMapping> getMappings(Range range,
            Shard shard,
            LookupOptions lookupOptions) {
        ExceptionUtils.disallowNullArgument(range, "range");
        ExceptionUtils.disallowNullArgument(shard, "shard");

        try (ActivityIdScope activityIdScope = new ActivityIdScope(UUID.randomUUID())) {
            log.info("GetMappings Start; Shard: {}; Range: {}; Lookup Options: {}", shard.getLocation(), lookupOptions, range);

            Stopwatch stopwatch = Stopwatch.createStarted();

            List<RangeMapping> rangeMappings = this.rsm.getMappingsForRange(range, shard, lookupOptions);

            stopwatch.stop();

            log.info("GetMappings Complete; Shard: {}; Lookup Options: {}; Duration: {}", shard.getLocation(), lookupOptions,
                    stopwatch.elapsed(TimeUnit.MILLISECONDS));

            return rangeMappings;
        }
    }

    /**
     * Marks the specified mapping offline.
     *
     * @param mapping
     *            Input range mapping.
     * @return An offline mapping.
     */
    public RangeMapping markMappingOffline(RangeMapping mapping) {
        return this.markMappingOffline(mapping, MappingLockToken.NoLock);
    }

    /**
     * Marks the specified mapping offline.
     *
     * @param mapping
     *            Input range mapping.
     * @param mappingLockToken
     *            An instance of <see cref="MappingLockToken"/>
     * @return An offline mapping.
     */
    public RangeMapping markMappingOffline(RangeMapping mapping,
            MappingLockToken mappingLockToken) {
        ExceptionUtils.disallowNullArgument(mapping, "mapping");
        ExceptionUtils.disallowNullArgument(mappingLockToken, "mappingLockToken");

        try (ActivityIdScope activityIdScope = new ActivityIdScope(UUID.randomUUID())) {
            log.info("MarkMappingOffline Start; ");

            Stopwatch stopwatch = Stopwatch.createStarted();

            RangeMapping result = this.rsm.markMappingOffline(mapping, mappingLockToken.getLockOwnerId());

            stopwatch.stop();

            log.info("MarkMappingOffline Complete; Duration: {}", stopwatch.elapsed(TimeUnit.MILLISECONDS));

            return result;
        }
    }

    /**
     * Marks the specified mapping online.
     *
     * @param mapping
     *            Input range mapping.
     * @return An online mapping.
     */
    public RangeMapping markMappingOnline(RangeMapping mapping) {
        return this.markMappingOnline(mapping, MappingLockToken.NoLock);
    }

    /**
     * Marks the specified mapping online.
     *
     * @param mapping
     *            Input range mapping.
     * @param mappingLockToken
     *            An instance of <see cref="MappingLockToken"/>
     * @return An online mapping.
     */
    public RangeMapping markMappingOnline(RangeMapping mapping,
            MappingLockToken mappingLockToken) {
        ExceptionUtils.disallowNullArgument(mapping, "mapping");
        ExceptionUtils.disallowNullArgument(mappingLockToken, "mappingLockToken");

        try (ActivityIdScope activityIdScope = new ActivityIdScope(UUID.randomUUID())) {
            log.info("MarkMappingOnline Start; ");

            Stopwatch stopwatch = Stopwatch.createStarted();

            RangeMapping result = this.rsm.markMappingOnline(mapping, mappingLockToken.getLockOwnerId());

            stopwatch.stop();

            log.info("MarkMappingOnline Complete; Duration: {}", stopwatch.elapsed(TimeUnit.MILLISECONDS));

            return result;
        }
    }

    /**
     * Gets the lock owner id of the specified mapping.
     *
     * @param mapping
     *            Input range mapping.
     * @return An instance of <see cref="MappingLockToken"/>
     */
    public MappingLockToken getMappingLockOwner(RangeMapping mapping) {
        ExceptionUtils.disallowNullArgument(mapping, "mapping");

        try (ActivityIdScope activityIdScope = new ActivityIdScope(UUID.randomUUID())) {
            log.info("LookupLockOwner Start");

            Stopwatch stopwatch = Stopwatch.createStarted();

            UUID storeLockOwnerId = this.rsm.getLockOwnerForMapping(mapping);

            stopwatch.stop();

            log.info("LookupLockOwner Complete; Duration: {}; StoreLockOwnerId: {}", stopwatch.elapsed(TimeUnit.MILLISECONDS), storeLockOwnerId);

            return new MappingLockToken(storeLockOwnerId);
        }
    }

    /**
     * Locks the mapping for the specified owner The state of a locked mapping can only be modified by the lock owner.
     *
     * @param mapping
     *            Input range mapping.
     * @param mappingLockToken
     *            An instance of <see cref="MappingLockToken"/>
     */
    public void lockMapping(RangeMapping mapping,
            MappingLockToken mappingLockToken) {
        ExceptionUtils.disallowNullArgument(mapping, "mapping");
        ExceptionUtils.disallowNullArgument(mappingLockToken, "mappingLockToken");

        try (ActivityIdScope activityIdScope = new ActivityIdScope(UUID.randomUUID())) {
            // Generate a lock owner id
            UUID lockOwnerId = mappingLockToken.getLockOwnerId();

            log.info("Lock Start; LockOwnerId: {}", lockOwnerId);

            Stopwatch stopwatch = Stopwatch.createStarted();

            this.rsm.lockOrUnlockMappings(mapping, lockOwnerId, LockOwnerIdOpType.Lock);

            stopwatch.stop();

            log.info("Lock Complete; Duration: {}; StoreLockOwnerId: {}", stopwatch.elapsed(TimeUnit.MILLISECONDS), lockOwnerId);
        }
    }

    /**
     * Unlocks the specified mapping
     *
     * @param mapping
     *            Input range mapping.
     * @param mappingLockToken
     *            An instance of <see cref="MappingLockToken"/>
     */
    public void unlockMapping(RangeMapping mapping,
            MappingLockToken mappingLockToken) {
        ExceptionUtils.disallowNullArgument(mapping, "mapping");
        ExceptionUtils.disallowNullArgument(mappingLockToken, "mappingLockToken");

        try (ActivityIdScope activityIdScope = new ActivityIdScope(UUID.randomUUID())) {
            UUID lockOwnerId = mappingLockToken.getLockOwnerId();
            log.info("Unlock Start; LockOwnerId: {}", lockOwnerId);

            Stopwatch stopwatch = Stopwatch.createStarted();

            this.rsm.lockOrUnlockMappings(mapping, lockOwnerId, LockOwnerIdOpType.UnlockMappingForId);

            stopwatch.stop();

            log.info("UnLock Complete; Duration: {}; StoreLockOwnerId: {}", stopwatch.elapsed(TimeUnit.MILLISECONDS), lockOwnerId);
        }
    }

    /**
     * Unlocks all mappings in this map that belong to the given <see cref="MappingLockToken"/>.
     *
     * @param mappingLockToken
     *            An instance of <see cref="MappingLockToken"/>
     */
    public void unlockMapping(MappingLockToken mappingLockToken) {
        ExceptionUtils.disallowNullArgument(mappingLockToken, "mappingLockToken");

        try (ActivityIdScope activityIdScope = new ActivityIdScope(UUID.randomUUID())) {
            UUID lockOwnerId = mappingLockToken.getLockOwnerId();
            log.info("UnlockAllMappingsWithLockOwnerId Start; LockOwnerId: {}", lockOwnerId);

            Stopwatch stopwatch = Stopwatch.createStarted();

            this.rsm.lockOrUnlockMappings(null, lockOwnerId, LockOwnerIdOpType.UnlockAllMappingsForId);

            stopwatch.stop();

            log.info("UnlockAllMappingsWithLockOwnerId Complete; Duration: {}", stopwatch.elapsed(TimeUnit.MILLISECONDS));
        }
    }

    /**
     * Updates a <see cref="RangeMapping{KeyT}"/> with the updates provided in the <paramref name="update"/> parameter.
     *
     * @param currentMapping
     *            Mapping being updated.
     * @param update
     *            Updated properties of the mapping.
     * @return New instance of mapping with updated information.
     */
    public RangeMapping updateMapping(RangeMapping currentMapping,
            RangeMappingUpdate update) {
        return this.updateMapping(currentMapping, update, MappingLockToken.NoLock);
    }

    /**
     * Updates a <see cref="RangeMapping{KeyT}"/> with the updates provided in the <paramref name="update"/> parameter.
     *
     * @param currentMapping
     *            Mapping being updated.
     * @param update
     *            Updated properties of the mapping.
     * @param mappingLockToken
     *            An instance of <see cref="MappingLockToken"/>
     * @return New instance of mapping with updated information.
     */
    public RangeMapping updateMapping(RangeMapping currentMapping,
            RangeMappingUpdate update,
            MappingLockToken mappingLockToken) {
        ExceptionUtils.disallowNullArgument(currentMapping, "currentMapping");
        ExceptionUtils.disallowNullArgument(update, "update");
        ExceptionUtils.disallowNullArgument(mappingLockToken, "mappingLockToken");

        try (ActivityIdScope activityIdScope = new ActivityIdScope(UUID.randomUUID())) {
            log.info("UpdateMapping Start; Current mapping shard: {}", currentMapping.getShard().getLocation());

            Stopwatch stopwatch = Stopwatch.createStarted();

            RangeMapping map = this.rsm.update(currentMapping, update, mappingLockToken.getLockOwnerId());

            stopwatch.stop();

            log.info("UpdateMapping Complete; Current mapping shard: {}; Duration: {}", currentMapping.getShard().getLocation(),
                    stopwatch.elapsed(TimeUnit.MILLISECONDS));

            return map;
        }
    }

    /**
     * Splits the specified mapping into two new mappings using the specified key as boundary. The new mappings point to the same shard as the
     * existing mapping.
     *
     * @param existingMapping
     *            Existing mapping.
     * @param splitAt
     *            Split point.
     * @return Read-only collection of two new mappings that were created.
     */
    public List<RangeMapping> splitMapping(RangeMapping existingMapping,
            KeyT splitAt) {
        return this.splitMapping(existingMapping, splitAt, MappingLockToken.NoLock);
    }

    /**
     * Splits the specified mapping into two new mappings using the specified key as boundary. The new mappings point to the same shard as the
     * existing mapping.
     *
     * @param existingMapping
     *            Existing mapping.
     * @param splitAt
     *            Split point.
     * @param mappingLockToken
     *            An instance of <see cref="MappingLockToken"/>
     * @return Read-only collection of two new mappings that were created.
     */
    public List<RangeMapping> splitMapping(RangeMapping existingMapping,
            KeyT splitAt,
            MappingLockToken mappingLockToken) {
        ExceptionUtils.disallowNullArgument(existingMapping, "existingMapping");
        ExceptionUtils.disallowNullArgument(mappingLockToken, "mappingLockToken");

        try (ActivityIdScope activityIdScope = new ActivityIdScope(UUID.randomUUID())) {
            log.info("SplitMapping Start; Shard: {}", existingMapping.getShard().getLocation());

            Stopwatch stopwatch = Stopwatch.createStarted();

            List<RangeMapping> rangeMapping = this.rsm.split(existingMapping, splitAt, mappingLockToken.getLockOwnerId());

            stopwatch.stop();

            log.info("SplitMapping Complete; Shard: {}; Duration: {}", existingMapping.getShard().getLocation(),
                    stopwatch.elapsed(TimeUnit.MILLISECONDS));

            return rangeMapping;
        }
    }

    /**
     * Merges 2 contiguous mappings into a single mapping. Both left and right mappings should point to the same location and must be contiguous.
     *
     * @param left
     *            Left mapping.
     * @param right
     *            Right mapping.
     * @return Mapping that results from the merge operation.
     */
    public RangeMapping mergeMappings(RangeMapping left,
            RangeMapping right) {
        return this.mergeMappings(left, right, MappingLockToken.NoLock, MappingLockToken.NoLock);
    }

    /**
     * Merges 2 contiguous mappings into a single mapping. Both left and right mappings should point to the same location and must be contiguous.
     *
     * @param left
     *            Left mapping.
     * @param right
     *            Right mapping.
     * @param leftMappingLockToken
     *            An instance of <see cref="MappingLockToken"/> for the left mapping
     * @param rightMappingLockToken
     *            An instance of <see cref="MappingLockToken"/> for the right mapping
     * @return Mapping that results from the merge operation.
     */
    public RangeMapping mergeMappings(RangeMapping left,
            RangeMapping right,
            MappingLockToken leftMappingLockToken,
            MappingLockToken rightMappingLockToken) {
        ExceptionUtils.disallowNullArgument(left, "left");
        ExceptionUtils.disallowNullArgument(right, "right");
        ExceptionUtils.disallowNullArgument(leftMappingLockToken, "leftMappingLockToken");
        ExceptionUtils.disallowNullArgument(rightMappingLockToken, "rightMappingLockToken");

        try (ActivityIdScope activityIdScope = new ActivityIdScope(UUID.randomUUID())) {
            log.info("SplitMapping Start; Left Shard: {}; Right Shard: {}", left.getShard().getLocation(), right.getShard().getLocation());

            Stopwatch stopwatch = Stopwatch.createStarted();

            RangeMapping rangeMapping = this.rsm.merge(left, right, leftMappingLockToken.getLockOwnerId(), rightMappingLockToken.getLockOwnerId());

            stopwatch.stop();

            log.info("SplitMapping Complete; Duration: {}", stopwatch.elapsed(TimeUnit.MILLISECONDS));

            return rangeMapping;
        }
    }

    /**
     * Gets the mapper. This method is used by OpenConnection/Lookup of V. <typeparam name="V">Shard provider type.</typeparam>
     *
     * @return RangeShardMapper for given key type.
     */
    @Override
    public <V> IShardMapper getMapper() {
        return rsm;
    }

    /**
     * Clones the given range shard map.
     *
     * @return A cloned instance of the range shard map.
     */
    public RangeShardMap clone() {
        ShardMap tempVar = this.cloneCore();
        return (RangeShardMap) ((tempVar instanceof RangeShardMap<?>) ? tempVar : null);
    }

    /**
     * Clones the current shard map instance.
     *
     * @return Cloned shard map instance.
     */
    @Override
    protected ShardMap cloneCore() {
        return new RangeShardMap(this.getShardMapManager(), this.getStoreShardMap());
    }
}
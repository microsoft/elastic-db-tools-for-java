package com.microsoft.azure.elasticdb.shard.recovery;

// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

import com.microsoft.azure.elasticdb.core.commons.helpers.ReferenceObjectHelper;
import com.microsoft.azure.elasticdb.shard.base.ShardLocation;
import com.microsoft.azure.elasticdb.shard.base.ShardRange;
import com.microsoft.azure.elasticdb.shard.map.ShardMapType;
import com.microsoft.azure.elasticdb.shard.mapmanager.ShardManagementErrorCategory;
import com.microsoft.azure.elasticdb.shard.mapmanager.ShardMapManager;
import com.microsoft.azure.elasticdb.shard.store.*;
import com.microsoft.azure.elasticdb.shard.storeops.base.IStoreOperationGlobal;
import com.microsoft.azure.elasticdb.shard.storeops.base.IStoreOperationLocal;
import com.microsoft.azure.elasticdb.shard.utils.Errors;
import com.microsoft.azure.elasticdb.shard.utils.ExceptionUtils;
import com.microsoft.azure.elasticdb.shard.utils.StringUtilsLocal;
import org.apache.commons.lang3.tuple.Pair;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Manages various recovery related tasks for a shard map manager. It helps
 * resolving data corruption issues between shard map information stored
 * locally on the shards and in the global shard map manager database.
 * It also helps with certain 'oops' recovery scenarios where reconstruction
 * of shard maps from database backups or database copies is necessary.
 * <p>
 * <p>
 * Note that some of the recovery methods can cause unrecoverable data loss when not used
 * properly. It is recommend to take backups or copies of all databases that participate
 * in recovery operations.
 */
public final class RecoveryManager {
    ///#region Constructors

    /**
     * Cached list of inconsistencies so user can resolve without knowing the data format.
     */
    private Map<RecoveryToken, Map<ShardRange, MappingDifference>> Inconsistencies;

    ///#endregion Constructors

    ///#region Private Properties
    /**
     * Cached list of IStoreShardMaps so user can reconstruct shards based on a token.
     */
    private Map<RecoveryToken, Pair<IStoreShardMap, StoreShard>> StoreShardMaps;
    /**
     * Cached list of ShardLocations so user can determine Shardlocation based on a token.
     */
    private Map<RecoveryToken, ShardLocation> Locations;
    /**
     * Reference to the associated shard map manager.
     */
    private ShardMapManager Manager;

    /**
     * Constructs an instance of the recovery manager for given shard map manager.
     *
     * @param shardMapManager Shard map manager being recovered.
     */
    public RecoveryManager(ShardMapManager shardMapManager) {
        assert shardMapManager != null;
        this.setManager(shardMapManager);
        this.setInconsistencies(new HashMap<RecoveryToken, Map<ShardRange, MappingDifference>>());
        this.setStoreShardMaps(new HashMap<RecoveryToken, Pair<IStoreShardMap, StoreShard>>());
        this.setLocations(new HashMap<RecoveryToken, ShardLocation>());
    }

    private Map<RecoveryToken, Map<ShardRange, MappingDifference>> getInconsistencies() {
        return Inconsistencies;
    }

    private void setInconsistencies(Map<RecoveryToken, Map<ShardRange, MappingDifference>> value) {
        Inconsistencies = value;
    }

    private Map<RecoveryToken, Pair<IStoreShardMap, StoreShard>> getStoreShardMaps() {
        return StoreShardMaps;
    }

    private void setStoreShardMaps(Map<RecoveryToken, Pair<IStoreShardMap, StoreShard>> value) {
        StoreShardMaps = value;
    }

    private Map<RecoveryToken, ShardLocation> getLocations() {
        return Locations;
    }

    private void setLocations(Map<RecoveryToken, ShardLocation> value) {
        Locations = value;
    }

    private ShardMapManager getManager() {
        return Manager;
    }

    private void setManager(ShardMapManager value) {
        Manager = value;
    }

    ///#endregion

    ///#region Public API

    /**
     * Attaches a shard to the shard map manager. Earlier versions
     * of mappings for the same shard map will automatically be updated
     * if more recent versions are found on the shard to be attached.
     * After attaching a shard, <see cref="DetectMappingDifferences(ShardLocation, string)"/>
     * should be called to check for any inconsistencies that warrant
     * manual conflict resolution.
     *
     * @param location Location of the shard being attached.
     *                 <p>
     *                 Note that this method can cause unrecoverable data loss. Make sure you have taken backups or copies
     *                 of your databases and only then proceed with great care.
     */
    public void AttachShard(ShardLocation location) {
        this.AttachShard(location, null);
    }

    /**
     * Attaches a shard to the shard map manager. Earlier versions
     * of mappings for the same shard map will automatically be updated
     * if more recent versions are found on the shard to be attached.
     * Shard location will be upgraded to latest version of local store as part of this operation.
     * After attaching a shard, <see cref="DetectMappingDifferences(ShardLocation, string)"/>
     * should be called to check for any inconsistencies that warrant
     * manual conflict resolution.
     *
     * @param location     Location of the shard being attached.
     * @param shardMapName Optional string to filter on the shard map name.
     *                     <p>
     *                     Note that this method can cause unrecoverable data loss. Make sure you have taken backups or copies
     *                     of your databases and only then proceed with great care.
     */
    public void AttachShard(ShardLocation location, String shardMapName) {
        ExceptionUtils.DisallowNullArgument(location, "location");

        IStoreResults result;

        try (IStoreOperationLocal op = this.getManager().getStoreOperationFactory().CreateGetShardsLocalOperation(this.getManager(), location, "AttachShard")) {
            result = op.Do();
        } catch (IOException e) {
            e.printStackTrace();
        }

        /*assert result.getResult() == StoreResult.Success;

//TODO TASK: There is no Java equivalent to LINQ queries:
        List<IStoreShardMap> shardMaps = shardMapName == null ? result.getStoreShardMaps() : result.getStoreShardMaps().Where(s = shardMapName.equals( > s.Name))
        ;

        shardMaps.<IStoreShardMap>ToList().ForEach((sm) -> {
            StoreShard shard = result.getStoreShards().SingleOrDefault(s -> s.getShardMapId() == sm.getId());

            // construct a new store shard with correct location
            DefaultStoreShard sNew = new DefaultStoreShard(shard.getId(), shard.getVersion(), shard.getShardMapId(), location, shard.getStatus());

            try (IStoreOperation op = this.getManager().getStoreOperationFactory().CreateAttachShardOperation(this.getManager(), sm, sNew)) {
                op.Do();
            }
        });*/
    }

    /**
     * Detaches the given shard from the shard map manager. Mappings pointing to the
     * shard to be deleted will automatically be removed by this method.
     *
     * @param location Location of the shard being detached.
     *                 <p>
     *                 Note that this method can cause unrecoverable data loss. Make sure you have taken backups or copies
     *                 of your databases and only then proceed with great care.
     */
    public void DetachShard(ShardLocation location) {
        this.DetachShard(location, null);
    }

    /**
     * Detaches the given shard from the shard map manager. Mappings pointing to the
     * shard to be deleted will automatically be removed by this method.
     *
     * @param location     Location of the shard being detached.
     * @param shardMapName Optional string to filter on shard map name.
     *                     <p>
     *                     Note that this method can cause unrecoverable data loss. Make sure you have taken backups or copies
     *                     of your databases and only then proceed with great care.
     */
    public void DetachShard(ShardLocation location, String shardMapName) {
        ExceptionUtils.DisallowNullArgument(location, "location");

        try (IStoreOperationGlobal op = this.getManager().getStoreOperationFactory().CreateDetachShardGlobalOperation(this.getManager(), "DetachShard", location, shardMapName)) {
            op.Do();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    ///#region Token information Getters

    /**
     * Returns a dictionary of range-to-location key-value pairs. The location returned is an enumerator stating
     * whether a given range (or point) is present only in the local shard map, only in the global shard map, or both.
     * Ranges not contained in either shard map cannot contain differences so those ranges are not shown.
     *
     * @param token Recovery token returned from <see cref="DetectMappingDifferences(ShardLocation, string)"/>.
     * @return The set of ranges and their corresponding <see cref="MappingLocation"/>.
     * <p>
     * This method assumes a previous call to <see cref="DetectMappingDifferences(ShardLocation, string)"/> that provides the recovery token parameter.
     * The result of this method is typically used in subsequent calls to resolve inconsistencies such as
     * <see cref="ResolveMappingDifferences"/> or <see cref="RebuildMappingsOnShard"/>.
     */
    public Map<ShardRange, MappingLocation> GetMappingDifferences(RecoveryToken token) {
        ExceptionUtils.DisallowNullArgument(token, "token");

        if (!this.getInconsistencies().containsKey(token)) {
            throw new IllegalArgumentException(StringUtilsLocal.FormatInvariant(Errors._Recovery_InvalidRecoveryToken, token), new Throwable("token"));
        }

        return null; //TODO: this.getInconsistencies().get(token).ToDictionary(i -> i.Key, i -> i.Value.getLocation());
    }

    /**
     * Retrieves shard map type, name and shard location based on the token returned from <see cref="DetectMappingDifferences(ShardLocation, string)"/>.
     *
     * @param token         Recovery token returned from <see cref="DetectMappingDifferences(ShardLocation, string)"/>.
     * @param mapType       Outputs shard map type (Range or List).
     * @param shardMapName  Outputs shard map name.
     * @param shardLocation Outputs shard location
     */
    public void GetShardInfo(RecoveryToken token, ReferenceObjectHelper<ShardMapType> mapType, ReferenceObjectHelper<String> shardMapName, ReferenceObjectHelper<ShardLocation> shardLocation) {
        ExceptionUtils.DisallowNullArgument(token, "token");

        Pair<IStoreShardMap, StoreShard> shardInfoLocal = null;

        if (!(this.getStoreShardMaps().containsKey(token) ? (shardInfoLocal = this.getStoreShardMaps().get(token)) == shardInfoLocal : false)) {
            throw new IllegalArgumentException(StringUtilsLocal.FormatInvariant(Errors._Recovery_InvalidRecoveryToken, token), new Throwable("token"));
        }

        //TODO
        /*mapType.argValue = shardInfoLocal.Item1.MapType;
        shardMapName.argValue = shardInfoLocal.Item1.Name;*/

        if (!(this.getLocations().containsKey(token) ? (shardLocation.argValue = this.getLocations().get(token)) == shardLocation.argValue : false)) {
            throw new IllegalArgumentException(StringUtilsLocal.FormatInvariant(Errors._Recovery_InvalidRecoveryToken, token), new Throwable("token"));
        }
    }

    /**
     * Retrieves shard map type and name based on the token returned from <see cref="DetectMappingDifferences(ShardLocation, string)"/>.
     *
     * @param token        Recovery token returned from <see cref="DetectMappingDifferences(ShardLocation, string)"/>.
     * @param mapType      Output Shardmap type (Range or List).
     * @param shardMapName Output name of shard map.
     */
    public void GetShardInfo(RecoveryToken token, ReferenceObjectHelper<ShardMapType> mapType, ReferenceObjectHelper<String> shardMapName) {
        ExceptionUtils.DisallowNullArgument(token, "token");

        Pair<IStoreShardMap, StoreShard> shardInfoLocal = null;

        if (!(this.getStoreShardMaps().containsKey(token) ? (shardInfoLocal = this.getStoreShardMaps().get(token)) == shardInfoLocal : false)) {
            throw new IllegalArgumentException(StringUtilsLocal.FormatInvariant(Errors._Recovery_InvalidRecoveryToken, token), new Throwable("token"));
        }

        //TODO
        /*mapType.argValue = shardInfoLocal.Item1.MapType;
        shardMapName.argValue = shardInfoLocal.Item1.Name;*/
    }

    /**
     * Returns the shard map type of the shard map processed by <see cref="DetectMappingDifferences(ShardLocation, string)"/>.
     *
     * @param token Recovery token returned from <see cref="DetectMappingDifferences(ShardLocation, string)"/>.
     * @return The type of shard map (list, range, etc...) corresponding to the recovery token.
     */
    public ShardMapType GetShardMapType(RecoveryToken token) {
        ExceptionUtils.DisallowNullArgument(token, "token");

        Pair<IStoreShardMap, StoreShard> shardInfoLocal = null;

        if (!(this.getStoreShardMaps().containsKey(token) ? (shardInfoLocal = this.getStoreShardMaps().get(token)) == shardInfoLocal : false)) {
            throw new IllegalArgumentException(StringUtilsLocal.FormatInvariant(Errors._Recovery_InvalidRecoveryToken, token), new Throwable("token"));
        }

        return null; //TODO: shardInfoLocal.Item1.MapType;
    }

    /**
     * Returns the shard map name of the shard map processed by <see cref="DetectMappingDifferences(ShardLocation, string)"/>.
     *
     * @param token Recovery token returned from <see cref="DetectMappingDifferences(ShardLocation, string)"/>.
     * @return The name of the shard map for the given recovery token.
     */
    public String GetShardMapName(RecoveryToken token) {
        ExceptionUtils.DisallowNullArgument(token, "token");

        Pair<IStoreShardMap, StoreShard> shardInfoLocal = null;

        if (!(this.getStoreShardMaps().containsKey(token) ? (shardInfoLocal = this.getStoreShardMaps().get(token)) == shardInfoLocal : false)) {
            throw new IllegalArgumentException(StringUtilsLocal.FormatInvariant(Errors._Recovery_InvalidRecoveryToken, token), new Throwable("token"));
        }

        return null; //TODO: shardInfoLocal.Item1.Name;
    }

    /**
     * Returns the shard location of the local shard map processed by <see cref="DetectMappingDifferences(ShardLocation, string)"/>.
     *
     * @param token Recovery token returned from <see cref="DetectMappingDifferences(ShardLocation, string)"/>
     * @return Location of the shard corresponding to the set of mapping differences detected in <see cref="DetectMappingDifferences(ShardLocation, string)"/>.
     */
    public ShardLocation GetShardLocation(RecoveryToken token) {
        ExceptionUtils.DisallowNullArgument(token, "token");

        ShardLocation shardLocation = null;

        if (!(this.getLocations().containsKey(token) ? (shardLocation = this.getLocations().get(token)) == shardLocation : false)) {
            throw new IllegalArgumentException(StringUtilsLocal.FormatInvariant(Errors._Recovery_InvalidRecoveryToken, token), new Throwable("token"));
        }

        return shardLocation;
    }

    ///#endregion Token Information Getters

    /**
     * Given a collection of shard locations, reconstructs local shard maps based
     * on the mapping information stored in the global shard map. The specified
     * shards need to be registered already in the global shard map. This method only
     * rebuilds mappings. It does not rebuild shard membership within the global shard map.
     *
     * @param shardLocations Collection of shard locations.
     *                       <p>
     *                       Note that this method can cause unrecoverable data loss. Make sure you have taken backups or copies
     *                       of your databases and only then proceed with great care.
     */
    public void RebuildMappingsOnShardsFromShardMapManager(List<ShardLocation> shardLocations) {
        this.RebuildMappingsOnShardsFromShardMapManager(shardLocations, null);
    }

    /**
     * Given a collection of shard locations, reconstructs local shard maps based
     * on the mapping information stored in the global shard map. The specified
     * shards need to be registered already in the global shard map. This method only
     * rebuilds mappings. It does not rebuild shard membership within the global shard map.
     *
     * @param shardLocations Collection of shard locations.
     * @param shardMapName   Optional parameter to filter by shard map name. If omitted, all shard maps will be rebuilt.
     *                       <p>
     *                       Note that this method can cause unrecoverable data loss. Make sure you have taken backups or copies
     *                       of your databases and only then proceed with great care.
     */
    public void RebuildMappingsOnShardsFromShardMapManager(List<ShardLocation> shardLocations, String shardMapName) {
        ExceptionUtils.DisallowNullArgument(shardLocations, "shardLocations");

        this.RebuildMappingsHelper("RebuildMappingsOnShardsFromShardMapManager", shardLocations, MappingDifferenceResolution.KeepShardMapMapping, shardMapName);
    }

    /**
     * Given a collection of shard locations, reconstructs the shard map manager based on mapping information
     * stored in the individual shards. The specified
     * shards need to be registered already in the global shard map. This method only
     * rebuilds mappings. It does not rebuild shard membership within the global shard map.
     * If the information in the individual shard maps is or becomes inconsistent, the behavior is undefined.
     * No cross shard locks are taken, so if any shards become inconsistent during the execution of this
     * method, the final state of the global shard map may be corrupt.
     *
     * @param shardLocations Collection of shard locations.
     *                       <p>
     *                       Note that this method can cause unrecoverable data loss. Make sure you have taken backups or copies
     *                       of your databases and only then proceed with great care.
     */
    public void RebuildMappingsOnShardMapManagerFromShards(List<ShardLocation> shardLocations) {
        RebuildMappingsOnShardMapManagerFromShards(shardLocations, null);
    }

    /**
     * Given a collection of shard locations, reconstructs the shard map manager based on mapping information
     * stored in the individual shards. The specified
     * shards need to be registered already in the global shard map. This method only
     * rebuilds mappings. It does not rebuild shard membership within the global shard map.
     * If the information in the individual shard maps is or becomes inconsistent, the behavior is undefined.
     * No cross shard locks are taken, so if any shards become inconsistent during the execution of this
     * method, the final state of the global shard map may be corrupt.
     *
     * @param shardLocations Collection of shard locations.
     * @param shardMapName   Optional name of shard map. If omitted, will attempt to recover from all shard maps present on each shard.
     *                       <p>
     *                       Note that this method can cause unrecoverable data loss. Make sure you have taken backups or copies
     *                       of your databases and only then proceed with great care.
     */
    public void RebuildMappingsOnShardMapManagerFromShards(List<ShardLocation> shardLocations, String shardMapName) {
        ExceptionUtils.DisallowNullArgument(shardLocations, "shardLocations");

        this.RebuildMappingsHelper("RebuildMappingsOnShardMapManagerFromShards", shardLocations, MappingDifferenceResolution.KeepShardMapping, shardMapName);
    }

    /**
     * Rebuilds a local range shard map from a list of inconsistent shard ranges
     * detected by <see cref="DetectMappingDifferences(ShardLocation, string)"/> and then accessed by <see cref="GetMappingDifferences"/>.
     * The resulting local range shard map will always still be inconsistent with
     * the global shard map in the shard map manager database. A subsequent call to <see cref="ResolveMappingDifferences"/>
     * is necessary to bring the system back to a healthy state.
     *
     * @param token  The recovery token from a previous call to <see cref="DetectMappingDifferences(ShardLocation, string)"/>.
     * @param ranges The set of ranges to keep on the local shard when rebuilding the local shard map.
     *               <p>
     *               Note that this method can cause unrecoverable data loss. Make sure you have taken backups or copies
     *               of your databases and only then proceed with great care.
     *               <p>
     *               Only shard ranges with inconsistencies can be rebuilt using this method. All ranges with no inconsistencies between
     *               the local shard and the global shard map will be kept intact on the local shard and are not affected by this call.
     *               Subsequent changes to the non-conflicting mappings can be made later using the regular interfaces in the shard map manager.
     *               It is not necessary to use the recovery manager to change non-conflicting mappings.
     */
    public void RebuildMappingsOnShard(RecoveryToken token, List<ShardRange> ranges) {
        ExceptionUtils.DisallowNullArgument(token, "token");
        ExceptionUtils.DisallowNullArgument(ranges, "ranges");

        ShardLocation location = this.GetShardLocation(token);

        if (!this.getInconsistencies().containsKey(token)) {
            throw new IllegalArgumentException(StringUtilsLocal.FormatInvariant(Errors._Recovery_InvalidRecoveryToken, token), new Throwable("token"));
        }

        IStoreShardMap ssmLocal = null;

        ReferenceObjectHelper<IStoreShardMap> tempRef_ssmLocal = new ReferenceObjectHelper<IStoreShardMap>(ssmLocal);
        StoreShard dss = this.GetStoreShardFromToken("RebuildMappingsOnShard", token, tempRef_ssmLocal);
        ssmLocal = tempRef_ssmLocal.argValue;

        List<IStoreMapping> mappingsToAdd = new ArrayList<IStoreMapping>();

        // Determine the ranges we want to keep based on input keeps list.
        /*for (ShardRange range : ranges) {
            MappingDifference difference = null;

            ReferenceObjectHelper<MappingDifference> tempRef_difference = new ReferenceObjectHelper<MappingDifference>(difference);
            if (!this.getInconsistencies().get(token).TryGetValue(range, tempRef_difference)) {
                difference = tempRef_difference.argValue;
                throw new IllegalArgumentException(StringUtilsLocal.FormatInvariant(Errors._Recovery_InvalidRebuildShardSpecification, range, location), new Throwable("ranges"));
            } else {
                difference = tempRef_difference.argValue;
            }

            // The storeMapping we will use as a template.
            IStoreMapping storeMappingTemplate = difference.getLocation().getValue() == MappingLocation.MappingInShardMapOnly.getValue() ? difference.getMappingForShardMap() : difference.getMappingForShard();

            IStoreMapping storeMappingToAdd = new DefaultStoreMapping(UUID.randomUUID(), storeMappingTemplate.getShardMapId(), dss, range.getLow().getRawValue(), range.getHigh().getRawValue(), storeMappingTemplate.getStatus(), null);

            mappingsToAdd.add(storeMappingToAdd);
        }

        try (IStoreOperationLocal op = this.getManager().getStoreOperationFactory().CreateReplaceMappingsLocalOperation(this.getManager(), location, "RebuildMappingsOnShard", ssmLocal, dss, this.getInconsistencies().get(token).keySet(), mappingsToAdd)) {
            op.Do();
        }*/
        //TODO

        this.getStoreShardMaps().remove(token);
        this.getLocations().remove(token);
        this.getInconsistencies().remove(token);
    }

    /**
     * Enumerates differences in the mappings between the global shard map manager database and the local shard
     * database in the specified shard location.
     *
     * @param location Location of shard for which to detect inconsistencies.
     * @return Collection of tokens to be used for further resolution tasks (see <see cref="ResolveMappingDifferences"/>).
     */
    public List<RecoveryToken> DetectMappingDifferences(ShardLocation location) {
        return this.DetectMappingDifferences(location, null);
    }

    /**
     * Enumerates differences in the mappings between the global shard map manager database and the local shard
     * database in the specified shard location.
     *
     * @param location     Location of shard for which to detect inconsistencies.
     * @param shardMapName Optional parameter to specify a particular shard map.
     * @return Collection of tokens to be used for further resolution tasks (see <see cref="ResolveMappingDifferences"/>).
     */
    public List<RecoveryToken> DetectMappingDifferences(ShardLocation location, String shardMapName) {
        ExceptionUtils.DisallowNullArgument(location, "location");

        List<RecoveryToken> listOfTokens = new ArrayList<RecoveryToken>();

        IStoreResults getShardsLocalResult;

        try (IStoreOperationLocal op = this.getManager().getStoreOperationFactory().CreateGetShardsLocalOperation(this.getManager(), location, "DetectMappingDifferences")) {
            getShardsLocalResult = op.Do();
        } catch (IOException e) {
            e.printStackTrace();
        }

        /*assert getShardsLocalResult.getResult() == StoreResult.Success;

//TODO TASK: There is no Java equivalent to LINQ queries:
        List<IStoreShardMap> shardMaps = shardMapName == null ? getShardsLocalResult.getStoreShardMaps() : getShardsLocalResult.getStoreShardMaps().Where(s = shardMapName.equals( > s.Name))
        ;

        List<Pair<IStoreShardMap, StoreShard>> shardInfos = shardMaps.Select(sm -> new Pair<IStoreShardMap, StoreShard>(sm, getShardsLocalResult.getStoreShards().SingleOrDefault(s -> s.getShardMapId() == sm.getId())));

        for (Pair<IStoreShardMap, StoreShard> shardInfo : shardInfos) {
            IStoreShardMap ssmLocal = shardInfo.Item1;
            StoreShard ssLocal = shardInfo.Item2;

            RecoveryToken token = new RecoveryToken();

            listOfTokens.add(token);
            this.getStoreShardMaps().put(token, shardInfo);
            this.getLocations().put(token, location);

            this.getInconsistencies().put(token, new HashMap<ShardRange, MappingDifference>());

            DefaultStoreShard dss = new DefaultStoreShard(ssLocal.getId(), ssLocal.getVersion(), ssLocal.getShardMapId(), ssLocal.getLocation(), ssLocal.getStatus());

            // First get all local mappings.
            IStoreResults lsmMappings;

            try (IStoreOperationLocal op = this.getManager().getStoreOperationFactory().CreateGetMappingsByRangeLocalOperation(this.getManager(), location, "DetectMappingDifferences", ssmLocal, dss, null, true)) {
                lsmMappings = op.Do();

                if (lsmMappings.getResult() == StoreResult.ShardMapDoesNotExist) {
                    // The shard needs to be re-attached. We are ignoring these errors in 
                    // DetectMappingDifferences, since corruption is more profound than 
                    // just inconsistent mappings.
                    // Alternatively, this shard belongs to a different shard map manager.
                    // Either way, we can't do anything about it here.
                    continue;
                }
            } catch (IOException e) {
                e.printStackTrace();
            }

            // Next build up a set of relevant global mappings.
            // This is the union of those mappings that are associated with this local shard
            // and those mappings which intersect with mappings found in the local shard.
            // We will partition these mappings based on ranges.
            Map<ShardRange, IStoreMapping> relevantGsmMappings = new HashMap<ShardRange, IStoreMapping>();

            IStoreResults gsmMappingsByMap;

            try (IStoreOperationGlobal op = this.getManager().getStoreOperationFactory().CreateGetMappingsByRangeGlobalOperation(this.getManager(), "DetectMappingDifferences", ssmLocal, dss, null, ShardManagementErrorCategory.Recovery, false, true)) {
                gsmMappingsByMap = op.Do();
            } catch (IOException e) {
                e.printStackTrace();
            }

            if (gsmMappingsByMap.getResult() == StoreResult.ShardMapDoesNotExist) {
                // The shard map is not properly attached to this GSM.
                // This is beyond what we can handle resolving mappings.
                continue;
            }

            for (IStoreMapping gsmMapping : gsmMappingsByMap.getStoreMappings()) {
                ShardKey min = ShardKey.FromRawValue(ssmLocal.getKeyType(), gsmMapping.getMinValue());

                ShardKey max = null;

                switch (ssmLocal.getMapType()) {
                    case Range:
                        max = ShardKey.FromRawValue(ssmLocal.getKeyType(), gsmMapping.getMaxValue());
                        break;

                    default:
                        assert ssmLocal.getMapType() == ShardMapType.List;
                        max = ShardKey.FromRawValue(ssmLocal.getKeyType(), gsmMapping.getMinValue()).GetNextKey();
                        break;
                }

                ShardRange range = new ShardRange(min, max);

                relevantGsmMappings.put(range, gsmMapping);
            }

            // Next, for each of the mappings in lsmMappings, we need to augment 
            // the gsmMappingsByMap by intersecting ranges.
            for (IStoreMapping lsmMapping : lsmMappings.getStoreMappings()) {
                ShardKey min = ShardKey.FromRawValue(ssmLocal.getKeyType(), lsmMapping.getMinValue());

                IStoreResults gsmMappingsByRange = null;

                if (ssmLocal.getMapType() == ShardMapType.Range) {
                    ShardKey max = ShardKey.FromRawValue(ssmLocal.getKeyType(), lsmMapping.getMaxValue());

                    ShardRange range = new ShardRange(min, max);

                    try (IStoreOperationGlobal op = this.getManager().getStoreOperationFactory().CreateGetMappingsByRangeGlobalOperation(this.getManager(), "DetectMappingDifferences", ssmLocal, null, range, ShardManagementErrorCategory.Recovery, false, true)) {
                        gsmMappingsByRange = op.Do();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }

                    if (gsmMappingsByRange.getResult() == StoreResult.ShardMapDoesNotExist) {
                        // The shard was not properly attached. 
                        // This is more than we can deal with in mapping resolution.
                        continue;
                    }
                } else {
                    assert ssmLocal.getMapType() == ShardMapType.List;
                    try (IStoreOperationGlobal op = this.getManager().getStoreOperationFactory().CreateFindMappingByKeyGlobalOperation(this.getManager(), "DetectMappingDifferences", ssmLocal, min, CacheStoreMappingUpdatePolicy.OverwriteExisting, ShardManagementErrorCategory.Recovery, false, true)) {
                        gsmMappingsByRange = op.Do();

                        if (gsmMappingsByRange.getResult() == StoreResult.MappingNotFoundForKey || gsmMappingsByRange.getResult() == StoreResult.ShardMapDoesNotExist) {
                            // * No intersections being found is fine. Skip to the next mapping.
                            // * The shard was not properly attached. 
                            // This is more than we can deal with in mapping resolution.
                            continue;
                        }
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }

                for (IStoreMapping gsmMapping : gsmMappingsByRange.getStoreMappings()) {
                    ShardKey retrievedMin = ShardKey.FromRawValue(ssmLocal.getKeyType(), gsmMapping.getMinValue());

                    ShardRange retrievedRange = null;

                    switch (ssmLocal.getMapType()) {
                        case Range:
                            ShardKey retrievedMax = ShardKey.FromRawValue(ssmLocal.getKeyType(), gsmMapping.getMaxValue());
                            retrievedRange = new ShardRange(retrievedMin, retrievedMax);
                            break;

                        default:
                            assert ssmLocal.getMapType() == ShardMapType.List;
                            retrievedMax = ShardKey.FromRawValue(ssmLocal.getKeyType(), gsmMapping.getMinValue()).GetNextKey();
                            retrievedRange = new ShardRange(retrievedMin, retrievedMax);
                            break;
                    }

                    relevantGsmMappings.put(retrievedRange, gsmMapping);
                }
            }

            List<MappingComparisonResult> comparisonResults = null;

            switch (ssmLocal.getMapType()) {
                case Range:
                    comparisonResults = MappingComparisonUtils.CompareRangeMappings(ssmLocal, relevantGsmMappings.values(), lsmMappings.getStoreMappings());
                    break;

                default:
                    assert ssmLocal.getMapType() == ShardMapType.List;
                    comparisonResults = MappingComparisonUtils.ComparePointMappings(ssmLocal, relevantGsmMappings.values(), lsmMappings.getStoreMappings());
                    break;
            }

            // Now we have 2 sets of mappings. Each submapping generated from this function is
            //  1.) in the GSM only: report.
            //  2.) in the LSM only: report.
            //  3.) in both but with different version number: report.
            //  4.) in both with the same version number: skip.
            for (MappingComparisonResult r : comparisonResults) {
                switch (r.MappingLocation) {
                    case MappingLocation.MappingInShardMapOnly:
                    case MappingLocation.MappingInShardOnly:
                        break;
                    default:
                        assert r.MappingLocation == MappingLocation.MappingInShardMapAndShard;

                        if (r.ShardMapManagerMapping.getId() == r.ShardMapping.getId()) {
                            // No conflict found, skip to the next range.
                            continue;
                        }
                        break;
                }

                // Store the inconsistency for later reporting.
//TODO TASK: C# to Java Converter could not resolve the named parameters in the following line:
//ORIGINAL LINE: this.Inconsistencies[token][r.Range] = new MappingDifference(type: MappingDifferenceType.Range, location: r.MappingLocation, shardMap: r.ShardMap, mappingForShard: r.ShardMapping, mappingForShardMap: r.ShardMapManagerMapping);
                this.getInconsistencies().get(token).getItem(r.Range) = new MappingDifference(type:
                MappingDifferenceType.Range, location:r.MappingLocation, shardMap:r.ShardMap, mappingForShard:
                r.ShardMapping, mappingForShardMap:r.ShardMapManagerMapping);
            }
        }

        return listOfTokens;*/
        return null; //TODO:
    }

    /**
     * Selects one of the shard maps (either local or global) as a source of truth and brings
     * mappings on both shard maps in sync.
     *
     * @param token      Recovery token returned from <see cref="DetectMappingDifferences(ShardLocation, string)"/>.
     * @param resolution The resolution strategy to be used for resolution.
     *                   <p>
     *                   Note that this method can cause unrecoverable data loss. Make sure you have taken backups or copies
     *                   of your databases and only then proceed with great care.
     */
    public void ResolveMappingDifferences(RecoveryToken token, MappingDifferenceResolution resolution) {
        switch (resolution) {
            case KeepShardMapMapping:
                this.RestoreShardFromShardmap(token);
                break;
            case KeepShardMapping:
                this.RestoreShardMapFromShard(token);
                break;
            case Ignore:
                break;
            default:
                //Debug.Fail("Unexpected value for MappingDifferenceResolution.");
                return;
        }

        this.getStoreShardMaps().remove(token);
        this.getLocations().remove(token);
        this.getInconsistencies().remove(token);
    }

    ///#endregion

    ///#region Private Helper Methods

    private void RebuildMappingsHelper(String operationName, List<ShardLocation> shardLocations, MappingDifferenceResolution resolutionStrategy) {
        RebuildMappingsHelper(operationName, shardLocations, resolutionStrategy, null);
    }

    /**
     * Given a collection of shard locations, reconstructs the shard map manager based on information
     * stored in the individual shards.
     * If the information in the individual shard maps is or becomes inconsistent, behavior is undefined.
     * No cross shard locks are taken, so if any shards become inconsistent during the execution of this
     * method, the final state of the global shard map may be corrupt.
     *
     * @param operationName      Operation name.
     * @param shardLocations     Collection of shard locations.
     * @param resolutionStrategy Strategy for resolving the mapping differences.
     * @param shardMapName       Optional name of shard map. If omitted, will attempt to recover from all shard maps present on each shard.
     */
    private void RebuildMappingsHelper(String operationName, List<ShardLocation> shardLocations, MappingDifferenceResolution resolutionStrategy, String shardMapName) {
        assert shardLocations != null;

        List<RecoveryToken> idsToProcess = new ArrayList<RecoveryToken>();

        // Collect the shard map-shard pairings to recover. Give each of these pairings a token.
        for (ShardLocation shardLocation : shardLocations) {
            IStoreResults getShardsLocalResult;

            try (IStoreOperationLocal op = this.getManager().getStoreOperationFactory().CreateGetShardsLocalOperation(this.getManager(), shardLocation, operationName)) {
                getShardsLocalResult = op.Do();
            } catch (IOException e) {
                e.printStackTrace();
            }

            //TODO
            /*assert getShardsLocalResult.getResult() == StoreResult.Success;

//TODO TASK: There is no Java equivalent to LINQ queries:
            List<IStoreShardMap> shardMaps = shardMapName == null ? getShardsLocalResult.StoreShardMaps : getShardsLocalResult.StoreShardMaps.Where(s = shardMapName.equals( > s.Name))
            ;

            List<Pair<IStoreShardMap, StoreShard>> shardInfos = shardMaps.forEach(sm -> new Pair<IStoreShardMap, StoreShard>(sm, getShardsLocalResult.getStoreShards().get(s -> s.getShardMapId() == sm.getId())));

            for (Pair<IStoreShardMap, StoreShard> shardInfo : shardInfos) {
                RecoveryToken token = new RecoveryToken();

                idsToProcess.add(token);

                this.getStoreShardMaps().put(token, shardInfo);
                this.getLocations().put(token, shardLocation);
            }*/
        }

        // Recover from the shard map-shard pairing corresponding to the collected token.
        for (RecoveryToken token : idsToProcess) {
            this.ResolveMappingDifferences(token, resolutionStrategy);

            this.getStoreShardMaps().remove(token);
            this.getLocations().remove(token);
        }
    }

    /**
     * Attaches a shard to the shard map manager.
     *
     * @param token Token from DetectMappingDifferences.
     */
    private void RestoreShardMapFromShard(RecoveryToken token) {
        IStoreShardMap ssmLocal = null;

        ReferenceObjectHelper<IStoreShardMap> tempRef_ssmLocal = new ReferenceObjectHelper<IStoreShardMap>(ssmLocal);
        StoreShard dss = this.GetStoreShardFromToken("ResolveMappingDifferences", token, tempRef_ssmLocal);
        ssmLocal = tempRef_ssmLocal.argValue;

        IStoreResults lsmMappingsToRemove;

        try (IStoreOperationLocal op = this.getManager().getStoreOperationFactory().CreateGetMappingsByRangeLocalOperation(this.getManager(), dss.getLocation(), "ResolveMappingDifferences", ssmLocal, dss, null, false)) {
            lsmMappingsToRemove = op.Do();
        } catch (IOException e) {
            e.printStackTrace();
        }

        /*List<IStoreMapping> gsmMappingsToAdd = lsmMappingsToRemove.getStoreMappings().Select(mapping -> new DefaultStoreMapping(mapping.getId(), mapping.getShardMapId(), dss, mapping.getMinValue(), mapping.getMaxValue(), mapping.getStatus(), null));

        try (IStoreOperationGlobal op = this.getManager().getStoreOperationFactory().CreateReplaceMappingsGlobalOperation(this.getManager(), "ResolveMappingDifferences", ssmLocal, dss, lsmMappingsToRemove.getStoreMappings(), gsmMappingsToAdd)) {
            op.Do();
        } catch (IOException e) {
            e.printStackTrace();
        }*/
    }

    /**
     * Helper function to bring a Shard into a consistent state with a ShardMap.
     *
     * @param token Token from DetectMappingDifferences
     */
    private void RestoreShardFromShardmap(RecoveryToken token) {
        IStoreShardMap ssmLocal = null;

        ReferenceObjectHelper<IStoreShardMap> tempRef_ssmLocal = new ReferenceObjectHelper<IStoreShardMap>(ssmLocal);
        StoreShard dss = this.GetStoreShardFromToken("ResolveMappingDifferences", token, tempRef_ssmLocal);
        ssmLocal = tempRef_ssmLocal.argValue;

        IStoreResults gsmMappings = null;

        try (IStoreOperationGlobal op = this.getManager().getStoreOperationFactory().CreateGetMappingsByRangeGlobalOperation(this.getManager(), "ResolveMappingDifferences", ssmLocal, dss, null, ShardManagementErrorCategory.Recovery, false, false)) {
            gsmMappings = op.Do();
        } catch (Exception e) {
            e.printStackTrace();
        }

        try (IStoreOperationLocal op = this.getManager().getStoreOperationFactory().CreateReplaceMappingsLocalOperation(this.getManager(), dss.getLocation(), "ResolveMappingDifferences", ssmLocal, dss, null, gsmMappings.getStoreMappings())) {
            op.Do();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * Helper function to obtain a store shard object from given recovery token.
     *
     * @param operationName Operation name.
     * @param token         Token from DetectMappingDifferences.
     * @param ssmLocal      Reference to store shard map corresponding to the token.
     * @return Store shard object corresponding to given token, or null if shard map is default shard map.
     */
    private StoreShard GetStoreShardFromToken(String operationName, RecoveryToken token, ReferenceObjectHelper<IStoreShardMap> ssmLocal) {
        Pair<IStoreShardMap, StoreShard> shardInfoLocal = null;

        if (!(this.getStoreShardMaps().containsKey(token) ? (shardInfoLocal = this.getStoreShardMaps().get(token)) == shardInfoLocal : false)) {
            throw new IllegalArgumentException(StringUtilsLocal.FormatInvariant(Errors._Recovery_InvalidRecoveryToken, token), new Throwable("token"));
        }

        //TODO
        /*ssmLocal.argValue = shardInfoLocal.Item1;
        StoreShard ssLocal = shardInfoLocal.Item2;*/

        ShardLocation location = this.GetShardLocation(token);

        try (IStoreOperationLocal op = this.getManager().getStoreOperationFactory().CreateCheckShardLocalOperation(operationName, this.getManager(), location)) {
            op.Do();
        } catch (IOException e) {
            e.printStackTrace();
        }

        return null; //TODO: new DefaultStoreShard(ssLocal.getId(), ssLocal.getVersion(), ssLocal.getShardMapId(), ssLocal.getLocation(), ssLocal.getStatus());
    }

    ///#endregion
}

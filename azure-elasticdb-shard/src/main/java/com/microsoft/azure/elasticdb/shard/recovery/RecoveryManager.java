package com.microsoft.azure.elasticdb.shard.recovery;

/* Copyright (c) Microsoft. All rights reserved.
Licensed under the MIT license. See LICENSE file in the project root for full license information.*/

import com.microsoft.azure.elasticdb.core.commons.helpers.ReferenceObjectHelper;
import com.microsoft.azure.elasticdb.shard.base.ShardKey;
import com.microsoft.azure.elasticdb.shard.base.ShardLocation;
import com.microsoft.azure.elasticdb.shard.base.ShardRange;
import com.microsoft.azure.elasticdb.shard.cache.CacheStoreMappingUpdatePolicy;
import com.microsoft.azure.elasticdb.shard.map.ShardMapType;
import com.microsoft.azure.elasticdb.shard.mapmanager.ShardManagementErrorCategory;
import com.microsoft.azure.elasticdb.shard.mapmanager.ShardManagementException;
import com.microsoft.azure.elasticdb.shard.mapmanager.ShardMapManager;
import com.microsoft.azure.elasticdb.shard.store.StoreMapping;
import com.microsoft.azure.elasticdb.shard.store.StoreResult;
import com.microsoft.azure.elasticdb.shard.store.StoreResults;
import com.microsoft.azure.elasticdb.shard.store.StoreShard;
import com.microsoft.azure.elasticdb.shard.store.StoreShardMap;
import com.microsoft.azure.elasticdb.shard.storeops.base.IStoreOperation;
import com.microsoft.azure.elasticdb.shard.storeops.base.IStoreOperationGlobal;
import com.microsoft.azure.elasticdb.shard.storeops.base.IStoreOperationLocal;
import com.microsoft.azure.elasticdb.shard.utils.Errors;
import com.microsoft.azure.elasticdb.shard.utils.ExceptionUtils;
import com.microsoft.azure.elasticdb.shard.utils.StringUtilsLocal;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;

/**
 * Manages various recovery related tasks for a shard map manager. It helps resolving data
 * corruption issues between shard map information stored locally on the shards and in the global
 * shard map manager database. It also helps with certain 'oops' recovery scenarios where
 * reconstruction of shard maps from database backups or database copies is necessary. Note that
 * some of the recovery methods can cause unrecoverable data loss when not used properly. It is
 * recommend to take backups or copies of all databases that participate in recovery operations.
 */
public final class RecoveryManager {

  /**
   * Cached list of inconsistencies so user can resolve without knowing the data format.
   */
  private Map<RecoveryToken, Map<ShardRange, MappingDifference>> inconsistencies;

  /**
   * Cached list of IStoreShardMaps so user can reconstruct shards based on a token.
   */
  private Map<RecoveryToken, Pair<StoreShardMap, StoreShard>> storeShardMaps;
  /**
   * Cached list of ShardLocations so user can determine ShardLocation based on a token.
   */
  private Map<RecoveryToken, ShardLocation> locations;
  /**
   * Reference to the associated shard map manager.
   */
  private ShardMapManager shardMapManager;

  /**
   * Constructs an instance of the recovery manager for given shard map manager.
   *
   * @param shardMapManager Shard map manager being recovered.
   */
  public RecoveryManager(ShardMapManager shardMapManager) {
    assert shardMapManager != null;
    this.setShardMapManager(shardMapManager);
    this.inconsistencies = new HashMap<>();
    this.storeShardMaps = new HashMap<>();
    this.setLocations(new HashMap<>());
  }

  private Map<RecoveryToken, Map<ShardRange, MappingDifference>> getInconsistencies() {
    return inconsistencies;
  }

  private void setInconsistencies(Map<RecoveryToken, Map<ShardRange, MappingDifference>> value) {
    inconsistencies.putAll(value);
  }

  private Map<RecoveryToken, Pair<StoreShardMap, StoreShard>> getStoreShardMaps() {
    return storeShardMaps;
  }

  private void setStoreShardMaps(Map<RecoveryToken, Pair<StoreShardMap, StoreShard>> value) {
    storeShardMaps.putAll(value);
  }

  private Map<RecoveryToken, ShardLocation> getLocations() {
    return locations;
  }

  private void setLocations(Map<RecoveryToken, ShardLocation> value) {
    locations = value;
  }

  private ShardMapManager getShardMapManager() {
    return shardMapManager;
  }

  private void setShardMapManager(ShardMapManager value) {
    shardMapManager = value;
  }

  /**
   * Attaches a shard to the shard map manager. Earlier versions
   * of mappings for the same shard map will automatically be updated
   * if more recent versions are found on the shard to be attached.
   * After attaching a shard, <see cref="DetectMappingDifferences(ShardLocation, string)"/>
   * should be called to check for any inconsistencies that warrant
   * manual conflict resolution.
   *
   * @param location Location of the shard being attached.  Note that this method can cause
   * unrecoverable data loss. Make sure you have taken backups or copies of your databases and only
   * then proceed with great care.
   */
  public void attachShard(ShardLocation location) {
    this.attachShard(location, null);
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
   * @param location Location of the shard being attached.
   * @param shardMapName Optional string to filter on the shard map name.  Note that this method can
   * cause unrecoverable data loss. Make sure you have taken backups or copies of your databases and
   * only then proceed with great care.
   */
  public void attachShard(ShardLocation location, String shardMapName) {
    ExceptionUtils.disallowNullArgument(location, "location");

    StoreResults result;

    try (IStoreOperationLocal op = this.getShardMapManager().getStoreOperationFactory()
        .createGetShardsLocalOperation(this.getShardMapManager(), location, "AttachShard")) {
      result = op.doLocal();
    } catch (IOException e) {
      e.printStackTrace();
      throw (ShardManagementException) e.getCause();
    }

    assert result.getResult() == StoreResult.Success;

    List<StoreShardMap> shardMaps = shardMapName == null ? result.getStoreShardMaps()
        : result.getStoreShardMaps().stream()
            .filter(s -> shardMapName.equals(s.getName())).collect(Collectors.toList());

    shardMaps.forEach((sm) -> {
      StoreShard shard = result.getStoreShards().stream()
          .filter(s -> s.getShardMapId().equals(sm.getId())).findFirst().orElse(null);

      // construct a new store shard with correct location
      StoreShard ssNew = new StoreShard(shard.getId(), shard.getVersion(),
          shard.getShardMapId(), location, shard.getStatus());

      try (IStoreOperation op = this.getShardMapManager().getStoreOperationFactory()
          .createAttachShardOperation(this.getShardMapManager(), sm, ssNew)) {
        op.doOperation();
      } catch (Exception e) {
        e.printStackTrace();
        throw (ShardManagementException) e.getCause();
      }
    });
  }

  /**
   * Detaches the given shard from the shard map manager. Mappings pointing to the
   * shard to be deleted will automatically be removed by this method.
   *
   * @param location Location of the shard being detached.  Note that this method can cause
   * unrecoverable data loss. Make sure you have taken backups or copies of your databases and only
   * then proceed with great care.
   */
  public void detachShard(ShardLocation location) {
    this.detachShard(location, null);
  }

  /**
   * Detaches the given shard from the shard map manager. Mappings pointing to the
   * shard to be deleted will automatically be removed by this method.
   *
   * @param location Location of the shard being detached.
   * @param shardMapName Optional string to filter on shard map name.  Note that this method can
   * cause unrecoverable data loss. Make sure you have taken backups or copies of your databases and
   * only then proceed with great care.
   */
  public void detachShard(ShardLocation location, String shardMapName) {
    ExceptionUtils.disallowNullArgument(location, "location");

    try (IStoreOperationGlobal op = this.getShardMapManager().getStoreOperationFactory()
        .createDetachShardGlobalOperation(this.getShardMapManager(), "DetachShard", location,
            shardMapName)) {
      op.doGlobal();
    } catch (Exception e) {
      e.printStackTrace();
      throw (ShardManagementException) e.getCause();
    }
  }

  /**
   * Returns a dictionary of range-to-location key-value pairs. The location returned is an
   * enumerator stating whether a given range (or point) is present only in the local shard map,
   * only in the global shard map, or both. Ranges not contained in either shard map cannot contain
   * differences so those ranges are not shown.
   *
   * @param token Recovery token returned from <see cref="DetectMappingDifferences(ShardLocation,
   * string)"/>.
   * @return The set of ranges and their corresponding <see cref="MappingLocation"/>.  This method
   * assumes a previous call to <see cref="DetectMappingDifferences(ShardLocation, string)"/> that
   * provides the recovery token parameter. The result of this method is typically used in
   * subsequent calls to resolve inconsistencies such as <see cref="ResolveMappingDifferences"/> or
   * <see cref="RebuildMappingsOnShard"/>.
   */
  public Map<ShardRange, MappingLocation> getMappingDifferences(RecoveryToken token) {
    ExceptionUtils.disallowNullArgument(token, "token");

    if (!this.getInconsistencies().containsKey(token)) {
      throw new IllegalArgumentException(StringUtilsLocal.formatInvariant(
          Errors._Recovery_InvalidRecoveryToken, token), new Throwable("token"));
    }

    Map<ShardRange, MappingLocation> map = new HashMap<>();
    this.getInconsistencies().get(token).forEach((k, v) -> map.put(k, v.getLocation()));
    return map;
  }

  /**
   * Retrieves shard map type, name and shard location based on the token returned from <see
   * cref="DetectMappingDifferences(ShardLocation, string)"/>.
   *
   * @param token Recovery token returned from <see cref="DetectMappingDifferences(ShardLocation,
   * string)"/>.
   * @param mapType Outputs shard map type (Range or List).
   * @param shardMapName Outputs shard map name.
   * @param shardLocation Outputs shard location
   */
  public void getShardInfo(RecoveryToken token, ReferenceObjectHelper<ShardMapType> mapType,
      ReferenceObjectHelper<String> shardMapName,
      ReferenceObjectHelper<ShardLocation> shardLocation) {
    ExceptionUtils.disallowNullArgument(token, "token");

    Pair<StoreShardMap, StoreShard> shardInfoLocal;

    if (this.getStoreShardMaps().containsKey(token)) {
      shardInfoLocal = this.getStoreShardMaps().get(token);
    } else {
      throw new IllegalArgumentException(StringUtilsLocal.formatInvariant(
          Errors._Recovery_InvalidRecoveryToken, token), new Throwable("token"));
    }

    mapType.argValue = shardInfoLocal.getLeft().getMapType();
    shardMapName.argValue = shardInfoLocal.getLeft().getName();

    if (this.getLocations().containsKey(token)) {
      shardLocation.argValue = this.getLocations().get(token);
    } else {
      throw new IllegalArgumentException(StringUtilsLocal.formatInvariant(
          Errors._Recovery_InvalidRecoveryToken, token), new Throwable("token"));
    }
  }

  /**
   * Retrieves shard map type and name based on the token returned from <see
   * cref="DetectMappingDifferences(ShardLocation, string)"/>.
   *
   * @param token Recovery token returned from <see cref="DetectMappingDifferences(ShardLocation,
   * string)"/>.
   * @param mapType Output ShardMap type (Range or List).
   * @param shardMapName Output name of shard map.
   */
  public void getShardInfo(RecoveryToken token, ReferenceObjectHelper<ShardMapType> mapType,
      ReferenceObjectHelper<String> shardMapName) {
    ExceptionUtils.disallowNullArgument(token, "token");

    Pair<StoreShardMap, StoreShard> shardInfoLocal;

    if (this.getStoreShardMaps().containsKey(token)) {
      shardInfoLocal = this.getStoreShardMaps().get(token);
    } else {
      throw new IllegalArgumentException(StringUtilsLocal.formatInvariant(
          Errors._Recovery_InvalidRecoveryToken, token), new Throwable("token"));
    }

    mapType.argValue = shardInfoLocal.getLeft().getMapType();
    shardMapName.argValue = shardInfoLocal.getLeft().getName();
  }

  /**
   * Returns the shard map type of the shard map processed by
   * <see cref="DetectMappingDifferences(ShardLocation, string)"/>.
   *
   * @param token Recovery token returned from <see cref="DetectMappingDifferences(ShardLocation,
   * string)"/>.
   * @return The type of shard map (list, range, etc...) corresponding to the recovery token.
   */
  public ShardMapType getShardMapType(RecoveryToken token) {
    ExceptionUtils.disallowNullArgument(token, "token");

    Pair<StoreShardMap, StoreShard> shardInfoLocal;

    if (this.getStoreShardMaps().containsKey(token)) {
      shardInfoLocal = this.getStoreShardMaps().get(token);
    } else {
      throw new IllegalArgumentException(StringUtilsLocal.formatInvariant(
          Errors._Recovery_InvalidRecoveryToken, token), new Throwable("token"));
    }

    return shardInfoLocal.getLeft().getMapType();
  }

  /**
   * Returns the shard map name of the shard map processed by
   * <see cref="DetectMappingDifferences(ShardLocation, string)"/>.
   *
   * @param token Recovery token returned from <see cref="DetectMappingDifferences(ShardLocation,
   * string)"/>.
   * @return The name of the shard map for the given recovery token.
   */
  public String getShardMapName(RecoveryToken token) {
    ExceptionUtils.disallowNullArgument(token, "token");

    Pair<StoreShardMap, StoreShard> shardInfoLocal;

    if (this.getStoreShardMaps().containsKey(token)) {
      shardInfoLocal = this.getStoreShardMaps().get(token);
    } else {
      throw new IllegalArgumentException(StringUtilsLocal.formatInvariant(
          Errors._Recovery_InvalidRecoveryToken, token), new Throwable("token"));
    }

    return shardInfoLocal.getLeft().getName();
  }

  /**
   * Returns the shard location of the local shard map processed by <see
   * cref="DetectMappingDifferences(ShardLocation, string)"/>.
   *
   * @param token Recovery token returned from <see cref="DetectMappingDifferences(ShardLocation,
   * string)"/>
   * @return Location of the shard corresponding to the set of mapping differences detected in <see
   * cref="DetectMappingDifferences(ShardLocation, string)"/>.
   */
  public ShardLocation getShardLocation(RecoveryToken token) {
    ExceptionUtils.disallowNullArgument(token, "token");

    ShardLocation shardLocation;

    if (this.getLocations().containsKey(token)) {
      shardLocation = this.getLocations().get(token);
    } else {
      throw new IllegalArgumentException(StringUtilsLocal.formatInvariant(
          Errors._Recovery_InvalidRecoveryToken, token), new Throwable("token"));
    }

    return shardLocation;
  }

  /**
   * Given a collection of shard locations, reconstructs local shard maps based
   * on the mapping information stored in the global shard map. The specified
   * shards need to be registered already in the global shard map. This method only
   * rebuilds mappings. It does not rebuild shard membership within the global shard map.
   *
   * @param shardLocations Collection of shard locations.  Note that this method can cause
   * unrecoverable data loss. Make sure you have taken backups or copies of your databases and only
   * then proceed with great care.
   */
  public void rebuildMappingsOnShardsFromShardMapManager(List<ShardLocation> shardLocations) {
    this.rebuildMappingsOnShardsFromShardMapManager(shardLocations, null);
  }

  /**
   * Given a collection of shard locations, reconstructs local shard maps based
   * on the mapping information stored in the global shard map. The specified
   * shards need to be registered already in the global shard map. This method only
   * rebuilds mappings. It does not rebuild shard membership within the global shard map.
   *
   * @param shardLocations Collection of shard locations.
   * @param shardMapName Optional parameter to filter by shard map name. If omitted, all shard maps
   * will be rebuilt.  Note that this method can cause unrecoverable data loss. Make sure you have
   * taken backups or copies of your databases and only then proceed with great care.
   */
  public void rebuildMappingsOnShardsFromShardMapManager(List<ShardLocation> shardLocations,
      String shardMapName) {
    ExceptionUtils.disallowNullArgument(shardLocations, "shardLocations");

    this.rebuildMappingsHelper("RebuildMappingsOnShardsFromShardMapManager", shardLocations,
        MappingDifferenceResolution.KeepShardMapMapping, shardMapName);
  }

  /**
   * Given a collection of shard locations, reconstructs the shard map manager based on mapping
   * information stored in the individual shards. The specified shards need to be registered already
   * in the global shard map. This method only rebuilds mappings. It does not rebuild shard
   * membership within the global shard map. If the information in the individual shard maps is or
   * becomes inconsistent, the behavior is undefined. No cross shard locks are taken, so if any
   * shards become inconsistent during the execution of this method, the final state of the global
   * shard map may be corrupt.
   *
   * @param shardLocations Collection of shard locations.  Note that this method can cause
   * unrecoverable data loss. Make sure you have taken backups or copies of your databases and only
   * then proceed with great care.
   */
  public void rebuildMappingsOnShardMapManagerFromShards(List<ShardLocation> shardLocations) {
    rebuildMappingsOnShardMapManagerFromShards(shardLocations, null);
  }

  /**
   * Given a collection of shard locations, reconstructs the shard map manager based on mapping
   * information stored in the individual shards. The specified shards need to be registered already
   * in the global shard map. This method only rebuilds mappings. It does not rebuild shard
   * membership within the global shard map. If the information in the individual shard maps is or
   * becomes inconsistent, the behavior is undefined. No cross shard locks are taken, so if any
   * shards become inconsistent during the execution of this method, the final state of the global
   * shard map may be corrupt.
   *
   * @param shardLocations Collection of shard locations.
   * @param shardMapName Optional name of shard map. If omitted, will attempt to recover from all
   * shard maps present on each shard.  Note that this method can cause unrecoverable data loss.
   * Make sure you have taken backups or copies of your databases and only then proceed with great
   * care.
   */
  public void rebuildMappingsOnShardMapManagerFromShards(List<ShardLocation> shardLocations,
      String shardMapName) {
    ExceptionUtils.disallowNullArgument(shardLocations, "shardLocations");

    this.rebuildMappingsHelper("RebuildMappingsOnShardMapManagerFromShards", shardLocations,
        MappingDifferenceResolution.KeepShardMapping, shardMapName);
  }

  /**
   * Rebuilds a local range shard map from a list of inconsistent shard ranges detected by <see
   * cref="DetectMappingDifferences(ShardLocation, string)"/> and then accessed by <see
   * cref="GetMappingDifferences"/>. The resulting local range shard map will always still be
   * inconsistent with the global shard map in the shard map manager database. A subsequent call to
   * <see cref="ResolveMappingDifferences"/> is necessary to bring the system back to a healthy
   * state.
   *
   * @param token The recovery token from a previous call to DetectMappingDifferences
   * @param ranges The set of ranges to keep on the local shard when rebuilding the local shard map.
   * Note that this method can cause unrecoverable data loss. Make sure you have taken backups or
   * copies of your databases and only then proceed with great care.  Only shard ranges with
   * inconsistencies can be rebuilt using this method. All ranges with no inconsistencies between
   * the local shard and the global shard map will be kept intact on the local shard and are not
   * affected by this call. Subsequent changes to the non-conflicting mappings can be made later
   * using the regular interfaces in the shard map manager. It is not necessary to use the recovery
   * manager to change non-conflicting mappings.
   */
  public void rebuildMappingsOnShard(RecoveryToken token, List<ShardRange> ranges) {
    ExceptionUtils.disallowNullArgument(token, "token");
    ExceptionUtils.disallowNullArgument(ranges, "ranges");

    ShardLocation location = this.getShardLocation(token);

    if (!this.getInconsistencies().containsKey(token)) {
      throw new IllegalArgumentException(StringUtilsLocal.formatInvariant(
          Errors._Recovery_InvalidRecoveryToken, token), new Throwable("token"));
    }

    StoreShardMap ssmLocal;

    ReferenceObjectHelper<StoreShardMap> refSsmLocal = new ReferenceObjectHelper<>(null);
    StoreShard dss = this.getStoreShardFromToken("RebuildMappingsOnShard", token, refSsmLocal);
    ssmLocal = refSsmLocal.argValue;

    List<StoreMapping> mappingsToAdd = new ArrayList<>();

    // Determine the ranges we want to keep based on input keeps list.
    for (ShardRange range : ranges) {
      MappingDifference difference = this.getInconsistencies().get(token).getOrDefault(range, null);
      if (difference == null) {
        throw new IllegalArgumentException(StringUtilsLocal.formatInvariant(
            Errors._Recovery_InvalidRebuildShardSpecification, range, location),
            new Throwable("ranges"));
      }

      // The storeMapping we will use as a template
      StoreMapping storeMappingTemplate
          = difference.getLocation().equals(MappingLocation.MappingInShardMapOnly)
          ? difference.getMappingForShardMap()
          : difference.getMappingForShard();

      StoreMapping storeMappingToAdd = new StoreMapping(UUID.randomUUID(),
          storeMappingTemplate.getShardMapId(), range.getLow().getRawValue(),
          range.getHigh().getRawValue(), storeMappingTemplate.getStatus(), null, dss);

      mappingsToAdd.add(storeMappingToAdd);
    }

    try (IStoreOperationLocal op = this.getShardMapManager().getStoreOperationFactory()
        .createReplaceMappingsLocalOperation(this.getShardMapManager(), location,
            "RebuildMappingsOnShard", ssmLocal, dss, new ArrayList<>(this.getInconsistencies()
                .get(token).keySet()), mappingsToAdd)) {
      op.doLocal();
    } catch (IOException e) {
      e.printStackTrace();
      throw (ShardManagementException) e.getCause();
    }

    this.getStoreShardMaps().remove(token);
    this.getLocations().remove(token);
    this.getInconsistencies().remove(token);
  }

  /**
   * Enumerates differences in the mappings between the global shard map manager database and the
   * local shard database in the specified shard location.
   *
   * @param location Location of shard for which to detect inconsistencies.
   * @return Collection of tokens to be used for further resolution tasks (see <see
   * cref="ResolveMappingDifferences"/>).
   */
  public List<RecoveryToken> detectMappingDifferences(ShardLocation location) {
    return this.detectMappingDifferences(location, null);
  }

  /**
   * Enumerates differences in the mappings between the global shard map manager database and the
   * local shard database in the specified shard location.
   *
   * @param location Location of shard for which to detect inconsistencies.
   * @param shardMapName Optional parameter to specify a particular shard map.
   * @return Collection of tokens to be used for further resolution tasks (see <see
   * cref="ResolveMappingDifferences"/>).
   */
  public List<RecoveryToken> detectMappingDifferences(ShardLocation location, String shardMapName) {
    ExceptionUtils.disallowNullArgument(location, "location");

    List<RecoveryToken> listOfTokens = new ArrayList<>();

    StoreResults getShardsLocalResult;

    try (IStoreOperationLocal op = this.getShardMapManager().getStoreOperationFactory()
        .createGetShardsLocalOperation(this.getShardMapManager(), location,
            "DetectMappingDifferences")) {
      getShardsLocalResult = op.doLocal();
    } catch (IOException e) {
      e.printStackTrace();
      throw (ShardManagementException) e.getCause();
    }

    assert getShardsLocalResult.getResult() == StoreResult.Success;

    List<StoreShardMap> shardMaps = shardMapName == null ? getShardsLocalResult.getStoreShardMaps()
        : getShardsLocalResult.getStoreShardMaps().stream()
            .filter(s -> shardMapName.equals(s.getName())).collect(Collectors.toList());

    List<Pair<StoreShardMap, StoreShard>> shardInfos = shardMaps.stream()
        .map(sm -> new ImmutablePair<>(sm, getShardsLocalResult.getStoreShards().stream()
            .filter(s -> s.getShardMapId().equals(sm.getId())).findFirst().orElse(null)))
        .collect(Collectors.toList());

    for (Pair<StoreShardMap, StoreShard> shardInfo : shardInfos) {

      RecoveryToken token = new RecoveryToken();

      listOfTokens.add(token);
      this.getStoreShardMaps().put(token, shardInfo);
      this.getLocations().put(token, location);

      this.getInconsistencies().put(token, new HashMap<>());
      StoreShard ssLocal = shardInfo.getRight();
      StoreShardMap ssmLocal = shardInfo.getLeft();

      StoreShard dss = new StoreShard(ssLocal.getId(), ssLocal.getVersion(),
          ssLocal.getShardMapId(), ssLocal.getLocation(), ssLocal.getStatus());

      // First get all local mappings.
      StoreResults lsmMappings;

      try (IStoreOperationLocal op = this.getShardMapManager().getStoreOperationFactory()
          .createGetMappingsByRangeLocalOperation(this.getShardMapManager(), location,
              "DetectMappingDifferences", ssmLocal, dss, null, true)) {
        lsmMappings = op.doLocal();

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
        throw (ShardManagementException) e.getCause();
      }

      // Next build up a set of relevant global mappings.
      // This is the union of those mappings that are associated with this local shard
      // and those mappings which intersect with mappings found in the local shard.
      // We will partition these mappings based on ranges.
      Map<ShardRange, StoreMapping> relevantGsmMappings = new HashMap<>();

      StoreResults gsmMappingsByMap;

      try (IStoreOperationGlobal op = this.getShardMapManager().getStoreOperationFactory()
          .createGetMappingsByRangeGlobalOperation(this.getShardMapManager(),
              "DetectMappingDifferences", ssmLocal, dss, null,
              ShardManagementErrorCategory.Recovery, false, true)) {
        gsmMappingsByMap = op.doGlobal();
      } catch (Exception e) {
        e.printStackTrace();
        throw (ShardManagementException) e.getCause();
      }

      if (gsmMappingsByMap.getResult() == StoreResult.ShardMapDoesNotExist) {
        // The shard map is not properly attached to this GSM.
        // This is beyond what we can handle resolving mappings.
        continue;
      }

      for (StoreMapping gsmMapping : gsmMappingsByMap.getStoreMappings()) {
        ShardKey min = ShardKey.fromRawValue(ssmLocal.getKeyType(), gsmMapping.getMinValue());

        ShardKey max;

        switch (ssmLocal.getMapType()) {
          case Range:
            max = ShardKey.fromRawValue(ssmLocal.getKeyType(), gsmMapping.getMaxValue());
            break;

          default:
            assert ssmLocal.getMapType() == ShardMapType.List;
            max = ShardKey.fromRawValue(ssmLocal.getKeyType(), gsmMapping.getMinValue())
                .getNextKey();
            break;
        }

        ShardRange range = new ShardRange(min, max);

        relevantGsmMappings.put(range, gsmMapping);
      }

      // Next, for each of the mappings in lsmMappings, we need to augment
      // the gsmMappingsByMap by intersecting ranges.
      for (StoreMapping lsmMapping : lsmMappings.getStoreMappings()) {
        ShardKey min = ShardKey.fromRawValue(ssmLocal.getKeyType(), lsmMapping.getMinValue());

        StoreResults gsmMappingsByRange;

        if (ssmLocal.getMapType() == ShardMapType.Range) {
          ShardKey max = ShardKey.fromRawValue(ssmLocal.getKeyType(), lsmMapping.getMaxValue());

          ShardRange range = new ShardRange(min, max);

          try (IStoreOperationGlobal op = this.getShardMapManager().getStoreOperationFactory()
              .createGetMappingsByRangeGlobalOperation(this.getShardMapManager(),
                  "DetectMappingDifferences", ssmLocal, null, range,
                  ShardManagementErrorCategory.Recovery, false, true)) {
            gsmMappingsByRange = op.doGlobal();
          } catch (Exception e) {
            e.printStackTrace();
            throw (ShardManagementException) e.getCause();
          }

          if (gsmMappingsByRange.getResult() == StoreResult.ShardMapDoesNotExist) {
            // The shard was not properly attached.
            // This is more than we can deal with in mapping resolution.
            continue;
          }
        } else {
          assert ssmLocal.getMapType() == ShardMapType.List;
          try (IStoreOperationGlobal op = this.getShardMapManager().getStoreOperationFactory()
              .createFindMappingByKeyGlobalOperation(this.getShardMapManager(),
                  "DetectMappingDifferences", ssmLocal, min,
                  CacheStoreMappingUpdatePolicy.OverwriteExisting,
                  ShardManagementErrorCategory.Recovery, false, true)) {
            gsmMappingsByRange = op.doGlobal();

            if (gsmMappingsByRange.getResult() == StoreResult.MappingNotFoundForKey
                || gsmMappingsByRange.getResult() == StoreResult.ShardMapDoesNotExist) {
              // * No intersections being found is fine. Skip to the next mapping.
              // * The shard was not properly attached.
              // This is more than we can deal with in mapping resolution.
              continue;
            }
          } catch (Exception e) {
            e.printStackTrace();
            throw (ShardManagementException) e.getCause();
          }
        }

        for (StoreMapping gsmMapping : gsmMappingsByRange.getStoreMappings()) {
          ShardKey retrievedMin = ShardKey.fromRawValue(ssmLocal.getKeyType(),
              gsmMapping.getMinValue());

          ShardRange retrievedRange;

          switch (ssmLocal.getMapType()) {
            case Range:
              ShardKey retrievedMax = ShardKey.fromRawValue(ssmLocal.getKeyType(),
                  gsmMapping.getMaxValue());
              retrievedRange = new ShardRange(retrievedMin, retrievedMax);
              break;

            default:
              assert ssmLocal.getMapType() == ShardMapType.List;
              retrievedMax = ShardKey.fromRawValue(ssmLocal.getKeyType(), gsmMapping.getMinValue())
                  .getNextKey();
              retrievedRange = new ShardRange(retrievedMin, retrievedMax);
              break;
          }

          relevantGsmMappings.put(retrievedRange, gsmMapping);
        }
      }

      List<MappingComparisonResult> comparisonResults;
      Map<ShardRange, MappingDifference> innerMap = new HashMap<>();

      switch (ssmLocal.getMapType()) {
        case Range:
          comparisonResults = MappingComparisonUtils.compareRangeMappings(ssmLocal,
              new ArrayList<>(relevantGsmMappings.values()), lsmMappings.getStoreMappings());
          break;

        default:
          assert ssmLocal.getMapType() == ShardMapType.List;
          comparisonResults = MappingComparisonUtils.comparePointMappings(ssmLocal,
              new ArrayList<>(relevantGsmMappings.values()), lsmMappings.getStoreMappings());
          break;
      }

      // Now we have 2 sets of mappings. Each sub mapping generated from this function is
      //  1.) in the GSM only: report.
      //  2.) in the LSM only: report.
      //  3.) in both but with different version number: report.
      //  4.) in both with the same version number: skip.
      for (MappingComparisonResult r : comparisonResults) {
        switch (r.getMappingLocation()) {
          case MappingInShardMapOnly:
          case MappingInShardOnly:
            break;
          default:
            assert r.getMappingLocation() == MappingLocation.MappingInShardMapAndShard;

            if (r.getShardMapManagerMapping().getId().equals(r.getShardMapping().getId())) {
              // No conflict found, skip to the next range.
              continue;
            }
            break;
        }

        // Store the inconsistency for later reporting.
        innerMap.put(r.getRange(), new MappingDifference(MappingDifferenceType.Range,
            r.getMappingLocation(), r.getShardMap(), r.getShardMapManagerMapping(),
            r.getShardMapping()));
      }

      this.getInconsistencies().get(token).putAll(innerMap);
    }

    return listOfTokens;
  }

  /**
   * Selects one of the shard maps (either local or global) as a source of truth and brings
   * mappings on both shard maps in sync.
   *
   * @param token Recovery token returned from <see cref="DetectMappingDifferences(ShardLocation,
   * string)"/>.
   * @param resolution The resolution strategy to be used for resolution.  Note that this method can
   * cause unrecoverable data loss. Make sure you have taken backups or copies of your databases and
   * only then proceed with great care.
   */
  public void resolveMappingDifferences(RecoveryToken token,
      MappingDifferenceResolution resolution) {
    switch (resolution) {
      case KeepShardMapMapping:
        this.restoreShardFromShardMap(token);
        break;
      case KeepShardMapping:
        this.restoreShardMapFromShard(token);
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

  private void rebuildMappingsHelper(String operationName, List<ShardLocation> shardLocations,
      MappingDifferenceResolution resolutionStrategy) {
    rebuildMappingsHelper(operationName, shardLocations, resolutionStrategy, null);
  }

  /**
   * Given a collection of shard locations, reconstructs the shard map manager based on information
   * stored in the individual shards. If the information in the individual shard maps is or becomes
   * inconsistent, behavior is undefined. No cross shard locks are taken, so if any shards become
   * inconsistent during the execution of this method, the final state of the global shard map may
   * be corrupt.
   *
   * @param operationName Operation name.
   * @param shardLocations Collection of shard locations.
   * @param resolutionStrategy Strategy for resolving the mapping differences.
   * @param shardMapName Optional name of shard map. If omitted, will attempt to recover from all
   * shard maps present on each shard.
   */
  private void rebuildMappingsHelper(String operationName, List<ShardLocation> shardLocations,
      MappingDifferenceResolution resolutionStrategy, String shardMapName) {
    assert shardLocations != null;

    List<RecoveryToken> idsToProcess = new ArrayList<>();

    // Collect the shard map-shard pairings to recover. Give each of these pairings a token.
    for (ShardLocation shardLocation : shardLocations) {
      StoreResults getShardsLocalResult;

      try (IStoreOperationLocal op = this.getShardMapManager().getStoreOperationFactory()
          .createGetShardsLocalOperation(this.getShardMapManager(), shardLocation, operationName)) {
        getShardsLocalResult = op.doLocal();
      } catch (IOException e) {
        e.printStackTrace();
        throw (ShardManagementException) e.getCause();
      }

      assert getShardsLocalResult != null
          && getShardsLocalResult.getResult() == StoreResult.Success;

      List<StoreShardMap> shardMaps = shardMapName == null
          ? getShardsLocalResult.getStoreShardMaps()
          : getShardsLocalResult.getStoreShardMaps().stream().filter(s
              -> shardMapName.equals(s.getName())).collect(Collectors.toList());

      StoreResults finalGetShardsLocalResult = getShardsLocalResult;
      List<Pair<StoreShardMap, StoreShard>> shardInfos = shardMaps.stream()
          .map(sm -> new ImmutablePair<>(sm, finalGetShardsLocalResult.getStoreShards().stream()
              .filter(s -> s.getShardMapId().equals(sm.getId())).findFirst().orElse(null)))
          .collect(Collectors.toList());

      for (Pair<StoreShardMap, StoreShard> shardInfo : shardInfos) {
        RecoveryToken token = new RecoveryToken();

        idsToProcess.add(token);

        this.getStoreShardMaps().put(token, shardInfo);
        this.getLocations().put(token, shardLocation);
      }
    }

    // Recover from the shard map-shard pairing corresponding to the collected token.
    for (RecoveryToken token : idsToProcess) {
      this.resolveMappingDifferences(token, resolutionStrategy);

      this.getStoreShardMaps().remove(token);
      this.getLocations().remove(token);
    }
  }

  /**
   * Attaches a shard to the shard map manager.
   *
   * @param token Token from DetectMappingDifferences.
   */
  private void restoreShardMapFromShard(RecoveryToken token) {
    StoreShardMap ssmLocal;

    ReferenceObjectHelper<StoreShardMap> refSsmLocal = new ReferenceObjectHelper<>(null);
    StoreShard dss = this.getStoreShardFromToken("ResolveMappingDifferences", token, refSsmLocal);
    ssmLocal = refSsmLocal.argValue;

    StoreResults lsmMappingsToRemove;

    try (IStoreOperationLocal op = this.getShardMapManager().getStoreOperationFactory()
        .createGetMappingsByRangeLocalOperation(this.getShardMapManager(), dss.getLocation(),
            "ResolveMappingDifferences", ssmLocal, dss, null, false)) {
      lsmMappingsToRemove = op.doLocal();
    } catch (IOException e) {
      e.printStackTrace();
      throw (ShardManagementException) e.getCause();
    }

    List<StoreMapping> gsmMappingsToAdd = lsmMappingsToRemove.getStoreMappings().stream()
        .map(mapping -> new StoreMapping(mapping.getId(), mapping.getShardMapId(),
            mapping.getMinValue(), mapping.getMaxValue(), mapping.getStatus(), null, dss))
        .collect(Collectors.toList());

    try (IStoreOperationGlobal op = this.getShardMapManager().getStoreOperationFactory()
        .createReplaceMappingsGlobalOperation(this.getShardMapManager(),
            "ResolveMappingDifferences", ssmLocal, dss, lsmMappingsToRemove.getStoreMappings(),
            gsmMappingsToAdd)) {
      op.doGlobal();
    } catch (Exception e) {
      e.printStackTrace();
      throw (ShardManagementException) e.getCause();
    }
  }

  /**
   * Helper function to bring a Shard into a consistent state with a ShardMap.
   *
   * @param token Token from DetectMappingDifferences
   */
  private void restoreShardFromShardMap(RecoveryToken token) {
    StoreShardMap ssmLocal;

    ReferenceObjectHelper<StoreShardMap> refSsmLocal = new ReferenceObjectHelper<>(null);
    StoreShard dss = this.getStoreShardFromToken("ResolveMappingDifferences", token, refSsmLocal);
    ssmLocal = refSsmLocal.argValue;

    StoreResults gsmMappings;

    try (IStoreOperationGlobal op = this.getShardMapManager().getStoreOperationFactory()
        .createGetMappingsByRangeGlobalOperation(this.getShardMapManager(),
            "ResolveMappingDifferences", ssmLocal, dss, null, ShardManagementErrorCategory.Recovery,
            false, false)) {
      gsmMappings = op.doGlobal();
    } catch (Exception e) {
      e.printStackTrace();
      throw (ShardManagementException) e.getCause();
    }

    try (IStoreOperationLocal op = this.getShardMapManager().getStoreOperationFactory()
        .createReplaceMappingsLocalOperation(this.getShardMapManager(), dss.getLocation(),
            "ResolveMappingDifferences", ssmLocal, dss, null, gsmMappings.getStoreMappings())) {
      op.doLocal();
    } catch (Exception e) {
      e.printStackTrace();
      throw (ShardManagementException) e.getCause();
    }
  }

  /**
   * Helper function to obtain a store shard object from given recovery token.
   *
   * @param operationName Operation name.
   * @param token Token from DetectMappingDifferences.
   * @param ssmLocal Reference to store shard map corresponding to the token.
   * @return Store shard object corresponding to given token, or null if shard map is default shard
   * map.
   */
  private StoreShard getStoreShardFromToken(String operationName, RecoveryToken token,
      ReferenceObjectHelper<StoreShardMap> ssmLocal) {
    Pair<StoreShardMap, StoreShard> shardInfoLocal;

    if (this.getStoreShardMaps().containsKey(token)) {
      shardInfoLocal = this.getStoreShardMaps().get(token);
    } else {
      throw new IllegalArgumentException(StringUtilsLocal.formatInvariant(
          Errors._Recovery_InvalidRecoveryToken, token), new Throwable("token"));
    }

    ssmLocal.argValue = shardInfoLocal.getLeft();
    StoreShard ssLocal = shardInfoLocal.getRight();

    ShardLocation location = this.getShardLocation(token);

    try (IStoreOperationLocal op = this.getShardMapManager().getStoreOperationFactory()
        .createCheckShardLocalOperation(operationName, this.getShardMapManager(), location)) {
      op.doLocal();
    } catch (IOException e) {
      e.printStackTrace();
      throw (ShardManagementException) e.getCause();
    }

    return new StoreShard(ssLocal.getId(), ssLocal.getVersion(), ssLocal.getShardMapId(),
        ssLocal.getLocation(), ssLocal.getStatus());
  }
}

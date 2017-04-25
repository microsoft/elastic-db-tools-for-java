package com.microsoft.azure.elasticdb.shard.mapmanager;

/* Copyright (c) Microsoft. All rights reserved.
Licensed under the MIT license. See LICENSE file in the project root for full license information.*/

/**
 * Represents error codes related to <see cref="ShardMapManager"/> operations.
 */
public enum ShardManagementErrorCode {
  /**
   * Successful execution.
   */
  Success(0),

  ///#region ShardMapManagerFactory

  /**
   * Store already exists on target shard map manager database.
   */
  ShardMapManagerStoreAlreadyExists(11),

  /**
   * Store does not exist on target shard map manager database.
   */
  ShardMapManagerStoreDoesNotExist(12),

  ///#endregion

  ///#region ShardMapManager

  /**
   * Shardmap with specified name already exists.
   */
  ShardMapAlreadyExists(21),

  /**
   * Shardmap with specified name not found.
   */
  ShardMapLookupFailure(22),

  /**
   * Shardmap has shards associated with it.
   */
  ShardMapHasShards(23),

  /**
   * GSM store version does not match with client library.
   */
  GlobalStoreVersionMismatch(24),

  /**
   * LSM store version does not match with client library.
   */
  LocalStoreVersionMismatch(25),

  /**
   * All necessary parameters for GSM stored procedure are not supplied.
   */
  GlobalStoreOperationInsufficientParameters(26),

  /**
   * All necessary parameters for LSM stored procedure are not supplied.
   */
  LocalStoreOperationInsufficientParameters(27),

  ///#endregion

  ///#region ShardMap

  /**
   * Conversion of shard map failed.
   */
  ShardMapTypeConversionError(31),

  /**
   * Shard has mappings associated with it.
   */
  ShardHasMappings(32),

  /**
   * Shard already exists.
   */
  ShardAlreadyExists(33),

  /**
   * Shard location already exists.
   */
  ShardLocationAlreadyExists(34),

  /**
   * Shard has been updated by concurrent user.
   */
  ShardVersionMismatch(35),

  ///#endregion

  ///#region PointMapping

  /**
   * Given point is already associated with a mapping.
   */
  MappingPointAlreadyMapped(41),

  ///#endregion PointMapping

  ///#region RangeMapping

  /**
   * Specified range is already associated with a mapping.
   */
  MappingRangeAlreadyMapped(51),

  ///#endregion RangeMapping

  ///#region Common

  /**
   * Storage operation failed.
   */
  StorageOperationFailure(61),

  /**
   * Shardmap does not exist any more.
   */
  ShardMapDoesNotExist(62),

  /**
   * Shard does not exist any more.
   */
  ShardDoesNotExist(63),

  /**
   * An application lock could not be acquired.
   */
  LockNotAcquired(64),

  /**
   * An application lock cound not be released.
   */
  LockNotReleased(65),

  /**
   * An unexpected error has occurred.
   */
  UnexpectedError(66),

  ///#endregion Common

  ///#region Common Mapper

  /**
   * Specified mapping no longer exists.
   */
  MappingDoesNotExist(71),

  /**
   * Could not locate a mapping corresponding to given key.
   */
  MappingNotFoundForKey(72),

  /**
   * Specified mapping is offline.
   */
  MappingIsOffline(73),

  /**
   * Could not terminate connections associated with the Specified mapping.
   */
  MappingsKillConnectionFailure(74),

  /**
   * Specified mapping is not offline which certain management operations warrant.
   */
  MappingIsNotOffline(75),

  /**
   * Specified mapping is locked and the given lock owner id does not match
   * the owner id in the store
   */
  MappingLockOwnerIdDoesNotMatch(76),

  /**
   * Specified mapping has already been locked
   */
  MappingIsAlreadyLocked(77),

  ///#endregion Common Mapper

  ///#region Recovery

  /**
   * Shard does not have storage structures.
   */
  ShardNotValid(81);

  ///#endregion

  public static final int SIZE = java.lang.Integer.SIZE;
  private static java.util.HashMap<Integer, ShardManagementErrorCode> mappings;
  private int intValue;

  private ShardManagementErrorCode(int value) {
    intValue = value;
    getMappings().put(value, this);
  }

  private static java.util.HashMap<Integer, ShardManagementErrorCode> getMappings() {
    if (mappings == null) {
      synchronized (ShardManagementErrorCode.class) {
        if (mappings == null) {
          mappings = new java.util.HashMap<Integer, ShardManagementErrorCode>();
        }
      }
    }
    return mappings;
  }

  public static ShardManagementErrorCode forValue(int value) {
    return getMappings().get(value);
  }

  public int getValue() {
    return intValue;
  }
}
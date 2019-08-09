package com.microsoft.azure.elasticdb.shard.mapmanager;

/*
 * Elastic database tools for Azure SQL Database.
 * 
 * Copyright(c) Microsoft Corporation All rights reserved.
 * 
 * This program is made available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

/**
 * Represents error codes related to <see cref="ShardMapManager"/> operations.
 */
public enum ShardManagementErrorCode {
    /**
     * Successful execution.
     */
    Success,

    /// #region ShardMapManagerFactory

    /**
     * Store already exists on target shard map manager database.
     */
    ShardMapManagerStoreAlreadyExists,

    /**
     * Store does not exist on target shard map manager database.
     */
    ShardMapManagerStoreDoesNotExist,

    /// #endregion

    /// #region ShardMapManager

    /**
     * ShardMap with specified name already exists.
     */
    ShardMapAlreadyExists,

    /**
     * ShardMap with specified name not found.
     */
    ShardMapLookupFailure,

    /**
     * ShardMap has shards associated with it.
     */
    ShardMapHasShards,

    /**
     * GSM store version does not match with client library.
     */
    GlobalStoreVersionMismatch,

    /**
     * LSM store version does not match with client library.
     */
    LocalStoreVersionMismatch,

    /**
     * All necessary parameters for GSM stored procedure are not supplied.
     */
    GlobalStoreOperationInsufficientParameters,

    /**
     * All necessary parameters for LSM stored procedure are not supplied.
     */
    LocalStoreOperationInsufficientParameters,

    /// #endregion

    /// #region ShardMap

    /**
     * Conversion of shard map failed.
     */
    ShardMapTypeConversionError,

    /**
     * Shard has mappings associated with it.
     */
    ShardHasMappings,

    /**
     * Shard already exists.
     */
    ShardAlreadyExists,

    /**
     * Shard location already exists.
     */
    ShardLocationAlreadyExists,

    /**
     * Shard has been updated by concurrent user.
     */
    ShardVersionMismatch,

    /// #endregion

    /// #region PointMapping

    /**
     * Given point is already associated with a mapping.
     */
    MappingPointAlreadyMapped,

    /// #endregion PointMapping

    /// #region RangeMapping

    /**
     * Specified range is already associated with a mapping.
     */
    MappingRangeAlreadyMapped,

    /// #endregion RangeMapping

    /// #region Common

    /**
     * Storage operation failed.
     */
    StorageOperationFailure,

    /**
     * ShardMap does not exist any more.
     */
    ShardMapDoesNotExist,

    /**
     * Shard does not exist any more.
     */
    ShardDoesNotExist,

    /**
     * An application lock could not be acquired.
     */
    LockNotAcquired,

    /**
     * An application lock count not be released.
     */
    LockNotReleased,

    /**
     * An unexpected error has occurred.
     */
    UnexpectedError,

    /// #endregion Common

    /// #region Common Mapper

    /**
     * Specified mapping no longer exists.
     */
    MappingDoesNotExist,

    /**
     * Could not locate a mapping corresponding to given key.
     */
    MappingNotFoundForKey,

    /**
     * Specified mapping is offline.
     */
    MappingIsOffline,

    /**
     * Could not terminate connections associated with the Specified mapping.
     */
    MappingsKillConnectionFailure,

    /**
     * Specified mapping is not offline which certain management operations warrant.
     */
    MappingIsNotOffline,

    /**
     * Specified mapping is locked and the given lock owner id does not match the owner id in the store.
     */
    MappingLockOwnerIdDoesNotMatch,

    /**
     * Specified mapping has already been locked.
     */
    MappingIsAlreadyLocked,

    /// #endregion Common Mapper

    /// #region Recovery

    /**
     * Shard does not have storage structures.
     */
    ShardNotValid;

    /// #endregion

}
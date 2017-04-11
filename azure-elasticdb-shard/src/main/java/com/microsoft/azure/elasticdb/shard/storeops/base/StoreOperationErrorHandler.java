package com.microsoft.azure.elasticdb.shard.storeops.base;

// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

import com.microsoft.azure.elasticdb.shard.base.ShardLocation;
import com.microsoft.azure.elasticdb.shard.mapmanager.ShardManagementErrorCategory;
import com.microsoft.azure.elasticdb.shard.mapmanager.ShardManagementErrorCode;
import com.microsoft.azure.elasticdb.shard.mapmanager.ShardManagementException;
import com.microsoft.azure.elasticdb.shard.schema.SchemaInfoErrorCode;
import com.microsoft.azure.elasticdb.shard.schema.SchemaInfoException;
import com.microsoft.azure.elasticdb.shard.store.IStoreResults;
import com.microsoft.azure.elasticdb.shard.store.StoreShard;
import com.microsoft.azure.elasticdb.shard.store.IStoreShardMap;
import com.microsoft.azure.elasticdb.shard.utils.Errors;
import com.microsoft.azure.elasticdb.shard.utils.GlobalConstants;
import com.microsoft.azure.elasticdb.shard.utils.Version;

/**
 * Utility class for handling SqlOperation errors returned from stored procedures.
 */
public class StoreOperationErrorHandler {
    /**
     * Returns the proper ShardManagementException corresponding to given error code in
     * <paramref name="result"/> for ShardMapManager operations.
     *
     * @param result         Operation result object.
     * @param shardMap       Shard map object.
     * @param operationName  Operation being performed.
     * @param storedProcName Stored procedure being executed.
     * @return ShardManagementException to be raised.
     */
    public static ShardManagementException OnShardMapManagerErrorGlobal(IStoreResults result, IStoreShardMap shardMap, String operationName, String storedProcName) {
        switch (result.getResult()) {
            case ShardMapExists:
                assert shardMap != null;
                return new ShardManagementException(ShardManagementErrorCategory.ShardMapManager, ShardManagementErrorCode.ShardMapAlreadyExists, Errors._Store_ShardMap_AlreadyExistsGlobal, shardMap.getName(), storedProcName, operationName);

            case ShardMapHasShards:
                assert shardMap != null;
                return new ShardManagementException(ShardManagementErrorCategory.ShardMapManager, ShardManagementErrorCode.ShardMapHasShards, Errors._Store_ShardMap_ContainsShardsGlobal, shardMap.getName(), storedProcName, operationName);

            case StoreVersionMismatch:
            case MissingParametersForStoredProcedure:
            default:
                return StoreOperationErrorHandler.OnCommonErrorGlobal(result, operationName, storedProcName);
        }
    }

    /**
     * Returns the proper ShardManagementException corresponding to given error code in
     * <paramref name="result"/> for ShardMap operations.
     *
     * @param result         Operation result object.
     * @param shardMap       Shard map object.
     * @param shard          Shard object.
     * @param errorCategory  Error category to use for raised errors.
     * @param operationName  Operation being performed.
     * @param storedProcName Stored procedure being executed.
     * @return ShardManagementException to be raised.
     */
    public static ShardManagementException OnShardMapErrorGlobal(IStoreResults result, IStoreShardMap shardMap, StoreShard shard, ShardManagementErrorCategory errorCategory, String operationName, String storedProcName) {
        switch (result.getResult()) {
            case ShardMapDoesNotExist:
                return new ShardManagementException(errorCategory, ShardManagementErrorCode.ShardMapDoesNotExist, Errors._Store_ShardMap_DoesNotExistGlobal, shardMap.getName(), storedProcName, operationName, shard != null ? shard.getLocation().toString() : "*");

            case ShardExists:
                return new ShardManagementException(errorCategory, ShardManagementErrorCode.ShardAlreadyExists, Errors._Store_Shard_AlreadyExistsGlobal, shard.getLocation(), shardMap.getName(), storedProcName, operationName);

            case ShardLocationExists:
                return new ShardManagementException(errorCategory, ShardManagementErrorCode.ShardLocationAlreadyExists, Errors._Store_Shard_LocationAlreadyExistsGlobal, shard.getLocation(), shardMap.getName(), storedProcName, operationName);

            case ShardDoesNotExist:
                return new ShardManagementException(errorCategory, ShardManagementErrorCode.ShardDoesNotExist, Errors._Store_Shard_DoesNotExistGlobal, shard.getLocation(), shardMap.getName(), storedProcName, operationName);

            case ShardVersionMismatch:
                return new ShardManagementException(errorCategory, ShardManagementErrorCode.ShardVersionMismatch, Errors._Store_Shard_VersionMismatchGlobal, shard.getLocation(), shardMap.getName(), storedProcName, operationName);

            case ShardHasMappings:
                return new ShardManagementException(errorCategory, ShardManagementErrorCode.ShardHasMappings, Errors._Store_Shard_HasMappingsGlobal, shard.getLocation(), shardMap.getName(), storedProcName, operationName);

            case StoreVersionMismatch:
            case MissingParametersForStoredProcedure:
            default:
                return StoreOperationErrorHandler.OnCommonErrorGlobal(result, operationName, storedProcName);
        }
    }

    /**
     * Returns the proper ShardManagementException corresponding to given error code in
     * <paramref name="result"/> for ShardMap operations.
     *
     * @param result         Operation result object.
     * @param shardMap       Shard map object.
     * @param location       Location of LSM operation.
     * @param errorCategory  Error category to use for raised errors.
     * @param operationName  Operation being performed.
     * @param storedProcName Stored procedure being executed.
     * @return ShardManagementException to be raised.
     */
    public static ShardManagementException OnShardMapErrorLocal(IStoreResults result, IStoreShardMap shardMap, ShardLocation location, ShardManagementErrorCategory errorCategory, String operationName, String storedProcName) {
        switch (result.getResult()) {
            case UnableToKillSessions:
                return new ShardManagementException(errorCategory, ShardManagementErrorCode.MappingsKillConnectionFailure, Errors._Store_ShardMapper_UnableToKillSessions, location, shardMap.getName(), operationName, storedProcName, location);

            case StoreVersionMismatch:
            case MissingParametersForStoredProcedure:
            case ShardDoesNotExist:
                // ShardDoesNotExist on local shard map can only occur in Recovery scenario.
                // For normal UpdateShard operation, we will get this error from GSM operation first.
                return new ShardManagementException(ShardManagementErrorCategory.Recovery, ShardManagementErrorCode.ShardDoesNotExist, Errors._Store_Validate_ShardDoesNotExist, location, shardMap.getName(), operationName, storedProcName);
            default:
                return StoreOperationErrorHandler.OnCommonErrorLocal(result, location, operationName, storedProcName);
        }
    }

    /**
     * Returns the proper ShardManagementException corresponding to given error code in
     * <paramref name="result"/> for ShardMapper operations.
     *
     * @param result         Operation result object.
     * @param shardMap       Shard map object.
     * @param shard          Shard object.
     * @param errorCategory  Error category to use for raised errors.
     * @param operationName  Operation being performed.
     * @param storedProcName Stored procedure being executed.
     * @return ShardManagementException to be raised.
     */
    public static ShardManagementException OnShardMapperErrorGlobal(IStoreResults result, IStoreShardMap shardMap, StoreShard shard, ShardManagementErrorCategory errorCategory, String operationName, String storedProcName) {
        switch (result.getResult()) {
            case ShardMapDoesNotExist:
                return new ShardManagementException(errorCategory, ShardManagementErrorCode.ShardMapDoesNotExist, Errors._Store_ShardMap_DoesNotExistGlobal, shardMap.getName(), storedProcName, operationName, shard != null ? shard.getLocation().toString() : "*");

            case ShardDoesNotExist:
                return new ShardManagementException(errorCategory, ShardManagementErrorCode.ShardDoesNotExist, Errors._Store_Shard_DoesNotExistGlobal, shard.getLocation(), shardMap.getName(), storedProcName, operationName);

            case ShardVersionMismatch:
                return new ShardManagementException(errorCategory, ShardManagementErrorCode.ShardVersionMismatch, Errors._Store_Shard_VersionMismatchGlobal, shard.getLocation(), shardMap.getName(), storedProcName, operationName);

            case MappingDoesNotExist:
                return new ShardManagementException(errorCategory, ShardManagementErrorCode.MappingDoesNotExist, Errors._Store_ShardMapper_MappingDoesNotExistGlobal, shard.getLocation(), shardMap.getName(), storedProcName, operationName);

            case MappingRangeAlreadyMapped:
                return new ShardManagementException(errorCategory, ShardManagementErrorCode.MappingRangeAlreadyMapped, Errors._Store_ShardMapper_MappingPointOrRangeAlreadyMapped, shard.getLocation(), shardMap.getName(), "Range", storedProcName, operationName);

            case MappingPointAlreadyMapped:
                return new ShardManagementException(errorCategory, ShardManagementErrorCode.MappingPointAlreadyMapped, Errors._Store_ShardMapper_MappingPointOrRangeAlreadyMapped, shard.getLocation(), shardMap.getName(), "Point", storedProcName, operationName);

            case MappingNotFoundForKey:
                //Debug.Fail("MappingNotFoundForKey should not be raised during SqlOperation.");
                return new ShardManagementException(errorCategory, ShardManagementErrorCode.MappingNotFoundForKey, Errors._Store_ShardMapper_MappingNotFoundForKeyGlobal, shardMap.getName(), storedProcName, operationName);

            case MappingIsAlreadyLocked:
                return new ShardManagementException(errorCategory, ShardManagementErrorCode.MappingIsAlreadyLocked, Errors._Store_ShardMapper_LockMappingAlreadyLocked, shard.getLocation(), shardMap.getName(), storedProcName, operationName);

            case MappingLockOwnerIdDoesNotMatch:
                return new ShardManagementException(errorCategory, ShardManagementErrorCode.MappingLockOwnerIdDoesNotMatch, Errors._Store_ShardMapper_LockOwnerDoesNotMatch, shard.getLocation(), shardMap.getName(), storedProcName, operationName);

            case MappingIsNotOffline:
                return new ShardManagementException(errorCategory, ShardManagementErrorCode.MappingIsNotOffline, Errors._Store_ShardMapper_MappingIsNotOffline, shard.getLocation(), shardMap.getName(), storedProcName, operationName);

            case StoreVersionMismatch:
            case MissingParametersForStoredProcedure:
            default:
                return StoreOperationErrorHandler.OnCommonErrorGlobal(result, operationName, storedProcName);
        }
    }

    /**
     * Returns the proper ShardManagementException corresponding to given error code in
     * <paramref name="result"/> for ShardMapper operations.
     *
     * @param result         Operation result object.
     * @param location       Location of LSM operation.
     * @param operationName  Operation being performed.
     * @param storedProcName Stored procedure being executed.
     * @return ShardManagementException to be raised.
     */
    public static ShardManagementException OnShardMapperErrorLocal(IStoreResults result, ShardLocation location, String operationName, String storedProcName) {
        switch (result.getResult()) {
            case StoreVersionMismatch:
            case MissingParametersForStoredProcedure:
            default:
                return StoreOperationErrorHandler.OnCommonErrorLocal(result, location, operationName, storedProcName);
        }
    }

    /**
     * Returns the proper ShardManagementException corresponding to given error code in
     * <paramref name="result"/> for ShardMap operations.
     *
     * @param result         Operation result object.
     * @param shardMap       Shard map object.
     * @param location       Location of LSM operation.
     * @param operationName  Operation being performed.
     * @param storedProcName Stored procedure being executed.
     * @return ShardManagementException to be raised.
     */
    public static ShardManagementException OnValidationErrorLocal(IStoreResults result, IStoreShardMap shardMap, ShardLocation location, String operationName, String storedProcName) {
        switch (result.getResult()) {
            case ShardMapDoesNotExist:
                return new ShardManagementException(ShardManagementErrorCategory.Validation, ShardManagementErrorCode.ShardMapDoesNotExist, Errors._Store_Validate_ShardMapDoesNotExist, shardMap.getName(), location, operationName, storedProcName);

            case ShardDoesNotExist:
                return new ShardManagementException(ShardManagementErrorCategory.Validation, ShardManagementErrorCode.ShardDoesNotExist, Errors._Store_Validate_ShardDoesNotExist, location, shardMap.getName(), operationName, storedProcName);

            case ShardVersionMismatch:
                return new ShardManagementException(ShardManagementErrorCategory.Validation, ShardManagementErrorCode.ShardVersionMismatch, Errors._Store_Validate_ShardVersionMismatch, location, shardMap.getName(), operationName, storedProcName);

            case MappingDoesNotExist:
                return new ShardManagementException(ShardManagementErrorCategory.Validation, ShardManagementErrorCode.MappingDoesNotExist, Errors._Store_Validate_MappingDoesNotExist, location, shardMap.getName(), operationName, storedProcName);

            case MappingIsOffline:
                return new ShardManagementException(ShardManagementErrorCategory.Validation, ShardManagementErrorCode.MappingIsOffline, Errors._Store_Validate_MappingIsOffline, location, shardMap.getName(), operationName, storedProcName);

            case StoreVersionMismatch:
            case MissingParametersForStoredProcedure:
            default:
                return StoreOperationErrorHandler.OnCommonErrorLocal(result, location, operationName, storedProcName);
        }
    }

    /**
     * Returns the proper ShardManagementException corresponding to given error code in
     * <paramref name="result"/> for ShardMapper operations.
     *
     * @param result         Operation result object.
     * @param shardMapName   Name of shard map.
     * @param operationName  Operation being performed.
     * @param storedProcName Stored procedure being executed.
     * @return
     */
    public static ShardManagementException OnShardSchemaInfoErrorGlobal(IStoreResults result, String shardMapName, String operationName, String storedProcName) {
        switch (result.getResult()) {
            case SchemaInfoNameConflict:
                throw new SchemaInfoException(SchemaInfoErrorCode.SchemaInfoNameConflict, Errors._Store_SchemaInfo_NameConflict, shardMapName);

            case SchemaInfoNameDoesNotExist:
                throw new SchemaInfoException(SchemaInfoErrorCode.SchemaInfoNameDoesNotExist, Errors._Store_SchemaInfo_NameDoesNotExist, operationName, shardMapName);

            case StoreVersionMismatch:
            case MissingParametersForStoredProcedure:
            default:
                return StoreOperationErrorHandler.OnCommonErrorGlobal(result, operationName, storedProcName);
        }
    }

    /**
     * Returns the proper ShardManagementException corresponding to given error code in
     * <paramref name="result"/> for ShardMap operations.
     *
     * @param result         Operation result object.
     * @param shardMap       Shard map object.
     * @param shard          Shard object.
     * @param errorCategory  Error category to use for raised errors.
     * @param operationName  Operation being performed.
     * @param storedProcName Stored procedure being executed.
     * @return ShardManagementException to be raised.
     */
    public static ShardManagementException OnRecoveryErrorGlobal(IStoreResults result, IStoreShardMap shardMap, StoreShard shard, ShardManagementErrorCategory errorCategory, String operationName, String storedProcName) {
        switch (result.getResult()) {
            case ShardLocationExists:
                return new ShardManagementException(errorCategory, ShardManagementErrorCode.ShardLocationAlreadyExists, Errors._Store_Shard_LocationAlreadyExistsGlobal, shard.getLocation(), shardMap.getName(), storedProcName, operationName);

            case ShardMapExists:
                assert shardMap != null;
                return new ShardManagementException(errorCategory, ShardManagementErrorCode.ShardMapAlreadyExists, Errors._Store_ShardMap_AlreadyExistsGlobal, shardMap.getName(), storedProcName, operationName);

            case StoreVersionMismatch:
            case MissingParametersForStoredProcedure:
            default:
                return StoreOperationErrorHandler.OnCommonErrorGlobal(result, operationName, storedProcName);
        }
    }

    /**
     * Returns the proper ShardManagementException corresponding to given error code in
     * <paramref name="result"/> for ShardMap operations.
     *
     * @param result         Operation result object.
     * @param shardMap       Shard map object.
     * @param location       Location of operation.
     * @param errorCategory  Error category to use for raised errors.
     * @param operationName  Operation being performed.
     * @param storedProcName Stored procedure being executed.
     * @return ShardManagementException to be raised.
     */
    public static ShardManagementException OnRecoveryErrorLocal(IStoreResults result, IStoreShardMap shardMap, ShardLocation location, ShardManagementErrorCategory errorCategory, String operationName, String storedProcName) {
        switch (result.getResult()) {
            case ShardMapDoesNotExist:
                return new ShardManagementException(errorCategory, ShardManagementErrorCode.ShardMapDoesNotExist, Errors._Store_ShardMap_DoesNotExistLocal, shardMap.getName(), location, storedProcName, operationName);

            case StoreVersionMismatch:
            case MissingParametersForStoredProcedure:
            default:
                return StoreOperationErrorHandler.OnCommonErrorLocal(result, location, operationName, storedProcName);
        }
    }

    /**
     * Returns the proper ShardManagementException corresponding to given common error code
     * in <paramref name="result"/>.
     *
     * @param result         Operation result object.
     * @param operationName  Operation being performed.
     * @param storedProcName Stored procedure being executed.
     * @return ShardManagementException to be raised.
     */
    public static ShardManagementException OnCommonErrorGlobal(IStoreResults result, String operationName, String storedProcName) {
        switch (result.getResult()) {
            case StoreVersionMismatch:
                return new ShardManagementException(ShardManagementErrorCategory.Validation, ShardManagementErrorCode.GlobalStoreVersionMismatch, Errors._Store_UnsupportedLibraryVersionGlobal, (result.getStoreVersion() != null) ? result.getStoreVersion().getVersion().toString() : "", GlobalConstants.GsmVersionClient, (result.getStoreVersion() != null) ? (Version.isFirstGreaterThan(result.getStoreVersion().getVersion(), GlobalConstants.GsmVersionClient) ? "library" : "store") : "store");

            case MissingParametersForStoredProcedure:
                return new ShardManagementException(ShardManagementErrorCategory.Validation, ShardManagementErrorCode.GlobalStoreOperationInsufficientParameters, Errors._Store_MissingSprocParametersGlobal, operationName, storedProcName);

            default:
                //Debug.Fail("Unexpected error code found.");
                return new ShardManagementException(ShardManagementErrorCategory.General, ShardManagementErrorCode.UnexpectedError, Errors._Store_UnexpectedErrorGlobal);
        }
    }

    /**
     * Returns the proper ShardManagementException corresponding to given common error code
     * in <paramref name="result"/>.
     *
     * @param result         Operation result object.
     * @param location       Location of LSM.
     * @param operationName  Operation being performed.
     * @param storedProcName Stored procedure being executed.
     * @return ShardManagementException to be raised.
     */
    private static ShardManagementException OnCommonErrorLocal(IStoreResults result, ShardLocation location, String operationName, String storedProcName) {
        switch (result.getResult()) {
            case StoreVersionMismatch:
                return new ShardManagementException(ShardManagementErrorCategory.Validation, ShardManagementErrorCode.LocalStoreVersionMismatch, Errors._Store_UnsupportedLibraryVersionLocal, (result.getStoreVersion() != null) ? result.getStoreVersion().getVersion().toString() : "", location, GlobalConstants.LsmVersionClient, (result.getStoreVersion() != null) ? (Version.isFirstGreaterThan(result.getStoreVersion().getVersion(), GlobalConstants.LsmVersionClient) ? "library" : "store") : "store");

            case MissingParametersForStoredProcedure:
                return new ShardManagementException(ShardManagementErrorCategory.Validation, ShardManagementErrorCode.LocalStoreOperationInsufficientParameters, Errors._Store_MissingSprocParametersLocal, operationName, location, storedProcName);

            default:
                //Debug.Fail("Unexpected error code found.");
                return new ShardManagementException(ShardManagementErrorCategory.General, ShardManagementErrorCode.UnexpectedError, Errors._Store_UnexpectedErrorLocal, location);
        }
    }

    /**
     * Given an operation code, returns the corresponding operation name.
     *
     * @param operationCode Operation code.
     * @return Operation name corresponding to given operation code.
     */
    public static String OperationNameFromStoreOperationCode(StoreOperationCode operationCode) {
        switch (operationCode) {
            case AddShard:
                return "CreateShard";
            case RemoveShard:
                return "DeleteShard";
            case UpdateShard:
                return "UpdateShard";
            case AddPointMapping:
                return "AddPointMapping";
            case RemovePointMapping:
                return "RemovePointMapping";
            case UpdatePointMapping:
                return "UpdatePointMapping";
            case UpdatePointMappingWithOffline:
                return "UpdatePointMappingMarkOffline";
            case AddRangeMapping:
                return "AddRangeMapping";
            case RemoveRangeMapping:
                return "RemoveRangeMapping";
            case UpdateRangeMapping:
                return "UpdateRangeMapping";
            case UpdateRangeMappingWithOffline:
                return "UpdateRangeMappingMarkOffline";
            case SplitMapping:
                return "SplitMapping";
            case MergeMappings:
                return "MergeMappings";
            case AttachShard:
                return "AttachShard";
            default:
                //Debug.Fail("Unexpected operation code found.");
                return "";
        }
    }
}
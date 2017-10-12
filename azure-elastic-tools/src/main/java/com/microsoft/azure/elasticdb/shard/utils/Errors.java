package com.microsoft.azure.elasticdb.shard.utils;

/*
 * Elastic database tools for Azure SQL Database.
 * 
 * Copyright(c) Microsoft Corporation All rights reserved.
 * 
 * This program is made available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

public class Errors {

    public static String _General_InvalidArgumentValue = "Unsupported value '%1$s' specified for" + " parameter '%2$s'.";
    public static String _Recovery_InvalidRebuildShardSpecification = "The specified ShardRange %1$s"
            + " was not in the set of ShardRanges from which the local shardMap information can be" + " rebuilt at location '%2$s'.";
    public static String _Recovery_InvalidRecoveryToken = "Recovery token %1$s was not recognized.";
    public static String _Recovery_ShardNotValid = "The given location '%1$s' does not have the"
            + " necessary storage structures present for a shard. Error occurred while performing" + " operation '%2$s'.";
    public static String _SchemaInfo_TableInfoAlreadyExists = "The SchemaInfo object already contains"
            + " a %1$s table by the given name of [%2$s].[%3$s].";
    public static String _Shard_DifferentShardMap = "Shard '%1$s' provided for the given '%2$s' is"
            + " not associated with current shard map '%3$s'. The '%4$s' operations required the shard"
            + " to be associated with the current shard map.";
    public static String _Shard_DifferentShardMapManager = "Shard '%1$s' provided for the given"
            + " '%2$s' is not associated with current shard map manager at '%3$s'. The '%4$s' operations"
            + " required the shard to be associated with the current shard map manager.";
    public static String _ShardKey_MaxValueCannotBeIncremented = "Shard key has maximum value which"
            + " cannot be incremented. Perform the IsMax check on shard key before requesting next key.";
    public static String _ShardKey_MaxValueCannotBeRepresented = "Shard key has maximum value which"
            + " cannot be represented. Perform the IsMax check on shard key before requesting a" + " conversion.";
    public static String _ShardKey_RequestedTypeDoesNotMatchShardKeyType = "The requested type '%1$s'" + " does not match the shard key type '%2$s'.";
    public static String _ShardKey_ShardKeyTypesMustMatchForComparison = "The data type of the shard"
            + " key '%1$s' does not match the data type of comparand shard key '%2$s'. Comparison is only"
            + " supported for shard keys with same type.";
    public static String _ShardKey_UnsupportedShardKeyType = "The specified ShardKeyType specified is" + " not supported. ";
    public static String _ShardKey_UnsupportedType = "Type not supported for shard keys.";
    public static String _ShardKey_UnsupportedValue = "The given value is of type '%1$s' which is an" + " unsupported shard key type.";
    public static String _ShardKey_ValueDoesNotMatchShardKeyType = "The type of specified value does"
            + " not match the type specified in the '%1$s' parameter.";
    public static String _ShardKey_ValueLengthUnexpected = "The length of raw value specified for"
            + " the shard key (%1$s bytes) does not match the expected length (%2$s bytes) for the" + " ShardKeyType (%3$s) specified.";
    public static String _ShardLocation_InvalidPort = "The given port number '%1$s' is invalid.";
    public static String _ShardLocation_InvalidServerOrDatabase = "The given '%1$s' name is longer"
            + " than the allowed length of '%2$s' characters.";
    public static String _ShardLocation_UnsupportedProtocol = "The given protocol value '%1$s' is not" + " supported.";
    public static String _ShardMap_GetShard_ShardDoesNotExist = "Shard corresponding to location"
            + " '%1$s' could not be found in shard map '%2$s' .";
    public static String _ShardMap_OpenConnection_ConnectionStringPropertyDisallowed = "Property"
            + " '%1$s' must not be set in the input connection string for 'OpenConnection' operations.";
    public static String _ShardMap_OpenConnectionForKey_KeyTypeNotSupported = "Key values of type"
            + " '%1$s' are not supported. OpenConnectionForKey requests for the shard map '%2$s'" + " requires keys of type '%3$s'.";
    public static String _ShardMapExtensions_AsTypedShardMap_ConversionFailure = "Shard map '%1$s'"
            + " can not be converted to the target type %2$sShardMap<%3$s>. Its actual type is" + " %4$sShardMap<%5$s>.";
    public static String _ShardMapManager_DifferentShardMapManager = "Shard map '%1$s' is not"
            + " associated with the current instance of shard map manager at '%2$s'. Only shard maps"
            + " that are obtained from the same 'ShardMapManager' instance can be used.";
    public static String _ShardMapManager_ShardMapLookupFailed = "Shard map '%1$s' could not be" + " found in the shard map manager store at '%2$s'.";
    public static String _ShardMapManager_UnsupportedShardMapName = "Shard map name '%1$s' is not"
            + " supported. Shard map names are only allowed to contain unicode letters and digits.";
    public static String _ShardMapManager_UnsupportedShardMapNameLength = "Length of shard map name"
            + " '%1$s' exceeds the maximum allowed length of '%2$s' characters.";
    public static String _ShardMapping_DifferentShardMap = "'%1$s' provided for operation '%2$s' is"
            + " not associated with the current shard map '%3$s'. Only mappings associated with the same"
            + " shard map as the given shard map can be used. Error occurred for parameter '%4$s'.";
    public static String _ShardMapping_DifferentShardMapManager = "'%1$s' provided for operation"
            + " '%2$s' is not associated with the current instance of shard map manager at '%3$s'. Only"
            + " mappings associated with the same 'ShardMapManager' instance as the the one associated"
            + " with current shard map '%4$s' can be used. Error occurred for parameter '%5$s'.";
    public static String _ShardMapping_DifferentStatus = "Mappings provided for the Merge operation"
            + " belonging to shard map '%1$s' differ in their Status property. Merge operation is only"
            + " allowed for mappings with same values for Status.";
    public static String _ShardMapping_LockIdNotSupported = "LockId for locking the mapping"
            + " referencing shard %1$s with shard map name %2$s has unsupported value '%3$s'.";
    public static String _ShardMapping_MergeDifferentShards = "Mappings provided for the merge"
            + " operation which belong to shard map '%1$s' belong to different shards '%2$s' and '%3$s'."
            + " Merge operation is only allowed for mappings belonging to the same shard.";
    public static String _ShardMapping_MergeNotAdjacent = "Mappings provided for the merge operation"
            + " are not adjacent. Merge operation is only allowed for adjacent mappings.";
    public static String _ShardMapping_RangeNotProperSubset = "Requested range is exactly the range"
            + " for existing mapping. Operation is only allowed for proper subsets of existing range.";
    public static String _ShardMapping_RangeNotSubset = "Requested range is not a subset of the" + " existing range mapping.";
    public static String _ShardMapping_SplitPointOutOfRange = "Split point lies on the boundary or" + " outside of the specified range mapping.";
    public static String _ShardRange_LowGreaterThanOrEqualToHigh = "The low value %s is greater than"
            + " or equal to the high value %s. Lower value must be less than the higher value.";
    public static String _SqlShardMapManagerCredentials_ConnectionStringPropertyRequired = "The"
            + " required property '%1$s' must be set in the connection string.";
    public static String _Store_MissingSprocParametersGlobal = "All required parameters for operation"
            + " '%1$s' are not supplied. Error occurred while executing procedure '%2$s' on the shard"
            + " map manager database. Please verify and match library and store version.";
    public static String _Store_MissingSprocParametersLocal = "All required parameters for operation"
            + " '%1$s' at shard '%2$s' are not supplied. Error occurred while executing procedure '%3$s'"
            + " on the shard. Please verify and match library and store version.";
    public static String _Store_SchemaInfo_NameConflict = "Unable to create schema info with name"
            + " '%1$s' as there is already an entry with the same name.";
    public static String _Store_SchemaInfo_NameDoesNotExist = "Unable to %1$s schema info with name"
            + " '%2$s' since there is no entry by the given name.";
    public static String _Store_Shard_AlreadyExistsGlobal = "Shard '%1$s' already exists in store"
            + " for shard map '%2$s'. Error occurred while executing stored procedure '%3$s' for"
            + " operation '%4$s'. This can happen when another concurrent user adds the shard to the" + " store.";
    public static String _Store_Shard_DoesNotExistGlobal = "Shard '%1$s' belonging to shard map"
            + " '%2$s' could not be found in the shard map manager database. Error occurred while"
            + " executing stored procedure '%3$s' for operation '%4$s'. This can happen when another"
            + " concurrent user removes the shard from the store.";
    public static String _Store_Shard_HasMappingsGlobal = "Shard '%1$s' belonging to shard map"
            + " '%2$s' has mappings associated with it. Error occurred while executing stored procedure"
            + " '%3$s' for operation '%4$s'. Remove all the mappings associated with the shard before" + " attempting the operation.";
    public static String _Store_Shard_LocationAlreadyExistsGlobal = "Shard referencing location"
            + " '%1$s' already exists in store for shard map '%2$s'. Error occurred while executing"
            + " stored procedure '%3$s' for operation '%4$s'. This can happen when another concurrent"
            + " user has added a shard with specified location in store.";
    public static String _Store_Shard_VersionMismatchGlobal = "Shard '%1$s' belonging to shard map"
            + " '%2$s' has been updated in store. Error occurred while executing stored procedure '%3$s'"
            + " for operation '%4$s'. This can occur if another concurrent user updates the shard."
            + " Perform a GetShard operation for the shard location to obtain the updated instance.";
    public static String _Store_ShardMap_AlreadyExistsGlobal = "Shard map with name '%1$s' already"
            + " exists in the store. Error occurred while executing stored procedure '%2$s' for" + " operation '%3$s'.";
    public static String _Store_ShardMap_ContainsShardsGlobal = "Shard map '%1$s' has shards"
            + " associated with it. Shard maps can only be removed if there are no shards associated"
            + " with them. Error occurred while executing stored procedure '%2$s' for operation '%3$s'.";
    public static String _Store_ShardMap_DoesNotExistGlobal = "Shard map '%1$s' does not exist in"
            + " the store. Error occurred while executing stored procedure '%2$s' for operation '%3$s'"
            + " for shard '%4$s'. This can happen if another concurrent user deletes the shard map.";
    public static String _Store_ShardMap_DoesNotExistLocal = "Shard map '%1$s' does not exist on"
            + " shard '%2$s'. Error occurred while executing stored procedure '%3$s' for operation"
            + " '%4$s'. This can happen if another concurrent user deletes the shard map.";
    public static String _Store_ShardMapManager_AlreadyExistsGlobal = "Data structures for shard map"
            + " manager persistence already exists at the target location.";
    public static String _Store_ShardMapManager_DoesNotExistGlobal = "Data structures for shard map"
            + " manager persistence do not exist at the target location.";
    public static String _Store_ShardMapper_LockMappingAlreadyLocked = "Mapping referencing shard"
            + " '%1$s' belonging to shard map '%2$s' is already locked. Error occurred while executing"
            + " procedure '%3$s' for operation '%4$s' on the shard map manager database. This can happen"
            + " if another concurrent user locks the mapping.";
    public static String _Store_ShardMapper_LockOwnerDoesNotMatch = "Mapping referencing shard '%1$s'"
            + " belonging to shard map '%2$s' is locked and correct lock token is not provided. Error"
            + " occurred while executing procedure '%3$s' for operation '%4$s' on the shard map manager"
            + " database. This can happen if another concurrent user locks the mapping.";
    public static String _Store_ShardMapper_MappingDoesNotExistGlobal = "Mapping referencing shard"
            + " '%1$s' in the shard map '%2$s' does not exist. Error occurred while executing stored"
            + " procedure '%3$s' for operation '%4$s'. This can occur if another concurrent user has" + " already removed the mapping.";
    public static String _Store_ShardMapper_MappingIsNotOffline = "Mapping referencing shard '%1$s'"
            + " in the shard map '%2$s' has 'Online' status.  Error occurred while executing stored"
            + " procedure '%3$s' for operation '%4$s'. Updates to a mapping involving modification of the"
            + " shard location or removal require the mapping to be 'Offline'. ";
    public static String _Store_ShardMapper_MappingNotFoundForKeyGlobal = "Mapping containing the"
            + " given key value could not be located in the shard map '%1$s'. Error occurred while"
            + " executing stored procedure '%2$s' for operation '%3$s'.";
    public static String _Store_ShardMapper_MappingPointOrRangeAlreadyMapped = "Mapping referencing"
            + " shard '%1$s' in the shard map '%2$s' cannot be added because the %3$s it covers is"
            + " already mapped by another mapping. Error occurred while executing stored procedure"
            + " '%4$s' for operation '%5$s'. This can occur if another concurrent user has already added" + " a mapping covering the given %3$s.";
    public static String _Store_ShardMapper_UnableToKillSessions = "Mapping referencing shard '%1$s'"
            + " belonging to shard map '%2$s' could not be taken offline because all existing connections"
            + " on the shard could not be terminated. Error occurred during '%3$s' operation while"
            + " executing stored procedure '%4$s' on shard '%5$s'.";
    public static String _Store_SqlExceptionGlobal = "Store Error: %1$s. The error occurred while"
            + " attempting to perform the underlying storage operation during '%2$s' operation on the"
            + " shard map manager database. See the inner StoreException for details.";
    public static String _Store_SqlExceptionLocal = "Store Error: %1$s. The error occurred while"
            + " attempting to perform the underlying storage operation during '%2$s' operation on shard"
            + " '%3$s'. See the inner StoreException for details.";
    public static String _Store_SqlOperation_LockNotAcquired = "Exclusive access to application" + " resource '%1$s' could not be acquired.";
    public static String _Store_SqlOperation_LockNotReleased = "Exclusive access to application" + " resource '%1$s' could not be released.";
    public static String _Store_StoreException = "Error occurred while performing store operation." + " See the inner SqlException for details.";
    public static String _Store_UnexpectedErrorGlobal = "Unexpected error code found while processing"
            + " errors returned from the shard map manager store. This can occur because of a defect in" + " the client library.";
    public static String _Store_UnexpectedErrorLocal = "Unexpected error code found while processing"
            + " errors returned from the shard location '%1$s'. This can occur because of a defect in the" + " client library.";
    public static String _Store_UnsupportedLibraryVersionGlobal = "Shard map manager store version"
            + " '%1$s' is not compatible with the version '%2$s' supported by client library. Please" + " upgrade the %3$s.";
    public static String _Store_UnsupportedLibraryVersionLocal = "Shard map manager store version"
            + " '%1$s' at shard '%2$s' is not compatible with the version '%3$s' supported by client" + " library. Please upgrade the %4$s.";
    public static String _Store_Validate_MappingDoesNotExist = "Mapping referencing shard '%1$s'"
            + " associated with shard map '%2$s' no longer exists in store. Error occurred while"
            + " executing stored procedure '%3$s' for operation '%4$s' on the shard. This can happen"
            + " if another concurrent user has deleted or modified the mapping.";
    public static String _Store_Validate_MappingIsOffline = "The shard key value for an"
            + " OpenConnection request is associated with a mapping that is marked ‘Offline’.  Data"
            + " containing this key value is likely being moved, and the connection is blocked to avoid"
            + " data corruption.  Validated connection requests for this shardlet will succeed when the"
            + " mapping is back ‘Online’.  Shard: '%1$s'.  Shard map: '%2$s'.  Error occurred while"
            + " executing stored procedure '%4$s' for operation '%3$s' on the shard.";
    public static String _Store_Validate_ShardDoesNotExist = "Shard '%1$s' belonging to shard map"
            + " '%2$s' no longer exists in store. Error occurred while executing stored procedure '%3$s'"
            + " for operation '%4$s' on the shard. This can happen if another concurrent user deletes" + " the shard. ";
    public static String _Store_Validate_ShardMapDoesNotExist = "Shard map '%1$s' associated with"
            + " shard '%2$s' no longer exists in store. Error occurred while executing stored procedure"
            + " '%3$s' for operation '%4$s'. This can happen if another concurrent user deletes" + " the shard map.";
    public static String _Store_Validate_ShardVersionMismatch = "Shard '%1$s' belonging to shard map"
            + " '%2$s' has been modified in the store. Error occurred while executing stored procedure"
            + " '%3$s' for operation '%4$s' on the shard. This can happen if another concurrent user"
            + " performs modification operations on the shard or its associated mappings.";
}

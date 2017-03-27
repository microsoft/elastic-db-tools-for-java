package com.microsoft.azure.elasticdb.shard.storeops.base;

// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

/**
 * Constructs requests for store operations.
 */
public final class StoreOperationRequestBuilder {

    ///#region GSM Stored Procedures

    /**
     * FindAndUpdateOperationLogEntryByIdGlobal stored procedure.
     */
    public static final String SpFindAndUpdateOperationLogEntryByIdGlobal = "__ShardManagement.spFindAndUpdateOperationLogEntryByIdGlobal";

    /**
     * GetAllShardMapsGlobal stored procedure.
     */
    public static final String SpGetAllShardMapsGlobal = "__ShardManagement.spGetAllShardMapsGlobal";

    /**
     * FindShardMapByNameGlobal stored procedure.
     */
    public static final String SpFindShardMapByNameGlobal = "__ShardManagement.spFindShardMapByNameGlobal";

    /**
     * GetAllDistinctShardLocationsGlobal stored procedure.
     */
    public static final String SpGetAllDistinctShardLocationsGlobal = "__ShardManagement.spGetAllDistinctShardLocationsGlobal";

    /**
     * AddShardMapGlobal stored procedure.
     */
    public static final String SpAddShardMapGlobal = "__ShardManagement.spAddShardMapGlobal";

    /**
     * RemoveShardMapGlobal stored procedure.
     */
    public static final String SpRemoveShardMapGlobal = "__ShardManagement.spRemoveShardMapGlobal";

    /**
     * GetAllShardsGlobal stored procedure.
     */
    public static final String SpGetAllShardsGlobal = "__ShardManagement.spGetAllShardsGlobal";

    /**
     * FindShardByLocationGlobal stored procedure.
     */
    public static final String SpFindShardByLocationGlobal = "__ShardManagement.spFindShardByLocationGlobal";

    /**
     * BulkOperationShardsGlobalBegin stored procedure.
     */
    public static final String SpBulkOperationShardsGlobalBegin = "__ShardManagement.spBulkOperationShardsGlobalBegin";

    /**
     * BulkOperationShardsGlobalEnd stored procedure.
     */
    public static final String SpBulkOperationShardsGlobalEnd = "__ShardManagement.spBulkOperationShardsGlobalEnd";

    /**
     * GetAllShardMappingsGlobal stored procedure.
     */
    public static final String SpGetAllShardMappingsGlobal = "__ShardManagement.spGetAllShardMappingsGlobal";

    /**
     * FindShardMappingByKeyGlobal stored procedure.
     */
    public static final String SpFindShardMappingByKeyGlobal = "__ShardManagement.spFindShardMappingByKeyGlobal";

    /**
     * FindShardMappingByIdGlobal stored procedure.
     */
    public static final String SpFindShardMappingByIdGlobal = "__ShardManagement.spFindShardMappingByIdGlobal";

    /**
     * BulkShardMappingOperationsGlobalBegin stored procedure.
     */
    public static final String SpBulkOperationShardMappingsGlobalBegin = "__ShardManagement.spBulkOperationShardMappingsGlobalBegin";

    /**
     * BulkShardMappingOperationsGlobalEnd stored procedure.
     */
    public static final String SpBulkOperationShardMappingsGlobalEnd = "__ShardManagement.spBulkOperationShardMappingsGlobalEnd";

    /**
     * LockOrUnLockShardMappingsGlobal stored procedure.
     */
    public static final String SpLockOrUnLockShardMappingsGlobal = "__ShardManagement.spLockOrUnlockShardMappingsGlobal";

    /**
     * GetAllShardingSchemaInfosGlobal stored procedure.
     */
    public static final String SpGetAllShardingSchemaInfosGlobal = "__ShardManagement.spGetAllShardingSchemaInfosGlobal";

    /**
     * FindShardingSchemaInfoByNameGlobal stored procedure.
     */
    public static final String SpFindShardingSchemaInfoByNameGlobal = "__ShardManagement.spFindShardingSchemaInfoByNameGlobal";

    /**
     * AddShardingSchemaInfoGlobal stored procedure.
     */
    public static final String SpAddShardingSchemaInfoGlobal = "__ShardManagement.spAddShardingSchemaInfoGlobal";

    /**
     * RemoveShardingSchemaInfoGlobal stored procedure.
     */
    public static final String SpRemoveShardingSchemaInfoGlobal = "__ShardManagement.spRemoveShardingSchemaInfoGlobal";

    /**
     * UpdateShardingSchemaInfoGlobal stored procedure.
     */
    public static final String SpUpdateShardingSchemaInfoGlobal = "__ShardManagement.spUpdateShardingSchemaInfoGlobal";

    /**
     * AttachShardGlobal stored procedure.
     */
    public static final String SpAttachShardGlobal = "__ShardManagement.spAttachShardGlobal";

    /**
     * DetachShardGlobal stored procedure.
     */
    public static final String SpDetachShardGlobal = "__ShardManagement.spDetachShardGlobal";

    /**
     * ReplaceShardMappingsGlobal stored procedure.
     */
    public static final String SpReplaceShardMappingsGlobal = "__ShardManagement.spReplaceShardMappingsGlobal";


    ///#endregion GSM Stored Procedures

    ///#region LSM Stored Procedures

    /**
     * GetAllShardsLocal stored procedure.
     */
    public static final String SpGetAllShardsLocal = "__ShardManagement.spGetAllShardsLocal";

    /**
     * ValidateShardLocal stored procedure.
     */
    public static final String SpValidateShardLocal = "__ShardManagement.spValidateShardLocal";

    /**
     * AddShardLocal stored procedure.
     */
    public static final String SpAddShardLocal = "__ShardManagement.spAddShardLocal";

    /**
     * RemoveShardLocal stored procedure.
     */
    public static final String SpRemoveShardLocal = "__ShardManagement.spRemoveShardLocal";

    /**
     * UpdateShardLocal stored procedure.
     */
    public static final String SpUpdateShardLocal = "__ShardManagement.spUpdateShardLocal";

    /**
     * GetAllShardMappingsLocal stored procedure.
     */
    public static final String SpGetAllShardMappingsLocal = "__ShardManagement.spGetAllShardMappingsLocal";

    /**
     * FindShardMappingByKeyLocal stored procedure.
     */
    public static final String SpFindShardMappingByKeyLocal = "__ShardManagement.spFindShardMappingByKeyLocal";

    /**
     * ValidateShardMappingLocal stored procedure.
     */
    public static final String SpValidateShardMappingLocal = "__ShardManagement.spValidateShardMappingLocal";

    /**
     * BulkOperationShardMappingsLocal stored procedure.
     */
    public static final String SpBulkOperationShardMappingsLocal = "__ShardManagement.spBulkOperationShardMappingsLocal";

    /**
     * KillSessionsForShardMappingLocal stored procedure.
     */
    public static final String SpKillSessionsForShardMappingLocal = "__ShardManagement.spKillSessionsForShardMappingLocal";

    ///#endregion LSM Stored Procedures
}
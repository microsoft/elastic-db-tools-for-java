-- Copyright (c) Microsoft. All rights reserved.
-- Licensed under the MIT license. See LICENSE file in the project root for full license information.

---------------------------------------------------------------------------------------------------
-- Stored Procedures
---------------------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------------------
-- Recovery
---------------------------------------------------------------------------------------------------
IF object_id(N'__ShardManagement.spReplaceShardMappingsGlobal', N'P') IS NOT NULL
  BEGIN
    DROP PROCEDURE __ShardManagement.spReplaceShardMappingsGlobal
  END
GO

IF object_id(N'__ShardManagement.spDetachShardGlobal', N'P') IS NOT NULL
  BEGIN
    DROP PROCEDURE __ShardManagement.spDetachShardGlobal
  END
GO

IF object_id(N'__ShardManagement.spAttachShardGlobal', N'P') IS NOT NULL
  BEGIN
    DROP PROCEDURE __ShardManagement.spAttachShardGlobal
  END
GO

---------------------------------------------------------------------------------------------------
-- Sharding Schema Infos
---------------------------------------------------------------------------------------------------
IF (object_id('__ShardManagement.spUpdateShardingSchemaInfoGlobal', N'P') IS NOT NULL)
  BEGIN
    DROP PROCEDURE __ShardManagement.spUpdateShardingSchemaInfoGlobal
  END
GO

IF (object_id('__ShardManagement.spRemoveShardingSchemaInfoGlobal', N'P') IS NOT NULL)
  BEGIN
    DROP PROCEDURE __ShardManagement.spRemoveShardingSchemaInfoGlobal
  END
GO

IF (object_id('__ShardManagement.spAddShardingSchemaInfoGlobal', N'P') IS NOT NULL)
  BEGIN
    DROP PROCEDURE __ShardManagement.spAddShardingSchemaInfoGlobal
  END
GO

IF (object_id('__ShardManagement.spFindShardingSchemaInfoByNameGlobal', N'P') IS NOT NULL)
  BEGIN
    DROP PROCEDURE __ShardManagement.spFindShardingSchemaInfoByNameGlobal
  END
GO

IF (object_id('__ShardManagement.spGetAllShardingSchemaInfosGlobal', N'P') IS NOT NULL)
  BEGIN
    DROP PROCEDURE __ShardManagement.spGetAllShardingSchemaInfosGlobal
  END
GO

---------------------------------------------------------------------------------------------------
-- Shard Mappings
---------------------------------------------------------------------------------------------------
IF object_id(N'__ShardManagement.spLockOrUnlockShardMappingsGlobal', N'P') IS NOT NULL
  BEGIN
    DROP PROCEDURE __ShardManagement.spLockOrUnlockShardMappingsGlobal
  END
GO

IF object_id(N'__ShardManagement.spBulkOperationShardMappingsGlobalEnd', N'P') IS NOT NULL
  BEGIN
    DROP PROCEDURE __ShardManagement.spBulkOperationShardMappingsGlobalEnd
  END
GO

IF object_id(N'__ShardManagement.spBulkOperationShardMappingsGlobalBegin', N'P') IS NOT NULL
  BEGIN
    DROP PROCEDURE __ShardManagement.spBulkOperationShardMappingsGlobalBegin
  END
GO

IF object_id(N'__ShardManagement.spFindShardMappingByIdGlobal', N'P') IS NOT NULL
  BEGIN
    DROP PROCEDURE __ShardManagement.spFindShardMappingByIdGlobal
  END
GO

IF object_id(N'__ShardManagement.spFindShardMappingByKeyGlobal', N'P') IS NOT NULL
  BEGIN
    DROP PROCEDURE __ShardManagement.spFindShardMappingByKeyGlobal
  END
GO

IF object_id(N'__ShardManagement.spGetAllShardMappingsGlobal', N'P') IS NOT NULL
  BEGIN
    DROP PROCEDURE __ShardManagement.spGetAllShardMappingsGlobal
  END
GO

---------------------------------------------------------------------------------------------------
-- Shards
---------------------------------------------------------------------------------------------------
IF object_id(N'__ShardManagement.spBulkOperationShardsGlobalEnd', N'P') IS NOT NULL
  BEGIN
    DROP PROCEDURE __ShardManagement.spBulkOperationShardsGlobalEnd
  END
GO

IF object_id(N'__ShardManagement.spBulkOperationShardsGlobalBegin', N'P') IS NOT NULL
  BEGIN
    DROP PROCEDURE __ShardManagement.spBulkOperationShardsGlobalBegin
  END
GO

IF object_id(N'__ShardManagement.spFindShardByLocationGlobal', N'P') IS NOT NULL
  BEGIN
    DROP PROCEDURE __ShardManagement.spFindShardByLocationGlobal
  END
GO

IF object_id(N'__ShardManagement.spGetAllShardsGlobal', N'P') IS NOT NULL
  BEGIN
    DROP PROCEDURE __ShardManagement.spGetAllShardsGlobal
  END
GO

---------------------------------------------------------------------------------------------------
-- Shard Maps
---------------------------------------------------------------------------------------------------
IF object_id(N'__ShardManagement.spRemoveShardMapGlobal', N'P') IS NOT NULL
  BEGIN
    DROP PROCEDURE __ShardManagement.spRemoveShardMapGlobal
  END
GO

IF object_id(N'__ShardManagement.spAddShardMapGlobal', N'P') IS NOT NULL
  BEGIN
    DROP PROCEDURE __ShardManagement.spAddShardMapGlobal
  END
GO

IF object_id(N'__ShardManagement.spGetAllDistinctShardLocationsGlobal', N'P') IS NOT NULL
  BEGIN
    DROP PROCEDURE __ShardManagement.spGetAllDistinctShardLocationsGlobal
  END
GO

IF object_id(N'__ShardManagement.spFindShardMapByNameGlobal', N'P') IS NOT NULL
  BEGIN
    DROP PROCEDURE __ShardManagement.spFindShardMapByNameGlobal
  END
GO

IF object_id(N'__ShardManagement.spGetAllShardMapsGlobal', N'P') IS NOT NULL
  BEGIN
    DROP PROCEDURE __ShardManagement.spGetAllShardMapsGlobal
  END
GO

---------------------------------------------------------------------------------------------------
-- Operations
---------------------------------------------------------------------------------------------------
IF object_id(N'__ShardManagement.spFindAndUpdateOperationLogEntryByIdGlobal', N'P') IS NOT NULL
  BEGIN
    DROP PROCEDURE __ShardManagement.spFindAndUpdateOperationLogEntryByIdGlobal
  END
GO

---------------------------------------------------------------------------------------------------
-- Helper SPs and Functions
---------------------------------------------------------------------------------------------------
IF object_id(N'__ShardManagement.spGetOperationLogEntryGlobalHelper', N'P') IS NOT NULL
  BEGIN
    DROP PROCEDURE __ShardManagement.spGetOperationLogEntryGlobalHelper
  END
GO

IF object_id(N'__ShardManagement.spGetStoreVersionGlobalHelper', N'P') IS NOT NULL
  BEGIN
    DROP PROCEDURE __ShardManagement.spGetStoreVersionGlobalHelper
  END
GO

IF object_id(N'__ShardManagement.fnGetStoreVersionGlobal', N'FN') IS NOT NULL
  BEGIN
    DROP FUNCTION __ShardManagement.fnGetStoreVersionGlobal
  END
GO

IF object_id(N'__ShardManagement.fnGetStoreVersionMajorGlobal', N'FN') IS NOT NULL
  BEGIN
    DROP FUNCTION __ShardManagement.fnGetStoreVersionMajorGlobal
  END
GO

---------------------------------------------------------------------------------------------------
-- Constraints
---------------------------------------------------------------------------------------------------
IF object_id(N'__ShardManagement.fkShardMappingsGlobal_ShardId', N'F') IS NOT NULL
  BEGIN
    ALTER TABLE __ShardManagement.ShardMappingsGlobal
      DROP CONSTRAINT fkShardMappingsGlobal_ShardId
  END
GO

IF object_id(N'__ShardManagement.fkShardMappingsGlobal_ShardMapId', N'F') IS NOT NULL
  BEGIN
    ALTER TABLE __ShardManagement.ShardMappingsGlobal
      DROP CONSTRAINT fkShardMappingsGlobal_ShardMapId
  END
GO

IF object_id(N'__ShardManagement.fkShardsGlobal_ShardMapId ', N'F') IS NOT NULL
  BEGIN
    ALTER TABLE __ShardManagement.ShardsGlobal
      DROP CONSTRAINT fkShardsGlobal_ShardMapId
  END
GO

IF object_id(N'__ShardManagement.ucShardMappingsGlobal_MappingId', N'UQ') IS NOT NULL
  BEGIN
    ALTER TABLE __ShardManagement.ShardMappingsGlobal
      DROP CONSTRAINT ucShardMappingsGlobal_MappingId
  END
GO

IF object_id(N'__ShardManagement.ucShardsGlobal_Location', N'UQ') IS NOT NULL
  BEGIN
    ALTER TABLE __ShardManagement.ShardsGlobal
      DROP CONSTRAINT ucShardsGlobal_Location
  END
GO

IF object_id(N'__ShardManagement.ucShardMapsGlobal_Name', N'UQ') IS NOT NULL
  BEGIN
    ALTER TABLE __ShardManagement.ShardMapsGlobal
      DROP CONSTRAINT ucShardMapsGlobal_Name
  END
GO

---------------------------------------------------------------------------------------------------
-- Tables
---------------------------------------------------------------------------------------------------
IF (object_id('__ShardManagement.ShardedDatabaseSchemaInfosGlobal', N'U') IS NOT NULL)
  BEGIN
    DROP TABLE __ShardManagement.ShardedDatabaseSchemaInfosGlobal
  END
GO

IF object_id(N'__ShardManagement.OperationsLogGlobal', N'U') IS NOT NULL
  BEGIN
    DROP TABLE __ShardManagement.OperationsLogGlobal
  END
GO

IF object_id(N'__ShardManagement.ShardMappingsGlobal', N'U') IS NOT NULL
  BEGIN
    DROP TABLE __ShardManagement.ShardMappingsGlobal
  END
GO

IF object_id(N'__ShardManagement.ShardsGlobal', N'U') IS NOT NULL
  BEGIN
    DROP TABLE __ShardManagement.ShardsGlobal
  END
GO

IF object_id(N'__ShardManagement.ShardMapsGlobal', N'U') IS NOT NULL
  BEGIN
    DROP TABLE __ShardManagement.ShardMapsGlobal
  END
GO

IF object_id(N'__ShardManagement.ShardMapManagerGlobal', N'U') IS NOT NULL
  BEGIN
    DROP TABLE __ShardManagement.ShardMapManagerGlobal
  END
GO

---------------------------------------------------------------------------------------------------
-- Schema
---------------------------------------------------------------------------------------------------
IF schema_id('__ShardManagement') IS NOT NULL
  BEGIN
    DROP SCHEMA __ShardManagement
  END
GO

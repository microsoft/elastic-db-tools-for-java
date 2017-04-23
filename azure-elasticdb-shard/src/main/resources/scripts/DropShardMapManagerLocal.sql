-- Copyright (c) Microsoft. All rights reserved.
-- Licensed under the MIT license. See LICENSE file in the project root for full license information.

---------------------------------------------------------------------------------------------------
-- Stored Procedures
---------------------------------------------------------------------------------------------------

---------------------------------------------------------------------------------------------------
-- Shard Mappings
---------------------------------------------------------------------------------------------------
IF object_id(N'__ShardManagement.spKillSessionsForShardMappingLocal', N'P') IS NOT NULL
  BEGIN
    DROP PROCEDURE __ShardManagement.spKillSessionsForShardMappingLocal
  END
GO

IF object_id(N'__ShardManagement.spBulkOperationShardMappingsLocal', N'P') IS NOT NULL
  BEGIN
    DROP PROCEDURE __ShardManagement.spBulkOperationShardMappingsLocal
  END
GO

IF object_id(N'__ShardManagement.spValidateShardMappingLocal', N'P') IS NOT NULL
  BEGIN
    DROP PROCEDURE __ShardManagement.spValidateShardMappingLocal
  END
GO

IF object_id(N'__ShardManagement.spFindShardMappingByKeyLocal', N'P') IS NOT NULL
  BEGIN
    DROP PROCEDURE __ShardManagement.spFindShardMappingByKeyLocal
  END
GO

IF object_id(N'__ShardManagement.spGetAllShardMappingsLocal', N'P') IS NOT NULL
  BEGIN
    DROP PROCEDURE __ShardManagement.spGetAllShardMappingsLocal
  END
GO

---------------------------------------------------------------------------------------------------
-- Shards
---------------------------------------------------------------------------------------------------
IF object_id(N'__ShardManagement.spUpdateShardLocal', N'P') IS NOT NULL
  BEGIN
    DROP PROCEDURE __ShardManagement.spUpdateShardLocal
  END
GO

IF object_id(N'__ShardManagement.spRemoveShardLocal', N'P') IS NOT NULL
  BEGIN
    DROP PROCEDURE __ShardManagement.spRemoveShardLocal
  END
GO

IF object_id(N'__ShardManagement.spAddShardLocal', N'P') IS NOT NULL
  BEGIN
    DROP PROCEDURE __ShardManagement.spAddShardLocal
  END
GO

IF object_id(N'__ShardManagement.spValidateShardLocal', N'P') IS NOT NULL
  BEGIN
    DROP PROCEDURE __ShardManagement.spValidateShardLocal
  END
GO

IF object_id(N'__ShardManagement.spGetAllShardsLocal', N'P') IS NOT NULL
  BEGIN
    DROP PROCEDURE __ShardManagement.spGetAllShardsLocal
  END
GO

---------------------------------------------------------------------------------------------------
-- Helper SPs and Functions
---------------------------------------------------------------------------------------------------
IF object_id(N'__ShardManagement.spGetStoreVersionLocalHelper', N'P') IS NOT NULL
  BEGIN
    DROP PROCEDURE __ShardManagement.spGetStoreVersionLocalHelper
  END
GO

IF object_id(N'__ShardManagement.fnGetStoreVersionLocal', N'FN') IS NOT NULL
  BEGIN
    DROP FUNCTION __ShardManagement.fnGetStoreVersionLocal
  END
GO

IF object_id(N'__ShardManagement.fnGetStoreVersionMajorLocal', N'FN') IS NOT NULL
  BEGIN
    DROP FUNCTION __ShardManagement.fnGetStoreVersionMajorLocal
  END
GO

---------------------------------------------------------------------------------------------------
-- Tables
---------------------------------------------------------------------------------------------------
IF object_id(N'__ShardManagement.fkShardMappingsLocal_ShardId', N'F') IS NOT NULL
  BEGIN
    ALTER TABLE __ShardManagement.ShardMappingsLocal
      DROP CONSTRAINT fkShardMappingsLocal_ShardId
  END
GO

IF object_id(N'__ShardManagement.fkShardMappingsLocal_ShardMapId', N'F') IS NOT NULL
  BEGIN
    ALTER TABLE __ShardManagement.ShardMappingsLocal
      DROP CONSTRAINT fkShardMappingsLocal_ShardMapId
  END
GO

IF object_id(N'__ShardManagement.fkShardsLocal_ShardMapId ', N'F') IS NOT NULL
  BEGIN
    ALTER TABLE __ShardManagement.ShardsLocal
      DROP CONSTRAINT fkShardsLocal_ShardMapId
  END
GO

IF object_id(N'__ShardManagement.ucShardMappingsLocal_ShardMapId_MinValue', N'UQ') IS NOT NULL
  BEGIN
    ALTER TABLE __ShardManagement.ShardMappingsLocal
      DROP CONSTRAINT ucShardMappingsLocal_ShardMapId_MinValue
  END
GO

IF object_id(N'__ShardManagement.ucShardsLocal_ShardMapId_Location', N'UQ') IS NOT NULL
  BEGIN
    ALTER TABLE __ShardManagement.ShardsLocal
      DROP CONSTRAINT ucShardsLocal_ShardMapId_Location
  END
GO

-- DEVNOTE(wbasheer): Introduce this once we allow overwrite existing semantics on CreateShard
--if object_id(N'__ShardManagement.ucShardMapsLocal_Name', N'UQ') is not null
--begin
--	alter table __ShardManagement.ShardsLocal
--		drop constraint ucShardMapsLocal_Name
--end
--go

IF object_id(N'__ShardManagement.ShardMappingsLocal', N'U') IS NOT NULL
  BEGIN
    DROP TABLE __ShardManagement.ShardMappingsLocal
  END
GO

IF object_id(N'__ShardManagement.ShardsLocal', N'U') IS NOT NULL
  BEGIN
    DROP TABLE __ShardManagement.ShardsLocal
  END
GO

IF object_id(N'__ShardManagement.ShardMapsLocal', N'U') IS NOT NULL
  BEGIN
    DROP TABLE __ShardManagement.ShardMapsLocal
  END
GO

IF object_id(N'__ShardManagement.ShardMapManagerLocal', N'U') IS NOT NULL
  BEGIN
    DROP TABLE __ShardManagement.ShardMapManagerLocal
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

-- Copyright (c) Microsoft. All rights reserved.
-- Licensed under the MIT license. See LICENSE file in the project root for full license information.

---------------------------------------------------------------------------------------------------
-- Script to upgrade Global Shard Map from version 1.1 to 1.2
-- Fix for VSTS# 3410606
---------------------------------------------------------------------------------------------------

-- drop extra objects from version 1.1

IF object_id(N'__ShardManagement.spLockOrUnlockShardMappingsGlobal', N'P') IS NOT NULL
  BEGIN
    DROP PROCEDURE __ShardManagement.spLockOrUnlockShardMappingsGlobal
  END
GO

-- create new objects for version 1.2

---------------------------------------------------------------------------------------------------
-- __ShardManagement.spLockOrUnlockShardMappingsGlobal
-- Constraints:
-- Locks the specified range
---------------------------------------------------------------------------------------------------
CREATE PROCEDURE __ShardManagement.spLockOrUnlockShardMappingsGlobal
    @input  XML,
    @result INT OUTPUT
AS
  BEGIN
    DECLARE @gsmVersionMajorClient INT,
    @gsmVersionMinorClient INT,
    @shardMapId UNIQUEIDENTIFIER,
    @mappingId UNIQUEIDENTIFIER,
    @lockOwnerId UNIQUEIDENTIFIER,
    @lockOperationType INT

    SELECT
      @gsmVersionMajorClient = x.value('(GsmVersion/MajorVersion)[1]', 'int'),
      @gsmVersionMinorClient = x.value('(GsmVersion/MinorVersion)[1]', 'int'),
      @shardMapId = x.value('(ShardMap/Id)[1]', 'uniqueidentifier'),
      @mappingId = x.value('(Mapping/Id)[1]', 'uniqueidentifier'),
      @lockOwnerId = x.value('(Lock/Id)[1]', 'uniqueidentifier'),
      @lockOperationType = x.value('(Lock/Operation)[1]', 'int')
    FROM
      @input.nodes('/LockOrUnlockShardMappingsGlobal') AS t(x)

    IF (@gsmVersionMajorClient IS NULL OR @gsmVersionMinorClient IS NULL OR @shardMapId IS NULL OR
        @lockOwnerId IS NULL
        OR @lockOperationType IS NULL)
      GOTO Error_MissingParameters;

    IF (@gsmVersionMajorClient <> __ShardManagement.fnGetStoreVersionMajorGlobal())
      GOTO Error_GSMVersionMismatch;

    IF (@lockOperationType < 2 AND @mappingId IS NULL)
      GOTO Error_MissingParameters;

    IF NOT exists(
        SELECT ShardMapId
        FROM
          __ShardManagement.ShardMapsGlobal
          WITH ( UPDLOCK )
        WHERE
          ShardMapId = @shardMapId)
      GOTO Error_ShardMapNotFound;

    DECLARE @DefaultLockOwnerId UNIQUEIDENTIFIER = '00000000-0000-0000-0000-000000000000',
    @currentOperationId UNIQUEIDENTIFIER

    IF (@lockOperationType < 2)
      BEGIN
        DECLARE @ForceUnLockLockOwnerId UNIQUEIDENTIFIER = 'FFFFFFFF-FFFF-FFFF-FFFF-FFFFFFFFFFFF',
        @currentLockOwnerId UNIQUEIDENTIFIER

        SELECT
          @currentOperationId = OperationId,
          @currentLockOwnerId = LockOwnerId
        FROM
          __ShardManagement.ShardMappingsGlobal
          WITH ( UPDLOCK )
        WHERE
          ShardMapId = @shardMapId AND MappingId = @mappingId

        IF (@currentLockOwnerId IS NULL)
          GOTO Error_MappingDoesNotExist;

        IF (@currentOperationId IS NOT NULL)
          GOTO Error_ShardPendingOperation;

        IF (@lockOperationType = 0 AND @currentLockOwnerId <> @DefaultLockOwnerId)
          GOTO Error_MappingAlreadyLocked;

        IF (@lockOperationType = 1 AND (@lockOwnerId <> @currentLockOwnerId) AND
            (@lockOwnerId <> @ForceUnLockLockOwnerId))
          GOTO Error_MappingLockOwnerIdMismatch;
      END

    UPDATE
      __ShardManagement.ShardMappingsGlobal
    SET
      LockOwnerId = CASE
                    WHEN
                      @lockOperationType = 0
                      THEN
                        @lockOwnerId
                    WHEN
                      @lockOperationType = 1 OR @lockOperationType = 2 OR @lockOperationType = 3
                      THEN
                        @DefaultLockOwnerId
                    END
    WHERE
      ShardMapId = @shardMapId AND (@lockOperationType = 3 OR -- unlock all mappings
                                    (@lockOperationType = 2 AND LockOwnerId = @lockOwnerId) OR
                                    -- unlock all mappings for specified LockOwnerId
                                    MappingId =
                                    @mappingId) -- lock/unlock specified mapping with specified LockOwnerId

    Success_Exit:
    SET @result = 1 -- success
    GOTO Exit_Procedure;

    Error_ShardMapNotFound:
    SET @result = 102
    GOTO Exit_Procedure;

    Error_MappingDoesNotExist:
    SET @result = 301
    GOTO Exit_Procedure;

    Error_MappingLockOwnerIdMismatch:
    SET @result = 307
    GOTO Exit_Procedure;

    Error_MappingAlreadyLocked:
    SET @result = 308
    GOTO Exit_Procedure;

    Error_MissingParameters:
    SET @result = 50
    EXEC __ShardManagement.spGetStoreVersionGlobalHelper
    GOTO Exit_Procedure;

    Error_GSMVersionMismatch:
    SET @result = 51
    EXEC __ShardManagement.spGetStoreVersionGlobalHelper
    GOTO Exit_Procedure;

    Error_ShardPendingOperation:
    SET @result = 52
    EXEC __ShardManagement.spGetOperationLogEntryGlobalHelper @currentOperationId
    GOTO Exit_Procedure;

    Exit_Procedure:
  END
GO

-- update version as 1.2
UPDATE
  __ShardManagement.ShardMapManagerGlobal
SET
  StoreVersionMinor = 2
WHERE
  StoreVersionMajor = 1 AND StoreVersionMinor = 1

GO

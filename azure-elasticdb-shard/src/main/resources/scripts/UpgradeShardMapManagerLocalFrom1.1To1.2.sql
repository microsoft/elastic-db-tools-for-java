-- Copyright (c) Microsoft. All rights reserved.
-- Licensed under the MIT license. See LICENSE file in the project root for full license information.

---------------------------------------------------------------------------------------------------
-- Script to upgrade Local Shard Map from version 1.1 to 1.2
---------------------------------------------------------------------------------------------------

-- drop extra objects from version 1.1

IF object_id(N'__ShardManagement.spUpdateShardLocal', N'P') IS NOT NULL
  BEGIN
    DROP PROCEDURE __ShardManagement.spUpdateShardLocal
  END
GO

IF object_id(N'__ShardManagement.spBulkOperationShardMappingsLocal', N'P') IS NOT NULL
  BEGIN
    DROP PROCEDURE __ShardManagement.spBulkOperationShardMappingsLocal
  END
GO

IF object_id(N'__ShardManagement.spAddShardLocal', N'P') IS NOT NULL
  BEGIN
    DROP PROCEDURE __ShardManagement.spAddShardLocal
  END
GO

-- create new objects for version 1.2

---------------------------------------------------------------------------------------------------
-- __ShardManagement.spUpdateShardLocal
-- Constraints:
---------------------------------------------------------------------------------------------------
CREATE PROCEDURE __ShardManagement.spUpdateShardLocal
    @input  XML,
    @result INT OUTPUT
AS
  BEGIN
    DECLARE @lsmVersionMajorClient INT,
    @lsmVersionMinorClient INT,
    @operationId UNIQUEIDENTIFIER,
    @shardMapId UNIQUEIDENTIFIER,
    @shardId UNIQUEIDENTIFIER,
    @shardVersion UNIQUEIDENTIFIER,
    @protocol INT,
    @serverName NVARCHAR(128),
    @port INT,
    @databaseName NVARCHAR(128),
    @shardStatus INT

    SELECT
      @lsmVersionMajorClient = x.value('(LsmVersion/MajorVersion)[1]', 'int'),
      @lsmVersionMinorClient = x.value('(LsmVersion/MinorVersion)[1]', 'int'),
      @operationId = x.value('(@OperationId)[1]', 'uniqueidentifier'),
      @shardMapId = x.value('(ShardMap/Id)[1]', 'uniqueidentifier'),
      @shardId = x.value('(Shard/Id)[1]', 'uniqueidentifier'),
      @shardVersion = x.value('(Shard/Version)[1]', 'uniqueidentifier'),
      @protocol = x.value('(Shard/Location/Protocol)[1]', 'int'),
      @serverName = x.value('(Shard/Location/ServerName)[1]', 'nvarchar(128)'),
      @port = x.value('(Shard/Location/Port)[1]', 'int'),
      @databaseName = x.value('(Shard/Location/DatabaseName)[1]', 'nvarchar(128)'),
      @shardStatus = x.value('(Shard/Status)[1]', 'int')
    FROM
      @input.nodes('/UpdateShardLocal') AS t(x)

    IF (@lsmVersionMajorClient IS NULL OR @lsmVersionMinorClient IS NULL OR @operationId IS NULL OR
        @shardMapId IS NULL OR @shardId IS NULL OR @shardVersion IS NULL OR @shardStatus IS NULL OR
        @protocol IS NULL OR @serverName IS NULL OR @port IS NULL OR @databaseName IS NULL)
      GOTO Error_MissingParameters;

    IF (@lsmVersionMajorClient <> __ShardManagement.fnGetStoreVersionMajorLocal())
      GOTO Error_LSMVersionMismatch;

    UPDATE
      __ShardManagement.ShardsLocal
    SET
      Version         = @shardVersion,
      Status          = @shardStatus,
      Protocol        = @protocol,
      ServerName      = @serverName,
      Port            = @port,
      DatabaseName    = @databaseName,
      LastOperationId = @operationId
    WHERE
      ShardMapId = @shardMapId AND ShardId = @shardId

    IF (@@rowcount = 0)
      GOTO Error_ShardDoesNotExist;

    SET @result = 1
    GOTO Exit_Procedure;

    Error_MissingParameters:
    SET @result = 50
    EXEC __ShardManagement.spGetStoreVersionLocalHelper
    GOTO Exit_Procedure;

    Error_LSMVersionMismatch:
    SET @result = 51
    EXEC __ShardManagement.spGetStoreVersionLocalHelper
    GOTO Exit_Procedure;

    Error_ShardDoesNotExist:
    SET @result = 202
    GOTO Exit_Procedure;

    Exit_Procedure:
  END
GO

---------------------------------------------------------------------------------------------------
-- __ShardManagement.spBulkOperationShardMappingsLocal
-- Constraints:
---------------------------------------------------------------------------------------------------
CREATE PROCEDURE __ShardManagement.spBulkOperationShardMappingsLocal
    @input  XML,
    @result INT OUTPUT
AS
  BEGIN
    DECLARE @lsmVersionMajorClient INT,
    @lsmVersionMinorClient INT,
    @operationId UNIQUEIDENTIFIER,
    @operationCode INT,
    @undo INT,
    @stepsCount INT,
    @shardMapId UNIQUEIDENTIFIER,
    @sm_kind INT,
    @shardId UNIQUEIDENTIFIER,
    @shardVersion UNIQUEIDENTIFIER

    -- get operation information as well as number of steps information
    SELECT
      @lsmVersionMajorClient = x.value('(LsmVersion/MajorVersion)[1]', 'int'),
      @lsmVersionMinorClient = x.value('(LsmVersion/MinorVersion)[1]', 'int'),
      @operationId = x.value('(@OperationId)[1]', 'uniqueidentifier'),
      @undo = x.value('(@Undo)[1]', 'int'),
      @stepsCount = x.value('(@StepsCount)[1]', 'int'),
      @shardMapId = x.value('(ShardMap/Id)[1]', 'uniqueidentifier'),
      @shardId = x.value('(Shard/Id)[1]', 'uniqueidentifier'),
      @shardVersion = x.value('(Shard/Version)[1]', 'uniqueidentifier')
    FROM
      @input.nodes('/BulkOperationShardMappingsLocal') AS t(x)

    IF (
      @lsmVersionMajorClient IS NULL OR @lsmVersionMinorClient IS NULL OR @operationId IS NULL OR
      @stepsCount IS NULL OR
      @shardMapId IS NULL OR @shardId IS NULL OR @shardVersion IS NULL)
      GOTO Error_MissingParameters;

    IF (@lsmVersionMajorClient <> __ShardManagement.fnGetStoreVersionMajorLocal())
      GOTO Error_LSMVersionMismatch;

    -- check for reentrancy
    IF exists(
        SELECT ShardId
        FROM
          __ShardManagement.ShardsLocal
        WHERE
          ShardMapId = @shardMapId AND ShardId = @shardId AND Version = @shardVersion AND
          LastOperationId = @operationId)
      GOTO Success_Exit;

    -- update the shard entry
    UPDATE __ShardManagement.ShardsLocal
    SET
      Version         = @shardVersion,
      LastOperationId = @operationId
    WHERE
      ShardMapId = @shardMapId AND ShardId = @shardId

    DECLARE @currentStep XML,
    @stepIndex INT = 1,
    @stepType INT,
    @stepMappingId UNIQUEIDENTIFIER

    WHILE (@stepIndex <= @stepsCount)
      BEGIN
        SELECT @currentStep = x.query('(./Step[@Id = sql:variable("@stepIndex")])[1]')
        FROM
          @input.nodes('/BulkOperationShardMappingsLocal/Steps') AS t(x)

        -- Identify the step type.
        SELECT
          @stepType = x.value('(@Kind)[1]', 'int'),
          @stepMappingId = x.value('(Mapping/Id)[1]', 'uniqueidentifier')
        FROM
          @currentStep.nodes('./Step') AS t(x)

        IF (@stepType IS NULL OR @stepMappingId IS NULL)
          GOTO Error_MissingParameters;

        IF (@stepType = 1)
          BEGIN
            -- Remove Mapping
            DELETE
              __ShardManagement.ShardMappingsLocal
            WHERE
              ShardMapId = @shardMapId AND MappingId = @stepMappingId
          END
        ELSE
          IF (@stepType = 3)
            BEGIN
              DECLARE @stepMinValue VARBINARY(128),
              @stepMaxValue VARBINARY(128),
              @stepMappingStatus INT

              -- AddMapping
              SELECT
                @stepMinValue =
                convert(VARBINARY(128), x.value('(Mapping/MinValue)[1]', 'varchar(258)'), 1),
                @stepMaxValue =
                convert(VARBINARY(128), x.value('(Mapping/MaxValue[@Null="0"])[1]', 'varchar(258)'),
                        1),
                @stepMappingStatus = x.value('(Mapping/Status)[1]', 'int')
              FROM
                @currentStep.nodes('./Step') AS t(x)

              IF (@stepMinValue IS NULL OR @stepMappingStatus IS NULL)
                GOTO Error_MissingParameters;

              -- add mapping
              BEGIN TRY
              INSERT INTO
                __ShardManagement.ShardMappingsLocal
                (MappingId,
                 ShardId,
                 ShardMapId,
                 MinValue,
                 MaxValue,
                 Status,
                 LastOperationId)
              VALUES
                (@stepMappingId,
                 @shardId,
                 @shardMapId,
                 @stepMinValue,
                 @stepMaxValue,
                 @stepMappingStatus,
                 @operationId)
              END TRY
              BEGIN CATCH
              IF (@undo != 1)
                BEGIN
                  DECLARE @errorMessage NVARCHAR(MAX) = error_message(),
                  @errorNumber INT = error_number(),
                  @errorSeverity INT = error_severity(),
                  @errorState INT = error_state(),
                  @errorLine INT = error_line(),
                  @errorProcedure NVARCHAR(128) = isnull(error_procedure(), '-');

                  SELECT @errorMessage =
                         N'Error %d, Level %d, State %d, Procedure %s, Line %d, Message: ' +
                         @errorMessage
                  RAISERROR (@errorMessage, @errorSeverity, 1, @errorNumber, @errorSeverity, @errorState, @errorProcedure, @errorLine);
                  ROLLBACK TRANSACTION; -- To avoid extra error message in response.
                  GOTO Error_UnexpectedError;
                END
              END CATCH

              SET @stepMinValue = NULL
              SET @stepMaxValue = NULL
              SET @stepMappingStatus = NULL

            END

        -- reset state for next iteration
        SET @stepType = NULL
        SET @stepMappingId = NULL

        SET @stepIndex = @stepIndex + 1
      END

    GOTO Success_Exit;

    Error_MissingParameters:
    SET @result = 50
    EXEC __ShardManagement.spGetStoreVersionLocalHelper
    GOTO Exit_Procedure;

    Error_LSMVersionMismatch:
    SET @result = 51
    EXEC __ShardManagement.spGetStoreVersionLocalHelper
    GOTO Exit_Procedure;

    Error_UnexpectedError:
    SET @result = 53
    GOTO Exit_Procedure;

    Success_Exit:
    SET @result = 1
    GOTO Exit_Procedure;

    Exit_Procedure:
  END
GO

---------------------------------------------------------------------------------------------------
-- __ShardManagement.spAddShardLocal
-- Constraints:
---------------------------------------------------------------------------------------------------
CREATE PROCEDURE __ShardManagement.spAddShardLocal
    @input  XML,
    @result INT OUTPUT
AS
  BEGIN
    DECLARE @lsmVersionMajorClient INT,
    @lsmVersionMinorClient INT,
    @operationId UNIQUEIDENTIFIER,
    @undo INT,
    @shardMapId UNIQUEIDENTIFIER,
    @shardId UNIQUEIDENTIFIER,
    @name NVARCHAR(50),
    @sm_kind INT,
    @sm_keykind INT,
    @shardVersion UNIQUEIDENTIFIER,
    @protocol INT,
    @serverName NVARCHAR(128),
    @port INT,
    @databaseName NVARCHAR(128),
    @shardStatus INT,
    @errorMessage NVARCHAR(MAX),
    @errorNumber INT,
    @errorSeverity INT,
    @errorState INT,
    @errorLine INT,
    @errorProcedure NVARCHAR(128)
    SELECT
      @lsmVersionMajorClient = x.value('(LsmVersion/MajorVersion)[1]', 'int'),
      @lsmVersionMinorClient = x.value('(LsmVersion/MinorVersion)[1]', 'int'),
      @operationId = x.value('(@OperationId)[1]', 'uniqueidentifier'),
      @undo = x.value('(@Undo)[1]', 'int'),
      @shardMapId = x.value('(ShardMap/Id)[1]', 'uniqueidentifier'),
      @name = x.value('(ShardMap/Name)[1]', 'nvarchar(50)'),
      @sm_kind = x.value('(ShardMap/Kind)[1]', 'int'),
      @sm_keykind = x.value('(ShardMap/KeyKind)[1]', 'int'),
      @shardId = x.value('(Shard/Id)[1]', 'uniqueidentifier'),
      @shardVersion = x.value('(Shard/Version)[1]', 'uniqueidentifier'),
      @protocol = x.value('(Shard/Location/Protocol)[1]', 'int'),
      @serverName = x.value('(Shard/Location/ServerName)[1]', 'nvarchar(128)'),
      @port = x.value('(Shard/Location/Port)[1]', 'int'),
      @databaseName = x.value('(Shard/Location/DatabaseName)[1]', 'nvarchar(128)'),
      @shardStatus = x.value('(Shard/Status)[1]', 'int')
    FROM
      @input.nodes('/AddShardLocal') AS t(x)

    IF (
      @lsmVersionMajorClient IS NULL OR @lsmVersionMinorClient IS NULL OR @shardMapId IS NULL OR
      @operationId IS NULL OR
      @name IS NULL OR @sm_kind IS NULL OR @sm_keykind IS NULL OR
      @shardId IS NULL OR @shardVersion IS NULL OR @protocol IS NULL OR @serverName IS NULL OR
      @port IS NULL OR @databaseName IS NULL OR @shardStatus IS NULL)
      GOTO Error_MissingParameters;

    IF (@lsmVersionMajorClient <> __ShardManagement.fnGetStoreVersionMajorLocal())
      GOTO Error_LSMVersionMismatch;

    -- check for reentrancy
    IF exists(
        SELECT ShardMapId
        FROM
          __ShardManagement.ShardMapsLocal
        WHERE
          ShardMapId = @shardMapId AND LastOperationId = @operationId)
      GOTO Success_Exit;

    -- add shard map row, ignore duplicate inserts in this is part of undo operation
    BEGIN TRY
    INSERT INTO
      __ShardManagement.ShardMapsLocal
      (ShardMapId, Name, MapType, KeyType, LastOperationId)
    VALUES
      (@shardMapId, @name, @sm_kind, @sm_keykind, @operationId)
    END TRY
    BEGIN CATCH
    IF (@undo != 1)
      BEGIN
        SET @errorMessage = error_message();
        SET @errorNumber = error_number();
        SET @errorSeverity = error_severity();
        SET @errorState = error_state();
        SET @errorLine = error_line();
        SET @errorProcedure = isnull(error_procedure(), '-');
        SELECT @errorMessage =
               N'Error %d, Level %d, State %d, Procedure %s, Line %d, Message: ' + @errorMessage
        RAISERROR (@errorMessage, @errorSeverity, 1, @errorNumber, @errorSeverity, @errorState, @errorProcedure, @errorLine);
        ROLLBACK TRANSACTION; -- To avoid extra error message in response.
        GOTO Error_UnexpectedError;
      END
    END CATCH

    -- add shard row, ignore duplicate inserts if this is part of undo operation
    BEGIN TRY
    INSERT INTO
      __ShardManagement.ShardsLocal (
        ShardId,
        Version,
        ShardMapId,
        Protocol,
        ServerName,
        Port,
        DatabaseName,
        Status,
        LastOperationId)
    VALUES (
      @shardId,
      @shardVersion,
      @shardMapId,
      @protocol,
      @serverName,
      @port,
      @databaseName,
      @shardStatus,
      @operationId)
    END TRY
    BEGIN CATCH
    IF (@undo != 1)
      BEGIN
        SET @errorMessage = error_message();
        SET @errorNumber = error_number();
        SET @errorSeverity = error_severity();
        SET @errorState = error_state();
        SET @errorLine = error_line();
        SET @errorProcedure = isnull(error_procedure(), '-');

        SELECT @errorMessage =
               N'Error %d, Level %d, State %d, Procedure %s, Line %d, Message: ' + @errorMessage
        RAISERROR (@errorMessage, @errorSeverity, 1, @errorNumber, @errorSeverity, @errorState, @errorProcedure, @errorLine);
        ROLLBACK TRANSACTION; -- To avoid extra error message in response.
        GOTO Error_UnexpectedError;
      END
    END CATCH

    GOTO Success_Exit;

    Error_MissingParameters:
    SET @result = 50
    EXEC __ShardManagement.spGetStoreVersionLocalHelper
    GOTO Exit_Procedure;

    Error_LSMVersionMismatch:
    SET @result = 51
    EXEC __ShardManagement.spGetStoreVersionLocalHelper
    GOTO Exit_Procedure;

    Error_UnexpectedError:
    SET @result = 53
    GOTO Exit_Procedure;

    Success_Exit:
    SET @result = 1
    GOTO Exit_Procedure;

    Exit_Procedure:
  END
GO

-- update version as 1.2
UPDATE
  __ShardManagement.ShardMapManagerLocal
SET
  StoreVersionMinor = 2
WHERE
  StoreVersionMajor = 1 AND StoreVersionMinor = 1
GO

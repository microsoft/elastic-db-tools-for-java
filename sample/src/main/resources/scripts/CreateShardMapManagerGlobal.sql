-- Copyright (c) Microsoft. All rights reserved.
-- Licensed under the MIT license. See LICENSE file in the project root for full license information.

---------------------------------------------------------------------------------------------------
-- Schema
---------------------------------------------------------------------------------------------------
IF schema_id('__ShardManagement') IS NULL
  BEGIN
    EXEC sp_executesql N'create schema __ShardManagement'
  END
GO

---------------------------------------------------------------------------------------------------
-- Tables
---------------------------------------------------------------------------------------------------
CREATE TABLE __ShardManagement.ShardMapManagerGlobal (
  StoreVersion INT NOT NULL
)
GO

CREATE TABLE __ShardManagement.ShardMapsGlobal (
  ShardMapId   UNIQUEIDENTIFIER                                  NOT NULL,
  Name         NVARCHAR(50) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL,
  ShardMapType INT                                               NOT NULL,
  KeyType      INT                                               NOT NULL
)
GO

CREATE TABLE __ShardManagement.ShardsGlobal (
  ShardId      UNIQUEIDENTIFIER                                   NOT NULL,
  Readable     BIT                                                NOT NULL,
  Version      UNIQUEIDENTIFIER                                   NOT NULL,
  ShardMapId   UNIQUEIDENTIFIER                                   NOT NULL,
  OperationId  UNIQUEIDENTIFIER,
  Protocol     INT                                                NOT NULL,
  ServerName   NVARCHAR(128) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL,
  Port         INT                                                NOT NULL,
  DatabaseName NVARCHAR(128) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL,
  Status       INT                                                NOT NULL -- user defined
)
GO

CREATE TABLE __ShardManagement.ShardMappingsGlobal (
  MappingId   UNIQUEIDENTIFIER                                                  NOT NULL,
  Readable    BIT                                                               NOT NULL,
  ShardId     UNIQUEIDENTIFIER                                                  NOT NULL,
  ShardMapId  UNIQUEIDENTIFIER                                                  NOT NULL,
  OperationId UNIQUEIDENTIFIER,
  MinValue    VARBINARY(128)                                                    NOT NULL,
  MaxValue    VARBINARY(128), -- null = +infinity for range shard map
  Status      INT                                                               NOT NULL,
  LockOwnerId UNIQUEIDENTIFIER DEFAULT ('00000000-0000-0000-0000-000000000000') NOT NULL
)
GO

CREATE TABLE __ShardManagement.OperationsLogGlobal (
  OperationId         UNIQUEIDENTIFIER  NOT NULL,
  OperationCode       INT               NOT NULL,
  Data                XML               NOT NULL,
  UndoStartState      INT DEFAULT (100) NOT NULL,
  ShardVersionRemoves UNIQUEIDENTIFIER,
  ShardVersionAdds    UNIQUEIDENTIFIER
)
GO

CREATE TABLE __ShardManagement.ShardedDatabaseSchemaInfosGlobal (
  Name       NVARCHAR(128) NOT NULL,
  SchemaInfo XML           NOT NULL
)
GO

---------------------------------------------------------------------------------------------------
-- Constraints
---------------------------------------------------------------------------------------------------
ALTER TABLE __ShardManagement.ShardMapManagerGlobal
  ADD CONSTRAINT pkShardMapManagerGlobal_StoreVersion PRIMARY KEY (StoreVersion)
GO

ALTER TABLE __ShardManagement.ShardMapsGlobal
  ADD CONSTRAINT pkShardMapsGlobal_ShardMapId PRIMARY KEY (ShardMapId)
GO

ALTER TABLE __ShardManagement.ShardsGlobal
  ADD CONSTRAINT pkShardsGlobal_ShardId PRIMARY KEY (ShardId)
GO

ALTER TABLE __ShardManagement.ShardMappingsGlobal
  ADD CONSTRAINT pkShardMappingsGlobal_ShardMapId_MinValue_Readable PRIMARY KEY (ShardMapId, MinValue, Readable)
GO

ALTER TABLE __ShardManagement.OperationsLogGlobal
  ADD CONSTRAINT pkOperationsLogGlobal_OperationId PRIMARY KEY (OperationId)
GO

ALTER TABLE __ShardManagement.ShardedDatabaseSchemaInfosGlobal
  ADD CONSTRAINT pkShardedDatabaseSchemaInfosGlobal_Name PRIMARY KEY (Name)
GO

ALTER TABLE __ShardManagement.ShardMapsGlobal
  ADD CONSTRAINT ucShardMapsGlobal_Name UNIQUE (Name)
GO

ALTER TABLE __ShardManagement.ShardsGlobal
  ADD CONSTRAINT ucShardsGlobal_Location UNIQUE (ShardMapId, Protocol, ServerName, DatabaseName, Port)
GO

ALTER TABLE __ShardManagement.ShardMappingsGlobal
  ADD CONSTRAINT ucShardMappingsGlobal_MappingId UNIQUE (MappingId)
GO

ALTER TABLE __ShardManagement.ShardsGlobal
  ADD CONSTRAINT fkShardsGlobal_ShardMapId FOREIGN KEY (ShardMapId) REFERENCES __ShardManagement.ShardMapsGlobal (ShardMapId)
GO

ALTER TABLE __ShardManagement.ShardMappingsGlobal
  ADD CONSTRAINT fkShardMappingsGlobal_ShardMapId FOREIGN KEY (ShardMapId) REFERENCES __ShardManagement.ShardMapsGlobal (ShardMapId)
GO

ALTER TABLE __ShardManagement.ShardMappingsGlobal
  ADD CONSTRAINT fkShardMappingsGlobal_ShardId FOREIGN KEY (ShardId) REFERENCES __ShardManagement.ShardsGlobal (ShardId)
GO

---------------------------------------------------------------------------------------------------
-- Data
---------------------------------------------------------------------------------------------------
INSERT INTO
  __ShardManagement.ShardMapManagerGlobal (StoreVersion)
VALUES
  (1)
GO

---------------------------------------------------------------------------------------------------
-- Result Codes: keep these in sync with enum StoreResult
---------------------------------------------------------------------------------------------------
-- 001 => Success.

-- 050 => Missing parameters for stored procedure.
-- 051 => Store Version mismatch.
-- 052 => There is a pending operation on shard.
-- 053 => Unexpected store error.

-- 101 => A shard map with the given Name already exists.
-- 102 => The shard map does not exist.
-- 103 => The shard map cannot be deleted since there are shards associated with it.

-- 201 => The shard already exists.
-- 202 => The shard does not exist.
-- 203 => The shard already has some mappings associated with it.
-- 204 => The shard Version does not match with the Version specified.
-- 205 => The shard map already has the same location associated with another shard.

-- 301 => The shard mapping does not exist.
-- 302 => Range specified is already mapped by another shard mapping.
-- 303 => Point specified is already mapped by another shard mapping.
-- 304 => Shard mapping could not be found for key.
-- 305 => Unable to kill sessions corresponding to shard mapping.
-- 306 => Shard mapping is not offline.
-- 307 => Lock owner Id of shard mapping does not match.
-- 308 => Shard mapping is already locked.
-- 309 => Mapping is offline.

-- 401 => Schema Info Name Does Not Exist.
-- 402 => Schema Info Name Conflict.

---------------------------------------------------------------------------------------------------
-- Rowset Codes
---------------------------------------------------------------------------------------------------
-- 1 => ShardMap
-- 2 => Shard
-- 3 => ShardMapping
-- 4 => ShardLocation
-- 5 => StoreVersion
-- 6 => Operation
-- 7 => SchemaInfo

---------------------------------------------------------------------------------------------------
-- Shard Map Kind
---------------------------------------------------------------------------------------------------
-- 0 => Default
-- 1 => List
-- 2 => Range

---------------------------------------------------------------------------------------------------
-- Shard Mapping Status
---------------------------------------------------------------------------------------------------
-- 0 => Offline
-- 1 => Online

---------------------------------------------------------------------------------------------------
-- Operation Kind
---------------------------------------------------------------------------------------------------
-- NULL => None
-- 1 => Add/Update
-- 2 => Remove

---------------------------------------------------------------------------------------------------
-- Bulk Operation Step Types
---------------------------------------------------------------------------------------------------
-- 1 => Remove
-- 2 => Update
-- 3 => Add

---------------------------------------------------------------------------------------------------
-- Helper SPs and Functions
---------------------------------------------------------------------------------------------------

---------------------------------------------------------------------------------------------------
-- __ShardManagement.fnGetStoreVersionGlobal
---------------------------------------------------------------------------------------------------
CREATE FUNCTION __ShardManagement.fnGetStoreVersionGlobal()
  RETURNS INT
AS
  BEGIN
    RETURN (SELECT StoreVersion
            FROM __ShardManagement.ShardMapManagerGlobal)
  END
GO

---------------------------------------------------------------------------------------------------
-- __ShardManagement.spGetStoreVersionGlobalHelper
---------------------------------------------------------------------------------------------------
CREATE PROCEDURE __ShardManagement.spGetStoreVersionGlobalHelper
AS
  BEGIN
    SELECT
      5,
      StoreVersion
    FROM
      __ShardManagement.ShardMapManagerGlobal
  END
GO

---------------------------------------------------------------------------------------------------
-- __ShardManagement.spGetOperationLogEntryGlobalHelper
---------------------------------------------------------------------------------------------------
CREATE PROCEDURE __ShardManagement.spGetOperationLogEntryGlobalHelper
    @operationId UNIQUEIDENTIFIER
AS
  BEGIN
    SELECT
      6,
      OperationId,
      OperationCode,
      Data,
      UndoStartState,
      ShardVersionRemoves,
      ShardVersionAdds
    FROM
      __ShardManagement.OperationsLogGlobal
    WHERE
      OperationId = @operationId
  END
GO

---------------------------------------------------------------------------------------------------
-- Stored Procedures
---------------------------------------------------------------------------------------------------

---------------------------------------------------------------------------------------------------
-- Operations
---------------------------------------------------------------------------------------------------

---------------------------------------------------------------------------------------------------
-- __ShardManagement.spFindAndUpdateOperationLogEntryByIdGlobal
-- Constraints:
---------------------------------------------------------------------------------------------------
CREATE PROCEDURE __ShardManagement.spFindAndUpdateOperationLogEntryByIdGlobal
    @input  XML,
    @result INT OUTPUT
AS
  BEGIN
    SET NOCOUNT ON
    DECLARE @gsmVersionClient INT,
    @operationId UNIQUEIDENTIFIER,
    @undoStartState INT

    SELECT
      @gsmVersionClient = x.value('(GsmVersion)[1]', 'int'),
      @operationId = x.value('(@OperationId)[1]', 'uniqueidentifier'),
      @undoStartState = x.value('(@UndoStartState)[1]', 'int')
    FROM
      @input.nodes('/FindAndUpdateOperationLogEntryByIdGlobal') AS t(x)

    IF (@gsmVersionClient IS NULL OR @operationId IS NULL OR @undoStartState IS NULL)
      GOTO Error_MissingParameters;

    IF (@gsmVersionClient <> __ShardManagement.fnGetStoreVersionGlobal())
      GOTO Error_GSMVersionMismatch;

    UPDATE
      __ShardManagement.OperationsLogGlobal
    SET
      UndoStartState = @undoStartState
    WHERE
      OperationId = @operationId

    SET @result = 1
    EXEC __ShardManagement.spGetOperationLogEntryGlobalHelper @operationId
    GOTO Exit_Procedure;

    Error_MissingParameters:
    SET @result = 50
    EXEC __ShardManagement.spGetStoreVersionGlobalHelper
    GOTO Exit_Procedure;

    Error_GSMVersionMismatch:
    SET @result = 51
    EXEC __ShardManagement.spGetStoreVersionGlobalHelper
    GOTO Exit_Procedure;

    Exit_Procedure:
  END
GO

---------------------------------------------------------------------------------------------------
-- Shard Maps
---------------------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------------------
-- __ShardManagement.spGetAllShardMapsGlobal
-- Constraints:
---------------------------------------------------------------------------------------------------
CREATE PROCEDURE __ShardManagement.spGetAllShardMapsGlobal
    @input  XML,
    @result INT OUTPUT
AS
  BEGIN
    DECLARE @gsmVersionClient INT

    SELECT @gsmVersionClient = x.value('(GsmVersion)[1]', 'int')
    FROM
      @input.nodes('/GetAllShardMapsGlobal') AS t(x)

    IF (@gsmVersionClient IS NULL)
      GOTO Error_MissingParameters;

    IF (@gsmVersionClient > __ShardManagement.fnGetStoreVersionGlobal())
      GOTO Error_GSMVersionMismatch;

    SELECT
      1,
      ShardMapId,
      Name,
      ShardMapType,
      KeyType
    FROM
      __ShardManagement.ShardMapsGlobal

    SET @result = 1
    GOTO Exit_Procedure;

    Error_MissingParameters:
    SET @result = 50
    EXEC __ShardManagement.spGetStoreVersionGlobalHelper
    GOTO Exit_Procedure;

    Error_GSMVersionMismatch:
    SET @result = 51
    EXEC __ShardManagement.spGetStoreVersionGlobalHelper
    GOTO Exit_Procedure;

    Exit_Procedure:
  END
GO

---------------------------------------------------------------------------------------------------
-- __ShardManagement.spFindShardMapByNameGlobal
-- Constraints:
---------------------------------------------------------------------------------------------------
CREATE PROCEDURE __ShardManagement.spFindShardMapByNameGlobal
    @input  XML,
    @result INT OUTPUT
AS
  BEGIN
    DECLARE @gsmVersionClient INT,
    @name NVARCHAR(50)

    SELECT
      @gsmVersionClient = x.value('(GsmVersion)[1]', 'int'),
      @name = x.value('(ShardMap/Name)[1]', ' nvarchar(50)')
    FROM
      @input.nodes('/FindShardMapByNameGlobal') AS t(x)

    IF (@gsmVersionClient IS NULL OR @name IS NULL)
      GOTO Error_MissingParameters;

    IF (@gsmVersionClient > __ShardManagement.fnGetStoreVersionGlobal())
      GOTO Error_GSMVersionMismatch;

    SELECT
      1,
      ShardMapId,
      Name,
      ShardMapType,
      KeyType
    FROM
      __ShardManagement.ShardMapsGlobal
    WHERE
      Name = @name

    SET @result = 1
    GOTO Exit_Procedure;

    Error_MissingParameters:
    SET @result = 50
    EXEC __ShardManagement.spGetStoreVersionGlobalHelper
    GOTO Exit_Procedure;

    Error_GSMVersionMismatch:
    SET @result = 51
    EXEC __ShardManagement.spGetStoreVersionGlobalHelper
    GOTO Exit_Procedure;

    Exit_Procedure:
  END
GO

---------------------------------------------------------------------------------------------------
-- __ShardManagement.spGetAllDistinctShardLocationsGlobal
-- Constraints:
---------------------------------------------------------------------------------------------------
CREATE PROCEDURE __ShardManagement.spGetAllDistinctShardLocationsGlobal
    @input  XML,
    @result INT OUTPUT
AS
  BEGIN
    DECLARE @gsmVersionClient INT

    SELECT @gsmVersionClient = x.value('(GsmVersion)[1]', 'int')
    FROM
      @input.nodes('/GetAllDistinctShardLocationsGlobal') AS t(x)

    IF (@gsmVersionClient IS NULL)
      GOTO Error_MissingParameters;

    IF (@gsmVersionClient > __ShardManagement.fnGetStoreVersionGlobal())
      GOTO Error_GSMVersionMismatch;

    SELECT DISTINCT
      4,
      Protocol,
      ServerName,
      Port,
      DatabaseName
    FROM
      __ShardManagement.ShardsGlobal
    WHERE
      Readable = 1

    SET @result = 1
    GOTO Exit_Procedure;

    Error_MissingParameters:
    SET @result = 50
    EXEC __ShardManagement.spGetStoreVersionGlobalHelper
    GOTO Exit_Procedure;

    Error_GSMVersionMismatch:
    SET @result = 51
    EXEC __ShardManagement.spGetStoreVersionGlobalHelper
    GOTO Exit_Procedure;

    Exit_Procedure:
  END
GO

---------------------------------------------------------------------------------------------------
-- __ShardManagement.spAddShardMapGlobal
-- Constraints:
---------------------------------------------------------------------------------------------------
CREATE PROCEDURE __ShardManagement.spAddShardMapGlobal
    @input  XML,
    @result INT OUTPUT
AS
  BEGIN
    DECLARE @gsmVersionClient INT,
    @shardMapId UNIQUEIDENTIFIER,
    @name NVARCHAR(50),
    @mapType INT,
    @keyType INT

    SELECT
      @gsmVersionClient = x.value('(GsmVersion)[1]', 'int'),
      @shardMapId = x.value('(ShardMap/Id)[1]', 'uniqueidentifier'),
      @name = x.value('(ShardMap/Name)[1]', 'nvarchar(50)'),
      @mapType = x.value('(ShardMap/Kind)[1]', 'int'),
      @keyType = x.value('(ShardMap/KeyKind)[1]', 'int')
    FROM
      @input.nodes('/AddShardMapGlobal') AS t(x)

    IF (@gsmVersionClient IS NULL OR @shardMapId IS NULL OR @name IS NULL OR @mapType IS NULL OR
        @keyType IS NULL)
      GOTO Error_MissingParameters;

    IF (@gsmVersionClient <> __ShardManagement.fnGetStoreVersionGlobal())
      GOTO Error_GSMVersionMismatch;

    -- try to insert the row for the shard map, duplicate violation will be detected by the
    -- uniqueness constraint on the Name
    BEGIN TRY
    INSERT INTO
      __ShardManagement.ShardMapsGlobal
      (ShardMapId, Name, ShardMapType, KeyType)
    VALUES
      (@shardMapId, @name, @mapType, @keyType)
    END TRY
    BEGIN CATCH
    IF (error_number() = 2627)
      GOTO Error_ShardMapAlreadyExists;
    ELSE
      BEGIN
        DECLARE @errorMessage NVARCHAR(MAX) = error_message(),
        @errorNumber INT = error_number(),
        @errorSeverity INT = error_severity(),
        @errorState INT = error_state(),
        @errorLine INT = error_line(),
        @errorProcedure NVARCHAR(128) = isnull(error_procedure(), '-');

        SELECT @errorMessage =
               N'Error %d, Level %d, State %d, Procedure %s, Line %d, Message: ' + @errorMessage;

        RAISERROR (@errorMessage, @errorSeverity, 1, @errorNumber, @errorSeverity, @errorState, @errorProcedure, @errorLine);

        ROLLBACK TRANSACTION; -- To avoid extra error message in response.
        GOTO Error_UnexpectedError;
      END
    END CATCH

    SET @result = 1
    GOTO Exit_Procedure;

    Error_ShardMapAlreadyExists:
    SET @result = 101
    GOTO Exit_Procedure;

    Error_MissingParameters:
    SET @result = 50
    EXEC __ShardManagement.spGetStoreVersionGlobalHelper
    GOTO Exit_Procedure;

    Error_GSMVersionMismatch:
    SET @result = 51
    EXEC __ShardManagement.spGetStoreVersionGlobalHelper
    GOTO Exit_Procedure;

    Error_UnexpectedError:
    SET @result = 53
    GOTO Exit_Procedure;

    Exit_Procedure:
  END
GO

---------------------------------------------------------------------------------------------------
-- __ShardManagement.spRemoveShardMapGlobal
-- Constraints:
---------------------------------------------------------------------------------------------------
CREATE PROCEDURE __ShardManagement.spRemoveShardMapGlobal
    @input  XML,
    @result INT OUTPUT
AS
  BEGIN
    DECLARE @gsmVersionClient INT,
    @shardMapId UNIQUEIDENTIFIER

    SELECT
      @gsmVersionClient = x.value('(GsmVersion)[1]', 'int'),
      @shardMapId = x.value('(ShardMap/Id)[1]', ' uniqueidentifier')
    FROM
      @input.nodes('/RemoveShardMapGlobal') AS t(x)

    IF (@gsmVersionClient IS NULL OR @shardMapId IS NULL)
      GOTO Error_MissingParameters;

    IF (@gsmVersionClient <> __ShardManagement.fnGetStoreVersionGlobal())
      GOTO Error_GSMVersionMismatch;

    DECLARE @currentShardMapId UNIQUEIDENTIFIER

    SELECT @currentShardMapId = ShardMapId
    FROM
      __ShardManagement.ShardMapsGlobal
      WITH ( UPDLOCK )
    WHERE
      ShardMapId = @shardMapId

    IF (@currentShardMapId IS NULL)
      GOTO Error_ShardMapNotFound;

    IF exists(
        SELECT ShardId
        FROM
          __ShardManagement.ShardsGlobal
        WHERE
          ShardMapId = @shardMapId)
      GOTO Error_ShardMapHasShards;

    DELETE FROM
      __ShardManagement.ShardMapsGlobal
    WHERE
      ShardMapId = @shardMapId

    SET @result = 1
    GOTO Exit_Procedure;

    Error_ShardMapNotFound:
    SET @result = 102
    GOTO Exit_Procedure;

    Error_ShardMapHasShards:
    SET @result = 103
    GOTO Exit_Procedure;

    Error_MissingParameters:
    SET @result = 50
    EXEC __ShardManagement.spGetStoreVersionGlobalHelper
    GOTO Exit_Procedure;

    Error_GSMVersionMismatch:
    SET @result = 51
    EXEC __ShardManagement.spGetStoreVersionGlobalHelper
    GOTO Exit_Procedure;

    Exit_Procedure:
  END
GO

---------------------------------------------------------------------------------------------------
-- Shards
---------------------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------------------
-- __ShardManagement.spGetAllShardsGlobal
-- Constraints:
---------------------------------------------------------------------------------------------------
CREATE PROCEDURE __ShardManagement.spGetAllShardsGlobal
    @input  XML,
    @result INT OUTPUT
AS
  BEGIN
    DECLARE @gsmVersionClient INT,
    @shardMapId UNIQUEIDENTIFIER

    SELECT
      @gsmVersionClient = x.value('(GsmVersion)[1]', 'int'),
      @shardMapId = x.value('(ShardMap/Id)[1]', 'uniqueidentifier')
    FROM
      @input.nodes('/GetAllShardsGlobal') AS t(x)

    IF (@gsmVersionClient IS NULL OR @shardMapId IS NULL)
      GOTO Error_MissingParameters;

    IF (@gsmVersionClient > __ShardManagement.fnGetStoreVersionGlobal())
      GOTO Error_GSMVersionMismatch;

    IF NOT exists(
        SELECT ShardMapId
        FROM
          __ShardManagement.ShardMapsGlobal
        WHERE
          ShardMapId = @shardMapId)
      GOTO Error_ShardMapNotFound;

    SELECT
      2,
      ShardId,
      Version,
      ShardMapId,
      Protocol,
      ServerName,
      Port,
      DatabaseName,
      Status
    FROM
      __ShardManagement.ShardsGlobal
    WHERE
      ShardMapId = @shardMapId AND Readable = 1

    SET @result = 1
    GOTO Exit_Procedure;

    Error_ShardMapNotFound:
    SET @result = 102
    GOTO Exit_Procedure;

    Error_MissingParameters:
    SET @result = 50
    EXEC __ShardManagement.spGetStoreVersionGlobalHelper
    GOTO Exit_Procedure;

    Error_GSMVersionMismatch:
    SET @result = 51
    EXEC __ShardManagement.spGetStoreVersionGlobalHelper
    GOTO Exit_Procedure;

    Exit_Procedure:
  END
GO

---------------------------------------------------------------------------------------------------
-- __ShardManagement.spFindShardByLocationGlobal
-- Constraints:
---------------------------------------------------------------------------------------------------
CREATE PROCEDURE __ShardManagement.spFindShardByLocationGlobal
    @input  XML,
    @result INT OUTPUT
AS
  BEGIN
    DECLARE @gsmVersionClient INT,
    @shardMapId UNIQUEIDENTIFIER,
    @protocol INT,
    @serverName NVARCHAR(128),
    @port INT,
    @databaseName NVARCHAR(128)

    SELECT
      @gsmVersionClient = x.value('(GsmVersion)[1]', 'int'),
      @shardMapId = x.value('(ShardMap/Id)[1]', 'uniqueidentifier'),
      @protocol = x.value('(Location/Protocol)[1]', 'int'),
      @serverName = x.value('(Location/ServerName)[1]', 'nvarchar(128)'),
      @port = x.value('(Location/Port)[1]', 'int'),
      @databaseName = x.value('(Location/DatabaseName)[1]', 'nvarchar(128)')
    FROM
      @input.nodes('/FindShardByLocationGlobal') AS t(x)

    IF (@gsmVersionClient IS NULL OR @shardMapId IS NULL OR
        @protocol IS NULL OR @serverName IS NULL OR @port IS NULL OR @databaseName IS NULL)
      GOTO Error_MissingParameters;

    IF (@gsmVersionClient > __ShardManagement.fnGetStoreVersionGlobal())
      GOTO Error_GSMVersionMismatch;

    IF NOT exists(
        SELECT ShardMapId
        FROM
          __ShardManagement.ShardMapsGlobal
        WHERE
          ShardMapId = @shardMapId)
      GOTO Error_ShardMapNotFound;

    SELECT
      2,
      ShardId,
      Version,
      ShardMapId,
      Protocol,
      ServerName,
      Port,
      DatabaseName,
      Status
    FROM
      __ShardManagement.ShardsGlobal
    WHERE
      ShardMapId = @shardMapId AND
      Protocol = @protocol AND ServerName = @serverName AND Port = @port AND
      DatabaseName = @databaseName AND
      Readable = 1

    SET @result = 1
    GOTO Exit_Procedure;

    Error_ShardMapNotFound:
    SET @result = 102
    GOTO Exit_Procedure;

    Error_MissingParameters:
    SET @result = 50
    EXEC __ShardManagement.spGetStoreVersionGlobalHelper
    GOTO Exit_Procedure;

    Error_GSMVersionMismatch:
    SET @result = 51
    EXEC __ShardManagement.spGetStoreVersionGlobalHelper
    GOTO Exit_Procedure;

    Exit_Procedure:
  END
GO

---------------------------------------------------------------------------------------------------
-- __ShardManagement.spBulkOperationShardsGlobalBegin
-- Constraints:
---------------------------------------------------------------------------------------------------
CREATE PROCEDURE __ShardManagement.spBulkOperationShardsGlobalBegin
    @input  XML,
    @result INT OUTPUT
AS
  BEGIN
    SET NOCOUNT ON
    DECLARE @gsmVersionClient INT,
    @operationId UNIQUEIDENTIFIER,
    @operationCode INT,
    @stepsCount INT,
    @shardMapId UNIQUEIDENTIFIER

    -- get operation information as well as number of steps
    SELECT
      @gsmVersionClient = x.value('(GsmVersion)[1]', 'int'),
      @operationId = x.value('(@OperationId)[1]', 'uniqueidentifier'),
      @operationCode = x.value('(@OperationCode)[1]', 'int'),
      @stepsCount = x.value('(@StepsCount)[1]', 'int'),
      @shardMapId = x.value('(ShardMap/Id)[1]', 'uniqueidentifier')
    FROM
      @input.nodes('/BulkOperationShardsGlobal') AS t(x)

    IF (@gsmVersionClient IS NULL OR @operationId IS NULL OR @operationCode IS NULL OR
        @stepsCount IS NULL OR @shardMapId IS NULL)
      GOTO Error_MissingParameters;

    IF (@gsmVersionClient <> __ShardManagement.fnGetStoreVersionGlobal())
      GOTO Error_GSMVersionMismatch;

    -- check if shard map exists
    IF NOT exists(
        SELECT ShardMapId
        FROM
          __ShardManagement.ShardMapsGlobal
          WITH ( UPDLOCK )
        WHERE
          ShardMapId = @shardMapId)
      GOTO Error_ShardMapNotFound;

    -- add log record
    BEGIN TRY
    INSERT INTO __ShardManagement.OperationsLogGlobal (
      OperationId,
      OperationCode,
      Data,
      ShardVersionRemoves,
      ShardVersionAdds)
    VALUES (
      @operationId,
      @operationCode,
      @input,
      NULL,
      NULL)
    END TRY
    BEGIN CATCH
    -- if log record already exists, ignore
    IF (error_number() <> 2627)
      BEGIN
        DECLARE @errorMessage NVARCHAR(MAX) = error_message(),
        @errorNumber INT = error_number(),
        @errorSeverity INT = error_severity(),
        @errorState INT = error_state(),
        @errorLine INT = error_line(),
        @errorProcedure NVARCHAR(128) = isnull(error_procedure(), '-');

        SELECT @errorMessage =
               N'Error %d, Level %d, State %d, Procedure %s, Line %d, Message: ' + @errorMessage;

        RAISERROR (@errorMessage, @errorSeverity, 1, @errorNumber, @errorSeverity, @errorState, @errorProcedure, @errorLine);

        ROLLBACK TRANSACTION; -- To avoid extra error message in response.
        GOTO Error_UnexpectedError;
      END
    END CATCH

    -- Remove/Update/Add specific
    DECLARE @currentStep XML,
    @stepIndex INT = 1,
    @stepType INT,
    @stepShardId UNIQUEIDENTIFIER,
    @stepShardVersion UNIQUEIDENTIFIER,
    @currentShardVersion UNIQUEIDENTIFIER,
    @currentShardOperationId UNIQUEIDENTIFIER

    -- Add specific
    DECLARE @stepProtocol INT,
    @stepServerName NVARCHAR(128),
    @stepPort INT,
    @stepDatabaseName NVARCHAR(128),
    @stepShardStatus INT

    WHILE (@stepIndex <= @stepsCount)
      BEGIN
        SELECT @currentStep = x.query('(./Step[@Id = sql:variable("@stepIndex")])[1]')
        FROM
          @input.nodes('/BulkOperationShardsGlobal/Steps') AS t(x)

        -- Identify the step type.
        SELECT
          @stepType = x.value('(@Kind)[1]', 'int'),
          @stepShardId = x.value('(Shard/Id)[1]', 'uniqueidentifier'),
          @stepShardVersion = x.value('(Shard/Version)[1]', 'uniqueidentifier')
        FROM
          @currentStep.nodes('./Step') AS t(x)

        IF (@stepType IS NULL OR @stepShardId IS NULL OR @stepShardVersion IS NULL)
          GOTO Error_MissingParameters;

        IF (@stepType = 1 OR @stepType = 2)
          BEGIN
            -- Remove/Update Shard

            -- Check re-entrancy or pending operations
            SELECT
              @currentShardVersion = Version,
              @currentShardOperationId = OperationId
            FROM
              __ShardManagement.ShardsGlobal
              WITH ( UPDLOCK )
            WHERE
              ShardMapId = @shardMapId AND ShardId = @stepShardId AND Readable = 1

            -- re-entrancy
            IF (@currentShardOperationId = @operationId)
              GOTO Success_Exit;

            -- pending operation
            IF (@currentShardOperationId IS NOT NULL)
              GOTO Error_ShardPendingOperation;

            IF (@currentShardVersion IS NULL)
              GOTO Error_ShardDoesNotExist;

            IF (@currentShardVersion <> @stepShardVersion)
              GOTO Error_ShardVersionMismatch;

            -- check if mappings exist for the shard being deleted
            IF (@stepType = 1)
              BEGIN
                IF exists(
                    SELECT ShardId
                    FROM
                      __ShardManagement.ShardMappingsGlobal
                    WHERE
                      ShardMapId = @shardMapId AND ShardId = @stepShardId)
                  GOTO Error_ShardHasMappings;
              END

            -- mark pending operation on current shard
            UPDATE
              __ShardManagement.ShardsGlobal
            SET
              OperationId = @operationId
            WHERE
              ShardMapId = @shardMapId AND ShardId = @stepShardId
          END
        ELSE
          IF (@stepType = 3)
            BEGIN
              -- Add Shard

              -- read the information for add only
              SELECT
                @stepProtocol = x.value('(Shard/Location/Protocol)[1]', 'int'),
                @stepServerName = x.value('(Shard/Location/ServerName)[1]', 'nvarchar(128)'),
                @stepPort = x.value('(Shard/Location/Port)[1]', 'int'),
                @stepDatabaseName = x.value('(Shard/Location/DatabaseName)[1]', 'nvarchar(128)'),
                @stepShardStatus = x.value('(Shard/Status)[1]', 'int')
              FROM
                @currentStep.nodes('./Step') AS t(x)

              IF (@stepProtocol IS NULL OR @stepServerName IS NULL OR @stepPort IS NULL OR
                  @stepDatabaseName IS NULL OR
                  @stepShardStatus IS NULL)
                GOTO Error_MissingParameters;

              -- Check re-entrancy or pending operations
              SELECT
                @currentShardVersion = Version,
                @currentShardOperationId = OperationId
              FROM
                __ShardManagement.ShardsGlobal
                WITH ( UPDLOCK )
              WHERE
                ShardMapId = @shardMapId AND ShardId = @stepShardId

              -- re-entrancy
              IF (@currentShardOperationId = @operationId)
                GOTO Success_Exit;

              -- pending operation
              IF (@currentShardOperationId IS NOT NULL)
                GOTO Error_ShardPendingOperation;

              IF (@currentShardVersion IS NOT NULL)
                GOTO Error_ShardAlreadyExists;

              -- check for duplicate locations for add shard
              SET @currentShardVersion = NULL
              SET @currentShardOperationId = NULL

              SELECT
                @currentShardVersion = Version,
                @currentShardOperationId = OperationId
              FROM
                __ShardManagement.ShardsGlobal
              WHERE
                ShardMapId = @shardMapId AND
                Protocol = @stepProtocol AND
                ServerName = @stepServerName AND
                Port = @stepPort AND
                DatabaseName = @stepDatabaseName

              -- Previous pending operation also had the same shard location
              -- We need to reconcile the previous operation first.
              IF (@currentShardOperationId IS NOT NULL)
                GOTO Error_ShardPendingOperation;

              -- Another shard with same location already exists.
              IF (@currentShardVersion IS NOT NULL)
                GOTO Error_ShardLocationAlreadyExists;

              -- perform the add/update
              BEGIN TRY
              INSERT INTO
                __ShardManagement.ShardsGlobal (
                  ShardId,
                  Readable,
                  Version,
                  ShardMapId,
                  OperationId,
                  Protocol,
                  ServerName,
                  Port,
                  DatabaseName,
                  Status)
              VALUES (
                @stepShardId,
                0,
                @stepShardVersion,
                @shardMapId,
                @operationId,
                @stepProtocol,
                @stepServerName,
                @stepPort,
                @stepDatabaseName,
                @stepShardStatus)
              END TRY
              BEGIN CATCH
              IF (error_number() = 2627)
                GOTO Error_ShardLocationAlreadyExists;
              ELSE
                BEGIN
                  SET @errorMessage = error_message()
                  SET @errorNumber = error_number()
                  SET @errorSeverity = error_severity()
                  SET @errorState = error_state()
                  SET @errorLine = error_line()
                  SET @errorProcedure = isnull(error_procedure(), '-')

                  SELECT @errorMessage =
                         N'Error %d, Level %d, State %d, Procedure %s, Line %d, Message: ' +
                         @errorMessage;

                  RAISERROR (@errorMessage, @errorSeverity, 2, @errorNumber, @errorSeverity, @errorState, @errorProcedure, @errorLine);

                  ROLLBACK TRANSACTION; -- To avoid extra error message in response.
                  GOTO Error_UnexpectedError;
                END
              END CATCH

              -- reset state for next iteration
              SET @stepProtocol = NULL
              SET @stepServerName = NULL
              SET @stepPort = NULL
              SET @stepDatabaseName = NULL
              SET @stepShardStatus = NULL
            END

        -- reset state for next iteration
        SET @stepType = NULL
        SET @stepShardId = NULL
        SET @stepShardVersion = NULL
        SET @currentShardVersion = NULL
        SET @currentShardOperationId = NULL

        SET @stepIndex = @stepIndex + 1
      END

    GOTO Success_Exit;

    Error_ShardMapNotFound:
    SET @result = 102
    GOTO Exit_Procedure;

    Error_ShardAlreadyExists:
    SET @result = 201
    GOTO Exit_Procedure;

    Error_ShardDoesNotExist:
    SET @result = 202
    GOTO Exit_Procedure;

    Error_ShardHasMappings:
    SET @result = 203
    GOTO Exit_Procedure;

    Error_ShardVersionMismatch:
    SET @result = 204
    GOTO Exit_Procedure;

    Error_ShardLocationAlreadyExists:
    SET @result = 205
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
    EXEC __ShardManagement.spGetOperationLogEntryGlobalHelper @currentShardOperationId
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
-- __ShardManagement.spBulkOperationShardsGlobalEnd
-- Constraints:
---------------------------------------------------------------------------------------------------
CREATE PROCEDURE __ShardManagement.spBulkOperationShardsGlobalEnd
    @input  XML,
    @result INT OUTPUT
AS
  BEGIN
    SET NOCOUNT ON
    DECLARE @gsmVersionClient INT,
    @operationId UNIQUEIDENTIFIER,
    @operationCode INT,
    @undo BIT,
    @stepsCount INT,
    @shardMapId UNIQUEIDENTIFIER

    -- get operation information as well as number of steps
    SELECT
      @gsmVersionClient = x.value('(GsmVersion)[1]', 'int'),
      @operationId = x.value('(@OperationId)[1]', 'uniqueidentifier'),
      @operationCode = x.value('(@OperationCode)[1]', 'int'),
      @undo = x.value('(@Undo)[1]', 'bit'),
      @stepsCount = x.value('(@StepsCount)[1]', 'int'),
      @shardMapId = x.value('(ShardMap/Id)[1]', 'uniqueidentifier')
    FROM
      @input.nodes('/BulkOperationShardsGlobal') AS t(x)

    IF (@gsmVersionClient IS NULL OR @operationId IS NULL OR @operationCode IS NULL OR @undo IS NULL
        OR
        @stepsCount IS NULL OR @shardMapId IS NULL)
      GOTO Error_MissingParameters;

    IF (@gsmVersionClient <> __ShardManagement.fnGetStoreVersionGlobal())
      GOTO Error_GSMVersionMismatch;

    -- check if shard map exists
    IF NOT exists(
        SELECT ShardMapId
        FROM
          __ShardManagement.ShardMapsGlobal
          WITH ( UPDLOCK )
        WHERE
          ShardMapId = @shardMapId)
      GOTO Error_ShardMapNotFound;

    -- Remove/Update/Add specific
    DECLARE @currentStep XML,
    @stepIndex INT = 1,
    @stepType INT,
    @stepShardId UNIQUEIDENTIFIER

    WHILE (@stepIndex <= @stepsCount)
      BEGIN
        SELECT @currentStep = x.query('(./Step[@Id = sql:variable("@stepIndex")])[1]')
        FROM
          @input.nodes('/BulkOperationShardsGlobal/Steps') AS t(x)

        -- Identify the step type.
        SELECT
          @stepType = x.value('(@Kind)[1]', 'int'),
          @stepShardId = x.value('(Shard/Id)[1]', 'uniqueidentifier')
        FROM
          @currentStep.nodes('./Step') AS t(x)

        IF (@stepType IS NULL OR @stepShardId IS NULL)
          GOTO Error_MissingParameters;

        IF (@stepType = 1)
          BEGIN
            IF (@undo = 1)
              BEGIN
                -- keep the Readable row as is
                UPDATE
                  __ShardManagement.ShardsGlobal
                SET
                  OperationId = NULL
                WHERE
                  ShardMapId = @shardMapId AND ShardId = @stepShardId AND OperationId = @operationId
              END
            ELSE
              BEGIN
                -- remove the row to be deleted
                DELETE FROM
                  __ShardManagement.ShardsGlobal
                WHERE
                  ShardMapId = @shardMapId AND ShardId = @stepShardId AND OperationId = @operationId
              END
          END
        ELSE
          IF (@stepType = 2)
            BEGIN
              DECLARE @newShardVersion UNIQUEIDENTIFIER,
              @newStatus INT

              IF (@undo = 1)
                BEGIN
                  -- keep the Readable row as is
                  UPDATE
                    __ShardManagement.ShardsGlobal
                  SET
                    OperationId = NULL
                  WHERE
                    ShardMapId = @shardMapId AND ShardId = @stepShardId AND
                    OperationId = @operationId
                END
              ELSE
                BEGIN
                  -- Update the row with new Version/Status information
                  SELECT
                    @newShardVersion = x.value('(Update/Shard/Version)[1]', 'uniqueidentifier'),
                    @newStatus = x.value('(Update/Shard/Status)[1]', 'int')
                  FROM
                    @currentStep.nodes('./Step') AS t(x)

                  UPDATE
                    __ShardManagement.ShardsGlobal
                  SET
                    Version     = @newShardVersion,
                    Status      = @newStatus,
                    OperationId = NULL
                  WHERE
                    ShardMapId = @shardMapId AND ShardId = @stepShardId AND
                    OperationId = @operationId
                END

              SET @newShardVersion = NULL
              SET @newStatus = NULL
            END
          ELSE
            IF (@stepType = 3)
              BEGIN
                IF (@undo = 1)
                  BEGIN
                    -- remove the row that we tried to add
                    DELETE FROM
                      __ShardManagement.ShardsGlobal
                    WHERE
                      ShardMapId = @shardMapId AND ShardId = @stepShardId AND
                      OperationId = @operationId
                  END
                ELSE
                  BEGIN
                    -- mark the new row Readable
                    UPDATE
                      __ShardManagement.ShardsGlobal
                    SET
                      Readable    = 1,
                      OperationId = NULL
                    WHERE
                      ShardMapId = @shardMapId AND ShardId = @stepShardId AND
                      OperationId = @operationId
                  END
              END

        -- reset state for next iteration
        SET @stepShardId = NULL

        SET @stepIndex = @stepIndex + 1
      END

    -- remove log record
    DELETE FROM
      __ShardManagement.OperationsLogGlobal
    WHERE
      OperationId = @operationId

    SET @result = 1
    GOTO Exit_Procedure;

    Error_ShardMapNotFound:
    SET @result = 102
    GOTO Exit_Procedure;

    Error_MissingParameters:
    SET @result = 50
    EXEC __ShardManagement.spGetStoreVersionGlobalHelper
    GOTO Exit_Procedure;

    Error_GSMVersionMismatch:
    SET @result = 51
    EXEC __ShardManagement.spGetStoreVersionGlobalHelper
    GOTO Exit_Procedure;

    Exit_Procedure:
  END
GO

---------------------------------------------------------------------------------------------------
-- Shard Mappings
---------------------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------------------
-- __ShardManagement.spGetAllShardMappingsGlobal
-- Constraints:
---------------------------------------------------------------------------------------------------
CREATE PROCEDURE __ShardManagement.spGetAllShardMappingsGlobal
    @input  XML,
    @result INT OUTPUT
AS
  BEGIN
    SET NOCOUNT ON
    DECLARE @gsmVersionClient INT,
    @shardMapId UNIQUEIDENTIFIER,
    @shardId UNIQUEIDENTIFIER,
    @shardVersion UNIQUEIDENTIFIER,
    @minValue VARBINARY(128),
    @maxValue VARBINARY(128)

    SELECT
      @gsmVersionClient = x.value('(GsmVersion)[1]', 'int'),
      @shardMapId = x.value('(ShardMap/Id)[1]', 'uniqueidentifier'),
      @shardId = x.value('(Shard[@Null="0"]/Id)[1]', 'uniqueidentifier'),
      @shardVersion = x.value('(Shard[@Null="0"]/Version)[1]', 'uniqueidentifier'),
      @minValue =
      convert(VARBINARY(128), x.value('(Range[@Null="0"]/MinValue)[1]', 'varchar(258)'), 1),
      @maxValue =
      convert(VARBINARY(128), x.value('(Range[@Null="0"]/MaxValue[@Null="0"])[1]', 'varchar(258)'),
              1)
    FROM
      @input.nodes('/GetAllShardMappingsGlobal') AS t(x)

    IF (@gsmVersionClient IS NULL OR @shardMapId IS NULL)
      GOTO Error_MissingParameters;

    IF (@gsmVersionClient > __ShardManagement.fnGetStoreVersionGlobal())
      GOTO Error_GSMVersionMismatch;

    DECLARE @shardMapType INT

    SELECT @shardMapType = ShardMapType
    FROM
      __ShardManagement.ShardMapsGlobal
    WHERE
      ShardMapId = @shardMapId

    IF (@shardMapType IS NULL)
      GOTO Error_ShardMapNotFound;

    DECLARE @currentShardVersion UNIQUEIDENTIFIER

    IF (@shardId IS NOT NULL)
      BEGIN
        IF (@shardVersion IS NULL)
          GOTO Error_MissingParameters;

        SELECT @currentShardVersion = Version
        FROM
          __ShardManagement.ShardsGlobal
        WHERE
          ShardMapId = @shardMapId AND ShardId = @shardId AND Readable = 1

        IF (@currentShardVersion IS NULL)
          GOTO Error_ShardDoesNotExist;

        -- DEVNOTE(wbasheer): Bring this back if we want to be strict.
        --if (@currentShardVersion <> @shardVersion)
        --	goto Error_ShardVersionMismatch;
      END

    DECLARE @tvShards TABLE(
      ShardId      UNIQUEIDENTIFIER                                   NOT NULL,
      Version      UNIQUEIDENTIFIER                                   NOT NULL,
      Protocol     INT                                                NOT NULL,
      ServerName   NVARCHAR(128) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL,
      Port         INT                                                NOT NULL,
      DatabaseName NVARCHAR(128) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL,
      Status       INT                                                NOT NULL,
    PRIMARY KEY (ShardId)
    )

    INSERT INTO
      @tvShards
      SELECT
          ShardId = s.ShardId,
          Version = s.Version,
          Protocol = s.Protocol,
          ServerName = s.ServerName,
          Port = s.Port,
          DatabaseName = s.DatabaseName,
          Status = s.Status
      FROM
        __ShardManagement.ShardsGlobal s
      WHERE
        (@shardId IS NULL OR s.ShardId = @shardId) AND s.ShardMapId = @shardMapId

    DECLARE @minValueCalculated VARBINARY(128) = 0x,
    @maxValueCalculated VARBINARY (128) = NULL

    -- check if range is supplied and update accordingly.
    IF (@minValue IS NOT NULL)
      SET @minValueCalculated = @minValue

    IF (@maxValue IS NOT NULL)
      SET @maxValueCalculated = @maxValue

    IF (@shardMapType = 1)
      BEGIN
        SELECT
          3,
          m.MappingId,
          m.ShardMapId,
          m.MinValue,
          m.MaxValue,
          m.Status,
          m.LockOwnerId,
          -- fields for SqlMapping
          s.ShardId,
          s.Version,
          m.ShardMapId,
          s.Protocol,
          s.ServerName,
          s.Port,
          s.DatabaseName,
          s.Status -- fields for SqlShard, ShardMapId is repeated here
        FROM
          __ShardManagement.ShardMappingsGlobal m
          JOIN
          @tvShards s
            ON
              m.ShardId = s.ShardId
        WHERE
          m.ShardMapId = @shardMapId AND
          m.Readable = 1 AND
          (@shardId IS NULL OR m.ShardId = @shardId) AND
          MinValue >= @minValueCalculated AND
          ((@maxValueCalculated IS NULL) OR (MinValue < @maxValueCalculated))
        ORDER BY
          m.MinValue
      END
    ELSE
      BEGIN
        SELECT
          3,
          m.MappingId,
          m.ShardMapId,
          m.MinValue,
          m.MaxValue,
          m.Status,
          m.LockOwnerId,
          -- fields for SqlMapping
          s.ShardId,
          s.Version,
          m.ShardMapId,
          s.Protocol,
          s.ServerName,
          s.Port,
          s.DatabaseName,
          s.Status -- fields for SqlShard, ShardMapId is repeated here
        FROM
          __ShardManagement.ShardMappingsGlobal m
          JOIN
          @tvShards s
            ON
              m.ShardId = s.ShardId
        WHERE
          m.ShardMapId = @shardMapId AND
          m.Readable = 1 AND
          (@shardId IS NULL OR m.ShardId = @shardId) AND
          ((MaxValue IS NULL) OR (MaxValue > @minValueCalculated)) AND
          ((@maxValueCalculated IS NULL) OR (MinValue < @maxValueCalculated))
        ORDER BY
          m.MinValue
      END

    SET @result = 1
    GOTO Exit_Procedure;

    Error_ShardMapNotFound:
    SET @result = 102
    GOTO Exit_Procedure;

    Error_ShardDoesNotExist:
    SET @result = 202
    GOTO Exit_Procedure;

    -- DEVNOTE(wbasheer): Bring this back if we want to be strict.
    --Error_ShardVersionMismatch:
    --	set @result = 204
    --	goto Exit_Procedure;

    Error_MissingParameters:
    SET @result = 50
    EXEC __ShardManagement.spGetStoreVersionGlobalHelper
    GOTO Exit_Procedure;

    Error_GSMVersionMismatch:
    SET @result = 51
    EXEC __ShardManagement.spGetStoreVersionGlobalHelper
    GOTO Exit_Procedure;

    Exit_Procedure:
  END
GO

---------------------------------------------------------------------------------------------------
-- __ShardManagement.spFindShardMappingByKeyGlobal
-- Constraints:
---------------------------------------------------------------------------------------------------
CREATE PROCEDURE __ShardManagement.spFindShardMappingByKeyGlobal
    @input  XML,
    @result INT OUTPUT
AS
  BEGIN
    DECLARE @gsmVersionClient INT,
    @shardMapId UNIQUEIDENTIFIER,
    @keyValue VARBINARY(128)

    SELECT
      @gsmVersionClient = x.value('(GsmVersion)[1]', 'int'),
      @shardMapId = x.value('(ShardMap/Id)[1]', 'uniqueidentifier'),
      @keyValue = convert(VARBINARY(128), x.value('(Key/Value)[1]', 'varchar(258)'), 1)
    FROM
      @input.nodes('/FindShardMappingByKeyGlobal') AS t(x)

    IF (@gsmVersionClient IS NULL OR @shardMapId IS NULL OR @keyValue IS NULL)
      GOTO Error_MissingParameters;

    IF (@gsmVersionClient > __ShardManagement.fnGetStoreVersionGlobal())
      GOTO Error_GSMVersionMismatch;

    DECLARE @shardMapType INT

    SELECT @shardMapType = ShardMapType
    FROM
      __ShardManagement.ShardMapsGlobal
    WHERE
      ShardMapId = @shardMapId

    IF (@shardMapType IS NULL)
      GOTO Error_ShardMapNotFound;

    DECLARE @currentMappingId UNIQUEIDENTIFIER,
    @currentShardId UNIQUEIDENTIFIER,
    @currentMinValue VARBINARY(128),
    @currentMaxValue VARBINARY(128),
    @currentStatus INT,
    @currentLockOwnerId UNIQUEIDENTIFIER

    IF (@shardMapType = 1)
      BEGIN
        SELECT
          @currentMappingId = MappingId,
          @currentShardId = ShardId,
          @currentMinValue = MinValue,
          @currentMaxValue = MaxValue,
          @currentStatus = Status,
          @currentLockOwnerId = LockOwnerId
        FROM
          __ShardManagement.ShardMappingsGlobal
        WHERE
          ShardMapId = @shardMapId AND
          Readable = 1 AND
          MinValue = @keyValue
      END
    ELSE
      BEGIN
        SELECT
          @currentMappingId = MappingId,
          @currentShardId = ShardId,
          @currentMinValue = MinValue,
          @currentMaxValue = MaxValue,
          @currentStatus = Status,
          @currentLockOwnerId = LockOwnerId
        FROM
          __ShardManagement.ShardMappingsGlobal
        WHERE
          ShardMapId = @shardMapId AND
          Readable = 1 AND
          MinValue <= @keyValue AND (MaxValue IS NULL OR MaxValue > @keyValue)
      END

    IF (@@rowcount = 0)
      GOTO Error_KeyNotFound;

    SELECT
      3,
      @currentMappingId AS MappingId,
      ShardMapId,
      @currentMinValue,
      @currentMaxValue,
      @currentStatus,
      @currentLockOwnerId,
      -- fields for SqlMapping
      ShardId,
      Version,
      ShardMapId,
      Protocol,
      ServerName,
      Port,
      DatabaseName,
      Status -- fields for SqlShard, ShardMapId is repeated here
    FROM
      __ShardManagement.ShardsGlobal
    WHERE
      ShardId = @currentShardId AND
      ShardMapId = @shardMapId

    IF (@@rowcount = 0)
      GOTO Error_KeyNotFound;

    SET @result = 1
    GOTO Exit_Procedure;

    Error_ShardMapNotFound:
    SET @result = 102
    GOTO Exit_Procedure;

    Error_KeyNotFound:
    SET @result = 304
    GOTO Exit_Procedure;

    Error_MissingParameters:
    SET @result = 50
    EXEC __ShardManagement.spGetStoreVersionGlobalHelper
    GOTO Exit_Procedure;

    Error_GSMVersionMismatch:
    SET @result = 51
    EXEC __ShardManagement.spGetStoreVersionGlobalHelper
    GOTO Exit_Procedure;

    Exit_Procedure:
  END
GO

---------------------------------------------------------------------------------------------------
-- __ShardManagement.spFindShardMappingByIdGlobal
-- Constraints:
---------------------------------------------------------------------------------------------------
CREATE PROCEDURE __ShardManagement.spFindShardMappingByIdGlobal
    @input  XML,
    @result INT OUTPUT
AS
  BEGIN
    DECLARE @gsmVersionClient INT,
    @shardMapId UNIQUEIDENTIFIER,
    @mappingId UNIQUEIDENTIFIER

    SELECT
      @gsmVersionClient = x.value('(GsmVersion)[1]', 'int'),
      @shardMapId = x.value('(ShardMap/Id)[1]', 'uniqueidentifier'),
      @mappingId = x.value('(Mapping/Id)[1]', 'uniqueidentifier')
    FROM
      @input.nodes('/FindShardMappingByIdGlobal') AS t(x)

    IF (@gsmVersionClient IS NULL OR @shardMapId IS NULL OR @mappingId IS NULL)
      GOTO Error_MissingParameters;

    IF (@gsmVersionClient > __ShardManagement.fnGetStoreVersionGlobal())
      GOTO Error_GSMVersionMismatch;

    DECLARE @shardMapType INT

    SELECT @shardMapType = ShardMapType
    FROM
      __ShardManagement.ShardMapsGlobal
    WHERE
      ShardMapId = @shardMapId

    IF (@shardMapType IS NULL)
      GOTO Error_ShardMapNotFound;

    DECLARE @currentShardId UNIQUEIDENTIFIER,
    @currentMinValue VARBINARY(128),
    @currentMaxValue VARBINARY(128),
    @currentStatus INT,
    @currentLockOwnerId UNIQUEIDENTIFIER

    -- select just MinValue so only 'ucShardMappingsGlobal_MappingId' will be used
    SELECT @currentMinValue = MinValue
    FROM
      __ShardManagement.ShardMappingsGlobal
    WHERE
      ShardMapId = @shardMapId AND
      Readable = 1 AND
      MappingId = @mappingId

    IF (@@rowcount = 0)
      GOTO Error_MappingDoesNotExist;

    -- now filter using MinValue to use 'pk_tblShardMappingsGlobal_smid_minvalue'
    SELECT
      @currentShardId = ShardId,
      @currentMaxValue = MaxValue,
      @currentStatus = Status,
      @currentLockOwnerId = LockOwnerId
    FROM
      __ShardManagement.ShardMappingsGlobal
    WHERE
      ShardMapId = @shardMapId AND
      MinValue = @currentMinValue

    IF (@@rowcount = 0)
      GOTO Error_MappingDoesNotExist;

    SELECT
      3,
      @mappingId AS MappingId,
      ShardMapId,
      @currentMinValue,
      @currentMaxValue,
      @currentStatus,
      @currentLockOwnerId,
      -- fields for SqlMapping
      ShardId,
      Version,
      ShardMapId,
      Protocol,
      ServerName,
      Port,
      DatabaseName,
      Status -- fields for SqlShard, ShardMapId is repeated here
    FROM
      __ShardManagement.ShardsGlobal
    WHERE
      ShardId = @currentShardId AND
      ShardMapId = @shardMapId

    IF (@@rowcount = 0)
      GOTO Error_MappingDoesNotExist;

    SET @result = 1
    GOTO Exit_Procedure;

    Error_ShardMapNotFound:
    SET @result = 102
    GOTO Exit_Procedure;

    Error_MappingDoesNotExist:
    SET @result = 301
    GOTO Exit_Procedure;

    Error_MissingParameters:
    SET @result = 50
    EXEC __ShardManagement.spGetStoreVersionGlobalHelper
    GOTO Exit_Procedure;

    Error_GSMVersionMismatch:
    SET @result = 51
    EXEC __ShardManagement.spGetStoreVersionGlobalHelper
    GOTO Exit_Procedure;

    Exit_Procedure:
  END
GO

---------------------------------------------------------------------------------------------------
-- __ShardManagement.spBulkOperationShardMappingsGlobalBegin
-- Constraints:
---------------------------------------------------------------------------------------------------
CREATE PROCEDURE __ShardManagement.spBulkOperationShardMappingsGlobalBegin
    @input  XML,
    @result INT OUTPUT
AS
  BEGIN
    SET NOCOUNT ON
    DECLARE @gsmVersionClient INT,
    @operationId UNIQUEIDENTIFIER,
    @operationCode INT,
    @stepsCount INT,
    @shardMapId UNIQUEIDENTIFIER

    -- get operation information as well as number of steps information
    SELECT
      @gsmVersionClient = x.value('(GsmVersion)[1]', 'int'),
      @operationId = x.value('(@OperationId)[1]', 'uniqueidentifier'),
      @operationCode = x.value('(@OperationCode)[1]', 'int'),
      @stepsCount = x.value('(@StepsCount)[1]', 'int'),
      @shardMapId = x.value('(ShardMap/Id)[1]', 'uniqueidentifier')
    FROM
      @input.nodes('/BulkOperationShardMappingsGlobal') AS t(x)

    IF (@gsmVersionClient IS NULL OR @operationId IS NULL OR @operationCode IS NULL OR
        @stepsCount IS NULL OR @shardMapId IS NULL)
      GOTO Error_MissingParameters;

    IF (@gsmVersionClient <> __ShardManagement.fnGetStoreVersionGlobal())
      GOTO Error_GSMVersionMismatch;

    -- check if shard map exists
    DECLARE @shardMapType INT

    SELECT @shardMapType = ShardMapType
    FROM
      __ShardManagement.ShardMapsGlobal
      WITH ( UPDLOCK )
    WHERE
      ShardMapId = @shardMapId

    IF (@shardMapType IS NULL)
      GOTO Error_ShardMapNotFound;

    DECLARE @shardIdForRemoves UNIQUEIDENTIFIER,
    @originalShardVersionForRemoves UNIQUEIDENTIFIER,
    @shardIdForAdds UNIQUEIDENTIFIER,
    @originalShardVersionForAdds UNIQUEIDENTIFIER,
    @currentShardOperationId UNIQUEIDENTIFIER

    SELECT
      @shardIdForRemoves = x.value('(Removes/Shard/Id)[1]', 'uniqueidentifier'),
      @shardIdForAdds = x.value('(Adds/Shard/Id)[1]', 'uniqueidentifier')
    FROM
      @input.nodes('/BulkOperationShardMappingsGlobal') AS t(x)

    IF (@shardIdForRemoves IS NULL OR @shardIdForAdds IS NULL)
      GOTO Error_MissingParameters;

    -- Check re-entrancy or pending operations
    SELECT
      @originalShardVersionForRemoves = Version,
      @currentShardOperationId = OperationId
    FROM
      __ShardManagement.ShardsGlobal
      WITH ( UPDLOCK )
    WHERE
      ShardMapId = @shardMapId AND ShardId = @shardIdForRemoves AND Readable = 1

    -- re-entrancy
    IF (@currentShardOperationId = @operationId)
      GOTO Success_Exit;

    -- pending operations
    IF (@currentShardOperationId IS NOT NULL)
      GOTO Error_ShardPendingOperation;

    IF (@originalShardVersionForRemoves IS NULL)
      GOTO Error_ShardDoesNotExist;

    -- mark the source shard for update
    UPDATE __ShardManagement.ShardsGlobal
    SET
      OperationId = @operationId
    WHERE
      ShardMapId = @shardMapId AND ShardId = @shardIdForRemoves

    SET @currentShardOperationId = NULL;

    IF (@shardIdForRemoves <> @shardIdForAdds)
      BEGIN
        -- Check re-entrancy or pending operations
        SELECT
          @originalShardVersionForAdds = Version,
          @currentShardOperationId = OperationId
        FROM
          __ShardManagement.ShardsGlobal
          WITH ( UPDLOCK )
        WHERE
          ShardMapId = @shardMapId AND ShardId = @shardIdForAdds AND Readable = 1

        -- re-entrancy
        IF (@currentShardOperationId = @operationId)
          GOTO Success_Exit;

        -- pending operations
        IF (@currentShardOperationId IS NOT NULL)
          GOTO Error_ShardPendingOperation;

        IF (@originalShardVersionForAdds IS NULL)
          GOTO Error_ShardDoesNotExist;

        -- mark the target shard for update
        UPDATE __ShardManagement.ShardsGlobal
        SET
          OperationId = @operationId
        WHERE
          ShardMapId = @shardMapId AND ShardId = @shardIdForAdds
      END
    ELSE
      BEGIN
        SET @originalShardVersionForAdds = @originalShardVersionForRemoves
      END

    -- add log record
    BEGIN TRY
    INSERT INTO __ShardManagement.OperationsLogGlobal (
      OperationId,
      OperationCode,
      Data,
      ShardVersionRemoves,
      ShardVersionAdds)
    VALUES (
      @operationId,
      @operationCode,
      @input,
      @originalShardVersionForRemoves,
      @originalShardVersionForAdds)
    END TRY
    BEGIN CATCH
    -- if log record already exists, ignore
    IF (error_number() <> 2627)
      BEGIN
        DECLARE @errorMessage NVARCHAR(MAX) = error_message(),
        @errorNumber INT = error_number(),
        @errorSeverity INT = error_severity(),
        @errorState INT = error_state(),
        @errorLine INT = error_line(),
        @errorProcedure NVARCHAR(128) = isnull(error_procedure(), '-');

        SELECT @errorMessage =
               N'Error %d, Level %d, State %d, Procedure %s, Line %d, Message: ' + @errorMessage;

        RAISERROR (@errorMessage, @errorSeverity, 1, @errorNumber, @errorSeverity, @errorState, @errorProcedure, @errorLine);

        ROLLBACK TRANSACTION; -- To avoid extra error message in response.
        GOTO Error_UnexpectedError;
      END
    END CATCH

    DECLARE @currentStep XML,
    @stepIndex INT = 1,
    @stepType INT,
    @stepMappingId UNIQUEIDENTIFIER,
    @stepLockOwnerId UNIQUEIDENTIFIER

    -- Remove/Update
    DECLARE @currentLockOwnerId UNIQUEIDENTIFIER,
    @currentStatus INT

    -- Update/Add
    DECLARE @stepStatus INT,
    @stepShouldValidate BIT,
    @stepMinValue VARBINARY(128),
    @stepMaxValue VARBINARY(128),
    @mappingIdFromValidate UNIQUEIDENTIFIER

    WHILE (@stepIndex <= @stepsCount)
      BEGIN
        SELECT @currentStep = x.query('(./Step[@Id = sql:variable("@stepIndex")])[1]')
        FROM
          @input.nodes('/BulkOperationShardMappingsGlobal/Steps') AS t(x)

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

            -- Check for locks
            SELECT @stepLockOwnerId = x.value('(Lock/Id)[1]', 'uniqueidentifier')
            FROM
              @currentStep.nodes('./Step') AS t(x)

            IF (@stepLockOwnerId IS NULL)
              GOTO Error_MissingParameters;

            SELECT
              @currentLockOwnerId = LockOwnerId,
              @currentStatus = Status
            FROM
              __ShardManagement.ShardMappingsGlobal
              WITH ( UPDLOCK )
            WHERE
              ShardMapId = @shardMapId AND MappingId = @stepMappingId AND Readable = 1

            IF (@currentLockOwnerId IS NULL)
              GOTO Error_MappingDoesNotExist;

            IF (@currentLockOwnerId <> @stepLockOwnerId)
              GOTO Error_MappingLockOwnerIdMismatch;

            -- removepoint/removerange/removerangefromrange cannot work on online mappings
            IF ((@currentStatus & 1) <> 0 AND
                (@operationCode = 5 OR
                 @operationCode = 9 OR
                 @operationCode = 13))
              GOTO Error_MappingIsNotOffline;

            -- mark pending operation on current mapping
            UPDATE
              __ShardManagement.ShardMappingsGlobal
            SET
              OperationId = @operationId
            WHERE
              ShardMapId = @shardMapId AND MappingId = @stepMappingId

            -- reset state for next iteration
            SET @currentLockOwnerId = NULL
            SET @currentStatus = NULL
          END
        ELSE
          IF (@stepType = 2)
            BEGIN
              -- UpdateMapping

              -- Check for locks
              SELECT
                @stepLockOwnerId = x.value('(Lock/Id)[1]', 'uniqueidentifier'),
                @stepStatus = x.value('(Update/Mapping/Status)[1]', 'int')
              FROM
                @currentStep.nodes('./Step') AS t(x)

              IF (@stepLockOwnerId IS NULL OR @stepStatus IS NULL)
                GOTO Error_MissingParameters;

              SELECT
                @currentLockOwnerId = LockOwnerId,
                @currentStatus = Status
              FROM
                __ShardManagement.ShardMappingsGlobal
                WITH ( UPDLOCK )
              WHERE
                ShardMapId = @shardMapId AND MappingId = @stepMappingId AND Readable = 1

              IF (@currentLockOwnerId IS NULL)
                GOTO Error_MappingDoesNotExist;

              IF (@currentLockOwnerId <> @stepLockOwnerId)
                GOTO Error_MappingLockOwnerIdMismatch;

              -- online -> online and location change is not allowed
              IF ((@currentStatus & 1) = 1 AND (@stepStatus & 1) = 1 AND
                  @shardIdForRemoves <> @shardIdForAdds)
                GOTO Error_MappingIsNotOffline;

              -- mark pending operation on current mapping
              UPDATE
                __ShardManagement.ShardMappingsGlobal
              SET
                OperationId = @operationId
              WHERE
                ShardMapId = @shardMapId AND MappingId = @stepMappingId

              -- reset state for next iteration
              SET @currentLockOwnerId = NULL
              SET @currentStatus = NULL

              SET @stepStatus = NULL
            END
          ELSE
            IF (@stepType = 3)
              BEGIN
                -- AddMapping
                SELECT
                  @stepShouldValidate = x.value('(@Validate)[1]', 'bit'),
                  @stepMappingId = x.value('(Mapping/Id)[1]', 'uniqueidentifier'),
                  @stepMinValue =
                  convert(VARBINARY(128), x.value('(Mapping/MinValue)[1]', 'varchar(258)'), 1),
                  @stepMaxValue =
                  convert(VARBINARY(128),
                          x.value('(Mapping/MaxValue[@Null="0"])[1]', 'varchar(258)'), 1),
                  @stepStatus = x.value('(Mapping/Status)[1]', 'int'),
                  @stepLockOwnerId = x.value('(Mapping/LockOwnerId)[1]', 'uniqueidentifier')
                FROM
                  @currentStep.nodes('./Step') AS t(x)

                IF (
                  @stepShouldValidate IS NULL OR @stepMappingId IS NULL OR @stepMinValue IS NULL OR
                  @stepStatus IS NULL
                  OR @stepLockOwnerId IS NULL)
                  GOTO Error_MissingParameters;

                -- if validation requested
                IF (@stepShouldValidate = 1)
                  BEGIN
                    IF (@shardMapType = 1)
                      BEGIN
                        SELECT
                          @mappingIdFromValidate = MappingId,
                          @currentShardOperationId = OperationId
                        FROM
                          __ShardManagement.ShardMappingsGlobal
                        WHERE
                          ShardMapId = @shardMapId AND
                          MinValue = @stepMinValue

                        IF (@mappingIdFromValidate IS NOT NULL)
                          BEGIN
                            IF (@currentShardOperationId IS NULL OR
                                @currentShardOperationId = @operationId)
                              GOTO Error_PointAlreadyMapped;
                            ELSE
                              GOTO Error_ShardPendingOperation;
                          END
                      END
                    ELSE
                      BEGIN
                        SELECT
                          @mappingIdFromValidate = MappingId,
                          @currentShardOperationId = OperationId
                        FROM
                          __ShardManagement.ShardMappingsGlobal
                        WHERE
                          ShardMapId = @shardMapId AND
                          (MaxValue IS NULL OR MaxValue > @stepMinValue) AND
                          (@stepMaxValue IS NULL OR MinValue < @stepMaxValue)

                        IF (@mappingIdFromValidate IS NOT NULL)
                          BEGIN
                            IF (@currentShardOperationId IS NULL OR
                                @currentShardOperationId = @operationId)
                              GOTO Error_RangeAlreadyMapped;
                            ELSE
                              GOTO Error_ShardPendingOperation;
                          END
                      END
                  END

                -- add mapping
                INSERT INTO
                  __ShardManagement.ShardMappingsGlobal (
                    MappingId,
                    Readable,
                    ShardId,
                    ShardMapId,
                    OperationId,
                    MinValue,
                    MaxValue,
                    Status,
                    LockOwnerId)
                VALUES (
                  @stepMappingId,
                  0,
                  @shardIdForAdds,
                  @shardMapId,
                  @operationId,
                  @stepMinValue,
                  @stepMaxValue,
                  @stepStatus,
                  @stepLockOwnerId)

                -- reset state for next iteration
                SET @stepStatus = NULL

                SET @stepShouldValidate = NULL
                SET @stepMinValue = NULL
                SET @stepMaxValue = NULL
                SET @mappingIdFromValidate = NULL
              END

        -- reset state for next iteration
        SET @stepType = NULL
        SET @stepMappingId = NULL
        SET @stepLockOwnerId = NULL

        SET @stepIndex = @stepIndex + 1
      END

    GOTO Success_Exit;

    Error_ShardMapNotFound:
    SET @result = 102
    GOTO Exit_Procedure;

    Error_ShardDoesNotExist:
    SET @result = 202
    GOTO Exit_Procedure;

    Error_MappingDoesNotExist:
    SET @result = 301
    GOTO Exit_Procedure;

    Error_RangeAlreadyMapped:
    SET @result = 302
    GOTO Exit_Procedure;

    Error_PointAlreadyMapped:
    SET @result = 303
    GOTO Exit_Procedure;

    Error_MappingLockOwnerIdMismatch:
    SET @result = 307
    GOTO Exit_Procedure;

    Error_MappingIsNotOffline:
    SET @result = 306
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
    EXEC __ShardManagement.spGetOperationLogEntryGlobalHelper @currentShardOperationId
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
-- __ShardManagement.spBulkOperationShardMappingsGlobalEnd
-- Constraints:
---------------------------------------------------------------------------------------------------
CREATE PROCEDURE __ShardManagement.spBulkOperationShardMappingsGlobalEnd
    @input  XML,
    @result INT OUTPUT
AS
  BEGIN
    SET NOCOUNT ON
    DECLARE @gsmVersionClient INT,
    @operationId UNIQUEIDENTIFIER,
    @operationCode INT,
    @undo INT,
    @stepsCount INT,
    @shardMapId UNIQUEIDENTIFIER

    -- get operation information as well as number of steps information
    SELECT
      @gsmVersionClient = x.value('(GsmVersion)[1]', 'int'),
      @operationId = x.value('(@OperationId)[1]', 'uniqueidentifier'),
      @operationCode = x.value('(@OperationCode)[1]', 'int'),
      @undo = x.value('(@Undo)[1]', 'int'),
      @stepsCount = x.value('(@StepsCount)[1]', 'int'),
      @shardMapId = x.value('(ShardMap/Id)[1]', 'uniqueidentifier')
    FROM
      @input.nodes('/BulkOperationShardMappingsGlobal') AS t(x)

    IF (@gsmVersionClient IS NULL OR @operationId IS NULL OR @operationCode IS NULL OR @undo IS NULL
        OR
        @stepsCount IS NULL OR @shardMapId IS NULL)
      GOTO Error_MissingParameters;

    IF (@gsmVersionClient <> __ShardManagement.fnGetStoreVersionGlobal())
      GOTO Error_GSMVersionMismatch;

    -- check if shard map exists
    IF NOT exists(
        SELECT ShardMapId
        FROM
          __ShardManagement.ShardMapsGlobal
          WITH ( UPDLOCK )
        WHERE
          ShardMapId = @shardMapId)
      GOTO Error_ShardMapNotFound;

    DECLARE @shardIdForRemoves UNIQUEIDENTIFIER,
    @shardVersionForRemoves UNIQUEIDENTIFIER,
    @shardIdForAdds UNIQUEIDENTIFIER,
    @shardVersionForAdds UNIQUEIDENTIFIER

    SELECT
      @shardIdForRemoves = x.value('(Removes/Shard/Id)[1]', 'uniqueidentifier'),
      @shardIdForAdds = x.value('(Adds/Shard/Id)[1]', 'uniqueidentifier'),
      @shardVersionForRemoves = x.value('(Removes/Shard/Version)[1]', 'uniqueidentifier'),
      @shardVersionForAdds = x.value('(Adds/Shard/Version)[1]', 'uniqueidentifier')
    FROM
      @input.nodes('/BulkOperationShardMappingsGlobal') AS t(x)

    IF (@shardIdForRemoves IS NULL OR @shardIdForAdds IS NULL OR @shardVersionForRemoves IS NULL OR
        @shardVersionForAdds IS NULL)
      GOTO Error_MissingParameters;

    -- perform shard updates
    IF (@undo = 1)
      BEGIN
        -- Unmark the pending operation
        UPDATE
          __ShardManagement.ShardsGlobal
        SET
          OperationId = NULL
        WHERE
          ShardMapId = @shardMapId AND ShardId = @shardIdForRemoves

        IF (@shardIdForRemoves <> @shardIdForAdds)
          BEGIN
            UPDATE
              __ShardManagement.ShardsGlobal
            SET
              OperationId = NULL
            WHERE
              ShardMapId = @shardMapId AND ShardId = @shardIdForAdds
          END
      END
    ELSE
      BEGIN
        -- update the source shard row with new Version
        UPDATE
          __ShardManagement.ShardsGlobal
        SET
          Version     = @shardVersionForRemoves,
          OperationId = NULL
        WHERE
          ShardMapId = @shardMapId AND ShardId = @shardIdForRemoves

        -- update the target shard row with new Version
        IF (@shardIdForRemoves <> @shardIdForAdds)
          BEGIN
            UPDATE
              __ShardManagement.ShardsGlobal
            SET
              Version     = @shardVersionForAdds,
              OperationId = NULL
            WHERE
              ShardMapId = @shardMapId AND ShardId = @shardIdForAdds
          END
      END

    -- Remove/Update/Add specific
    DECLARE @currentStep XML,
    @stepIndex INT = 1,
    @stepType INT,
    @stepMappingId UNIQUEIDENTIFIER

    WHILE (@stepIndex <= @stepsCount)
      BEGIN
        SELECT @currentStep = x.query('(./Step[@Id = sql:variable("@stepIndex")])[1]')
        FROM
          @input.nodes('/BulkOperationShardMappingsGlobal/Steps') AS t(x)

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
            IF (@undo = 1)
              BEGIN
                -- keep the Readable row as is
                UPDATE
                  __ShardManagement.ShardMappingsGlobal
                SET
                  OperationId = NULL
                WHERE
                  ShardMapId = @shardMapId AND MappingId = @stepMappingId
              END
            ELSE
              BEGIN
                -- remove the row to be deleted
                DELETE FROM
                  __ShardManagement.ShardMappingsGlobal
                WHERE
                  ShardMapId = @shardMapId AND MappingId = @stepMappingId
              END
          END
        ELSE
          IF (@stepType = 2)
            BEGIN
              DECLARE @newMappingId UNIQUEIDENTIFIER,
              @newMappingStatus INT

              IF (@undo = 1)
                BEGIN
                  -- keep the Readable row as is
                  UPDATE
                    __ShardManagement.ShardMappingsGlobal
                  SET
                    OperationId = NULL
                  WHERE
                    ShardMapId = @shardMapId AND MappingId = @stepMappingId
                END
              ELSE
                BEGIN
                  -- Update the row with new Version/Status information
                  SELECT
                    @newMappingId = x.value('(Update/Mapping/Id)[1]', 'uniqueidentifier'),
                    @newMappingStatus = x.value('(Update/Mapping/Status)[1]', 'int')
                  FROM
                    @currentStep.nodes('./Step') AS t(x)

                  UPDATE
                    __ShardManagement.ShardMappingsGlobal
                  SET
                    MappingId   = @newMappingId,
                    ShardId     = @shardIdForAdds,
                    Status      = @newMappingStatus,
                    OperationId = NULL
                  WHERE
                    ShardMapId = @shardMapId AND MappingId = @stepMappingId
                END

              SET @newMappingId = NULL
              SET @newMappingStatus = NULL
            END
          ELSE
            IF (@stepType = 3)
              BEGIN
                IF (@undo = 1)
                  BEGIN
                    -- remove the row that we tried to add
                    DELETE FROM
                      __ShardManagement.ShardMappingsGlobal
                    WHERE
                      ShardMapId = @shardMapId AND MappingId = @stepMappingId
                  END
                ELSE
                  BEGIN
                    -- mark the new row Readable
                    UPDATE
                      __ShardManagement.ShardMappingsGlobal
                    SET
                      Readable    = 1,
                      OperationId = NULL
                    WHERE
                      ShardMapId = @shardMapId AND MappingId = @stepMappingId
                  END
              END

        -- reset state for next iteration
        SET @stepMappingId = NULL

        SET @stepIndex = @stepIndex + 1
      END

    -- delete log record
    DELETE FROM
      __ShardManagement.OperationsLogGlobal
    WHERE
      OperationId = @operationId

    SET @result = 1
    GOTO Exit_Procedure;

    Error_ShardMapNotFound:
    SET @result = 102
    GOTO Exit_Procedure;

    Error_MissingParameters:
    SET @result = 50
    EXEC __ShardManagement.spGetStoreVersionGlobalHelper
    GOTO Exit_Procedure;

    Error_GSMVersionMismatch:
    SET @result = 51
    EXEC __ShardManagement.spGetStoreVersionGlobalHelper
    GOTO Exit_Procedure;

    Exit_Procedure:
  END
GO

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
    DECLARE @gsmVersionClient INT,
    @shardMapId UNIQUEIDENTIFIER,
    @mappingId UNIQUEIDENTIFIER,
    @lockOwnerId UNIQUEIDENTIFIER,
    @lockOperationType INT

    SELECT
      @gsmVersionClient = x.value('(GsmVersion)[1]', 'int'),
      @shardMapId = x.value('(ShardMap/Id)[1]', 'uniqueidentifier'),
      @mappingId = x.value('(Mapping/Id)[1]', 'uniqueidentifier'),
      @lockOwnerId = x.value('(Lock/Id)[1]', 'uniqueidentifier'),
      @lockOperationType = x.value('(Lock/Operation)[1]', 'int')
    FROM
      @input.nodes('/LockOrUnlockShardMappingsGlobal') AS t(x)

    IF (@gsmVersionClient IS NULL OR @shardMapId IS NULL OR @lockOwnerId IS NULL OR
        @lockOperationType IS NULL)
      GOTO Error_MissingParameters;

    IF (@gsmVersionClient <> __ShardManagement.fnGetStoreVersionGlobal())
      GOTO Error_GSMVersionMismatch;

    IF (@lockOperationType <> 2 AND @mappingId IS NULL)
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

    IF (@lockOperationType <> 2)
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
                      @lockOperationType = 1 OR @lockOperationType = 2
                      THEN
                        @DefaultLockOwnerId
                    END
    WHERE
      ShardMapId = @shardMapId AND (@lockOperationType = 2 OR MappingId = @mappingId)

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

---------------------------------------------------------------------------------------------------
-- Schema Info
---------------------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------------------
-- __ShardManagement.spGetAllShardingSchemaInfosGlobal
---------------------------------------------------------------------------------------------------
CREATE PROCEDURE __ShardManagement.spGetAllShardingSchemaInfosGlobal
    @input  XML,
    @result INT OUTPUT
AS
  BEGIN
    DECLARE @gsmVersionClient INT

    SELECT @gsmVersionClient = x.value('(GsmVersion)[1]', 'int')
    FROM
      @input.nodes('/GetAllShardingSchemaInfosGlobal') AS t(x)

    IF (@gsmVersionClient IS NULL)
      GOTO Error_MissingParameters;

    IF (@gsmVersionClient > __ShardManagement.fnGetStoreVersionGlobal())
      GOTO Error_GSMVersionMismatch;

    SELECT
      7,
      Name,
      SchemaInfo
    FROM
      __ShardManagement.ShardedDatabaseSchemaInfosGlobal

    SET @result = 1
    GOTO Exit_Procedure;

    Error_MissingParameters:
    SET @result = 50
    EXEC __ShardManagement.spGetStoreVersionGlobalHelper
    GOTO Exit_Procedure;

    Error_GSMVersionMismatch:
    SET @result = 51
    EXEC __ShardManagement.spGetStoreVersionGlobalHelper
    GOTO Exit_Procedure;

    Exit_Procedure:
  END
GO

---------------------------------------------------------------------------------------------------
-- __ShardManagement.spFindShardingSchemaInfoByNameGlobal
---------------------------------------------------------------------------------------------------
CREATE PROCEDURE __ShardManagement.spFindShardingSchemaInfoByNameGlobal
    @input  XML,
    @result INT OUTPUT
AS
  BEGIN
    DECLARE @gsmVersionClient INT,
    @name NVARCHAR(128)

    SELECT
      @gsmVersionClient = x.value('(GsmVersion)[1]', 'int'),
      @name = x.value('(SchemaInfo/Name)[1]', 'nvarchar(128)')
    FROM
      @input.nodes('/FindShardingSchemaInfoGlobal') AS t(x)

    IF (@gsmVersionClient IS NULL OR @name IS NULL)
      GOTO Error_MissingParameters;

    IF (@gsmVersionClient > __ShardManagement.fnGetStoreVersionGlobal())
      GOTO Error_GSMVersionMismatch;

    SELECT
      7,
      Name,
      SchemaInfo
    FROM
      __ShardManagement.ShardedDatabaseSchemaInfosGlobal
    WHERE
      Name = @name

    IF (@@rowcount = 0)
      GOTO Error_SchemaInfoNameDoesNotExist;

    SET @result = 1
    GOTO Exit_Procedure;

    Error_SchemaInfoNameDoesNotExist:
    SET @result = 401
    GOTO Exit_Procedure;

    Error_MissingParameters:
    SET @result = 50
    EXEC __ShardManagement.spGetStoreVersionGlobalHelper
    GOTO Exit_Procedure;

    Error_GSMVersionMismatch:
    SET @result = 51
    EXEC __ShardManagement.spGetStoreVersionGlobalHelper
    GOTO Exit_Procedure;

    Exit_Procedure:
  END
GO

---------------------------------------------------------------------------------------------------
-- __ShardManagement.spAddShardingSchemaInfoGlobal
---------------------------------------------------------------------------------------------------
CREATE PROCEDURE __ShardManagement.spAddShardingSchemaInfoGlobal
    @input  XML,
    @result INT OUTPUT
AS
  BEGIN
    DECLARE @gsmVersionClient INT,
    @name NVARCHAR(128),
    @schemaInfo XML

    SELECT
      @gsmVersionClient = x.value('(GsmVersion)[1]', 'int'),
      @name = x.value('(SchemaInfo/Name)[1]', 'nvarchar(128)'),
      @schemaInfo = x.query('SchemaInfo/Info/*')
    FROM
      @input.nodes('/AddShardingSchemaInfoGlobal') AS t(x)

    IF (@gsmVersionClient IS NULL OR @name IS NULL OR @schemaInfo IS NULL)
      GOTO Error_MissingParameters;

    IF (@gsmVersionClient <> __ShardManagement.fnGetStoreVersionGlobal())
      GOTO Error_GSMVersionMismatch;

    IF exists(
        SELECT Name
        FROM
          __ShardManagement.ShardedDatabaseSchemaInfosGlobal
        WHERE
          Name = @name)
      GOTO Error_SchemaInfoAlreadyExists;

    INSERT INTO
      __ShardManagement.ShardedDatabaseSchemaInfosGlobal
      (Name, SchemaInfo)
    VALUES
      (@name, @schemaInfo)

    SET @result = 1
    GOTO Exit_Procedure;

    Error_SchemaInfoAlreadyExists:
    SET @result = 402
    GOTO Exit_Procedure;

    Error_MissingParameters:
    SET @result = 50
    EXEC __ShardManagement.spGetStoreVersionGlobalHelper
    GOTO Exit_Procedure;

    Error_GSMVersionMismatch:
    SET @result = 51
    EXEC __ShardManagement.spGetStoreVersionGlobalHelper
    GOTO Exit_Procedure;

    Exit_Procedure:
  END
GO

---------------------------------------------------------------------------------------------------
-- __ShardManagement.spRemoveShardingSchemaInfoGlobal
---------------------------------------------------------------------------------------------------
CREATE PROCEDURE __ShardManagement.spRemoveShardingSchemaInfoGlobal
    @input  XML,
    @result INT OUTPUT
AS
  BEGIN
    DECLARE @gsmVersionClient INT,
    @name NVARCHAR(128)

    SELECT
      @gsmVersionClient = x.value('(GsmVersion)[1]', 'int'),
      @name = x.value('(SchemaInfo/Name)[1]', 'nvarchar(128)')
    FROM
      @input.nodes('/RemoveShardingSchemaInfoGlobal') AS t(x)

    IF (@gsmVersionClient IS NULL OR @name IS NULL)
      GOTO Error_MissingParameters;

    IF (@gsmVersionClient <> __ShardManagement.fnGetStoreVersionGlobal())
      GOTO Error_GSMVersionMismatch;

    DELETE FROM
      __ShardManagement.ShardedDatabaseSchemaInfosGlobal
    WHERE
      Name = @name

    IF (@@rowcount = 0)
      GOTO Error_SchemaInfoNameDoesNotExist;

    SET @result = 1
    GOTO Exit_Procedure;

    Error_SchemaInfoNameDoesNotExist:
    SET @result = 401
    GOTO Exit_Procedure;

    Error_MissingParameters:
    SET @result = 50
    EXEC __ShardManagement.spGetStoreVersionGlobalHelper
    GOTO Exit_Procedure;

    Error_GSMVersionMismatch:
    SET @result = 51
    EXEC __ShardManagement.spGetStoreVersionGlobalHelper
    GOTO Exit_Procedure;

    Exit_Procedure:
  END
GO

---------------------------------------------------------------------------------------------------
-- __ShardManagement.spUpdateShardingSchemaInfoGlobal
---------------------------------------------------------------------------------------------------
CREATE PROCEDURE __ShardManagement.spUpdateShardingSchemaInfoGlobal
    @input  XML,
    @result INT OUTPUT
AS
  BEGIN
    DECLARE @gsmVersionClient INT,
    @name NVARCHAR(128),
    @schemaInfo XML

    SELECT
      @gsmVersionClient = x.value('(GsmVersion)[1]', 'int'),
      @name = x.value('(SchemaInfo/Name)[1]', 'nvarchar(128)'),
      @schemaInfo = x.query('SchemaInfo/Info/*')
    FROM
      @input.nodes('/UpdateShardingSchemaInfoGlobal') AS t(x)

    IF (@gsmVersionClient IS NULL OR @name IS NULL OR @schemaInfo IS NULL)
      GOTO Error_MissingParameters;

    IF (@gsmVersionClient <> __ShardManagement.fnGetStoreVersionGlobal())
      GOTO Error_GSMVersionMismatch;

    UPDATE
      __ShardManagement.ShardedDatabaseSchemaInfosGlobal
    SET
      SchemaInfo = @schemaInfo
    WHERE
      Name = @name

    IF (@@rowcount = 0)
      GOTO Error_SchemaInfoNameDoesNotExist;

    SET @result = 1
    GOTO Exit_Procedure;

    Error_SchemaInfoNameDoesNotExist:
    SET @result = 401
    GOTO Exit_Procedure;

    Error_MissingParameters:
    SET @result = 50
    EXEC __ShardManagement.spGetStoreVersionGlobalHelper
    GOTO Exit_Procedure;

    Error_GSMVersionMismatch:
    SET @result = 51
    EXEC __ShardManagement.spGetStoreVersionGlobalHelper
    GOTO Exit_Procedure;

    Exit_Procedure:
  END
GO

---------------------------------------------------------------------------------------------------
-- Recovery
---------------------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------------------
-- __ShardManagement.spAttachShardGlobal
-- Constraints:
---------------------------------------------------------------------------------------------------
CREATE PROCEDURE __ShardManagement.spAttachShardGlobal
    @input  XML,
    @result INT OUTPUT
AS
  BEGIN
    DECLARE @gsmVersionClient INT,
    @shardMapId UNIQUEIDENTIFIER,
    @name NVARCHAR(50),
    @mapType INT,
    @keyType INT,
    @shardId UNIQUEIDENTIFIER,
    @shardVersion UNIQUEIDENTIFIER,
    @protocol INT,
    @serverName NVARCHAR(128),
    @port INT,
    @databaseName NVARCHAR(128),
    @shardStatus INT

    SELECT
      @gsmVersionClient = x.value('(GsmVersion)[1]', 'int'),
      @shardMapId = x.value('(ShardMap/Id)[1]', 'uniqueidentifier'),
      @name = x.value('(ShardMap/Name)[1]', 'nvarchar(50)'),
      @mapType = x.value('(ShardMap/Kind)[1]', 'int'),
      @keyType = x.value('(ShardMap/KeyKind)[1]', 'int'),

      @shardId = x.value('(Shard/Id)[1]', 'uniqueidentifier'),
      @shardVersion = x.value('(Shard/Version)[1]', 'uniqueidentifier'),
      @protocol = x.value('(Shard/Location/Protocol)[1]', 'int'),
      @serverName = x.value('(Shard/Location/ServerName)[1]', 'nvarchar(128)'),
      @port = x.value('(Shard/Location/Port)[1]', 'int'),
      @databaseName = x.value('(Shard/Location/DatabaseName)[1]', 'nvarchar(128)'),
      @shardStatus = x.value('(Shard/Status)[1]', 'int')
    FROM
      @input.nodes('/AttachShardGlobal') AS t(x)

    IF (@gsmVersionClient IS NULL OR @shardMapId IS NULL OR @name IS NULL OR @mapType IS NULL OR
        @keyType IS NULL OR
        @shardId IS NULL OR @shardVersion IS NULL OR @protocol IS NULL OR @serverName IS NULL OR
        @port IS NULL OR @databaseName IS NULL OR @shardStatus IS NULL)
      GOTO Error_MissingParameters;

    IF (@gsmVersionClient <> __ShardManagement.fnGetStoreVersionGlobal())
      GOTO Error_GSMVersionMismatch;

    IF exists(
        SELECT ShardMapId
        FROM
          __ShardManagement.ShardMapsGlobal
        WHERE
          (ShardMapId = @shardMapId AND Name <> @name) OR
          (ShardMapId <> @shardMapId AND Name = @name))
      GOTO Error_ShardMapAlreadyExists;

    -- ignore duplicate shard maps
    BEGIN TRY
    INSERT INTO
      __ShardManagement.ShardMapsGlobal
      (ShardMapId, Name, ShardMapType, KeyType)
    VALUES
      (@shardMapId, @name, @mapType, @keyType)
    END TRY
    BEGIN CATCH
    IF (error_number() <> 2627)
      BEGIN
        DECLARE @errorMessage NVARCHAR(MAX) = error_message(),
        @errorNumber INT = error_number(),
        @errorSeverity INT = error_severity(),
        @errorState INT = error_state(),
        @errorLine INT = error_line(),
        @errorProcedure NVARCHAR(128) = isnull(error_procedure(), '-');

        SELECT @errorMessage =
               N'Error %d, Level %d, State %d, Procedure %s, Line %d, Message: ' + @errorMessage;

        RAISERROR (@errorMessage, @errorSeverity, 1, @errorNumber, @errorSeverity, @errorState, @errorProcedure, @errorLine);

        ROLLBACK TRANSACTION; -- To avoid extra error message in response.
        GOTO Error_UnexpectedError;
      END
    END CATCH

    -- attempt to add the shard
    BEGIN TRY
    INSERT INTO
      __ShardManagement.ShardsGlobal (
        ShardId,
        Readable,
        Version,
        ShardMapId,
        OperationId,
        Protocol,
        ServerName,
        Port,
        DatabaseName,
        Status)
    VALUES (
      @shardId,
      1,
      @shardVersion,
      @shardMapId,
      NULL,
      @protocol,
      @serverName,
      @port,
      @databaseName,
      @shardStatus)
    END TRY
    BEGIN CATCH
    IF (error_number() = 2627)
      GOTO Error_ShardLocationAlreadyExists;
    ELSE
      BEGIN
        SET @errorMessage = error_message()
        SET @errorNumber = error_number()
        SET @errorSeverity = error_severity()
        SET @errorState = error_state()
        SET @errorLine = error_line()
        SET @errorProcedure = isnull(error_procedure(), '-')

        SELECT @errorMessage =
               N'Error %d, Level %d, State %d, Procedure %s, Line %d, Message: ' + @errorMessage;

        RAISERROR (@errorMessage, @errorSeverity, 2, @errorNumber, @errorSeverity, @errorState, @errorProcedure, @errorLine);

        ROLLBACK TRANSACTION; -- To avoid extra error message in response.
        GOTO Error_UnexpectedError;
      END
    END CATCH

    SET @result = 1
    GOTO Exit_Procedure;

    Error_ShardMapAlreadyExists:
    SET @result = 101
    GOTO Exit_Procedure;

    Error_ShardLocationAlreadyExists:
    SET @result = 205
    GOTO Exit_Procedure;

    Error_MissingParameters:
    SET @result = 50
    EXEC __ShardManagement.spGetStoreVersionGlobalHelper
    GOTO Exit_Procedure;

    Error_GSMVersionMismatch:
    SET @result = 51
    EXEC __ShardManagement.spGetStoreVersionGlobalHelper
    GOTO Exit_Procedure;

    Error_UnexpectedError:
    SET @result = 53
    GOTO Exit_Procedure;

    Exit_Procedure:
  END
GO

---------------------------------------------------------------------------------------------------
-- __ShardManagement.spDetachShardGlobal
-- Constraints:
---------------------------------------------------------------------------------------------------
CREATE PROCEDURE __ShardManagement.spDetachShardGlobal
    @input  XML,
    @result INT OUTPUT
AS
  BEGIN
    DECLARE @gsmVersionClient INT,
    @protocol INT,
    @serverName NVARCHAR(128),
    @port INT,
    @databaseName NVARCHAR(128),
    @name NVARCHAR(50)

    SELECT
      @gsmVersionClient = x.value('(GsmVersion)[1]', 'int'),
      @protocol = x.value('(Location/Protocol)[1]', 'int'),
      @serverName = x.value('(Location/ServerName)[1]', 'nvarchar(128)'),
      @port = x.value('(Location/Port)[1]', 'int'),
      @databaseName = x.value('(Location/DatabaseName)[1]', 'nvarchar(128)'),
      @name = x.value('(ShardMap[@Null="0"]/Name)[1]', 'nvarchar(50)')
    FROM
      @input.nodes('/DetachShardGlobal') AS t(x)

    IF (@gsmVersionClient IS NULL OR @protocol IS NULL OR @serverName IS NULL OR @port IS NULL OR
        @databaseName IS NULL)
      GOTO Error_MissingParameters;

    IF (@gsmVersionClient <> __ShardManagement.fnGetStoreVersionGlobal())
      GOTO Error_GSMVersionMismatch;

    DECLARE @tvShardsToDetach TABLE(ShardMapId UNIQUEIDENTIFIER, ShardId UNIQUEIDENTIFIER)

    -- note the detached shards
    INSERT INTO
      @tvShardsToDetach
      SELECT
        tShardMaps.ShardMapId,
        tShards.ShardId
      FROM
        __ShardManagement.ShardMapsGlobal tShardMaps
        JOIN
        __ShardManagement.ShardsGlobal tShards
          ON
            tShards.ShardMapId = tShardMaps.ShardMapId AND
            tShards.Protocol = @protocol AND
            tShards.ServerName = @serverName AND
            tShards.Port = @port AND
            tShards.DatabaseName = @databaseName
      WHERE
        @name IS NULL OR tShardMaps.Name = @name

    -- remove all mappings
    DELETE
      tShardMappings
    FROM
      __ShardManagement.ShardMappingsGlobal tShardMappings
      JOIN
      @tvShardsToDetach tShardsToDetach
        ON
          tShardsToDetach.ShardMapId = tShardMappings.ShardMapId AND
          tShardsToDetach.ShardId = tShardMappings.ShardId

    -- remove all shards
    DELETE
      tShards
    FROM
      __ShardManagement.ShardsGlobal tShards
      JOIN
      @tvShardsToDetach tShardsToDetach
        ON
          tShardsToDetach.ShardMapId = tShards.ShardMapId AND
          tShardsToDetach.ShardId = tShards.ShardId

    SET @result = 1
    GOTO Exit_Procedure;

    Error_MissingParameters:
    SET @result = 50
    EXEC __ShardManagement.spGetStoreVersionGlobalHelper
    GOTO Exit_Procedure;

    Error_GSMVersionMismatch:
    SET @result = 51
    EXEC __ShardManagement.spGetStoreVersionGlobalHelper
    GOTO Exit_Procedure;

    Exit_Procedure:
  END
GO

---------------------------------------------------------------------------------------------------
-- __ShardManagement.spReplaceShardMappingsGlobal
-- Constraints:
---------------------------------------------------------------------------------------------------
CREATE PROCEDURE __ShardManagement.spReplaceShardMappingsGlobal
    @input  XML,
    @result INT OUTPUT
AS
  BEGIN
    DECLARE @gsmVersionClient INT,
    @removeStepsCount INT,
    @addStepsCount INT,
    @shardMapId UNIQUEIDENTIFIER

    -- get operation information as well as number of steps information
    SELECT
      @gsmVersionClient = x.value('(GsmVersion)[1]', 'int'),
      @removeStepsCount = x.value('(@RemoveStepsCount)[1]', 'int'),
      @addStepsCount = x.value('(@AddStepsCount)[1]', 'int'),
      @shardMapId = x.value('(ShardMap/Id)[1]', 'uniqueidentifier')
    FROM
      @input.nodes('ReplaceShardMappingsGlobal') AS t(x)

    IF (@gsmVersionClient IS NULL OR @removeStepsCount IS NULL OR @addStepsCount IS NULL OR
        @shardMapId IS NULL)
      GOTO Error_MissingParameters;

    IF (@gsmVersionClient <> __ShardManagement.fnGetStoreVersionGlobal())
      GOTO Error_GSMVersionMismatch;

    -- check if shard map exists
    IF NOT exists(
        SELECT ShardMapId
        FROM
          __ShardManagement.ShardMapsGlobal
          WITH ( UPDLOCK )
        WHERE
          ShardMapId = @shardMapId)
      GOTO Error_ShardMapNotFound;

    DECLARE @stepShardId UNIQUEIDENTIFIER,
    @stepMappingId UNIQUEIDENTIFIER

    -- read the input for the remove operations
    IF (@removeStepsCount > 0)
      BEGIN
        -- read the shard information for removes
        SELECT @stepShardId = x.value('(Shard/Id)[1]', 'uniqueidentifier')
        FROM
          @input.nodes('ReplaceShardMappingsGlobal/RemoveSteps') AS t(x)

        IF (@stepShardId IS NULL)
          GOTO Error_MissingParameters;

        DECLARE @currentRemoveStep XML,
        @removeStepIndex INT = 1

        WHILE (@removeStepIndex <= @removeStepsCount)
          BEGIN
            SELECT
              @currentRemoveStep = x.query('(./Step[@Id = sql:variable("@removeStepIndex")])[1]')
            FROM
              @input.nodes('ReplaceShardMappingsGlobal/RemoveSteps') AS t(x)

            -- read the remove step
            SELECT @stepMappingId = x.value('(Mapping/Id)[1]', 'uniqueidentifier')
            FROM
              @currentRemoveStep.nodes('./Step') AS t(x)

            IF (@stepMappingId IS NULL)
              GOTO Error_MissingParameters;

            DELETE FROM
              __ShardManagement.ShardMappingsGlobal
            WHERE
              ShardMapId = @shardMapId AND MappingId = @stepMappingId AND ShardId = @stepShardId

            -- reset state for next iteration
            SET @stepMappingId = NULL

            SET @removeStepIndex = @removeStepIndex + 1
          END

        -- reset state for add/update case
        SET @stepShardId = NULL
      END

    -- read the input for the add operations
    IF (@addStepsCount > 0)
      BEGIN
        -- read the shard information for removes
        SELECT @stepShardId = x.value('(Shard/Id)[1]', 'uniqueidentifier')
        FROM
          @input.nodes('ReplaceShardMappingsGlobal/AddSteps') AS t(x)

        IF (@stepShardId IS NULL)
          GOTO Error_MissingParameters;

        DECLARE @currentAddStep XML,
        @addStepIndex INT = 1,
        @stepMinValue VARBINARY(128),
        @stepMaxValue VARBINARY(128),
        @stepStatus INT

        WHILE (@addStepIndex <= @addStepsCount)
          BEGIN
            SELECT @currentAddStep = x.query('(./Step[@Id = sql:variable("@addStepIndex")])[1]')
            FROM
              @input.nodes('ReplaceShardMappingsGlobal/AddSteps') AS t(x)

            SELECT
              @stepMappingId = x.value('(Mapping/Id)[1]', 'uniqueidentifier'),
              @stepMinValue =
              convert(VARBINARY(128), x.value('(Mapping/MinValue)[1]', 'varchar(258)'), 1),
              @stepMaxValue =
              convert(VARBINARY(128), x.value('(Mapping/MaxValue[@Null="0"])[1]', 'varchar(258)'),
                      1),
              @stepStatus = x.value('(Mapping/Status)[1]', 'int')
            FROM
              @currentAddStep.nodes('./Step') AS t(x)

            IF (@stepMappingId IS NULL OR @stepMinValue IS NULL OR @stepStatus IS NULL)
              GOTO Error_MissingParameters;

            -- add mapping
            INSERT INTO
              __ShardManagement.ShardMappingsGlobal (
                MappingId,
                Readable,
                ShardId,
                ShardMapId,
                OperationId,
                MinValue,
                MaxValue,
                Status)
            VALUES (
              @stepMappingId,
              1,
              @stepShardId,
              @shardMapId,
              NULL,
              @stepMinValue,
              @stepMaxValue,
              @stepStatus)

            -- reset state for next iteration
            SET @stepMappingId = NULL
            SET @stepMinValue = NULL
            SET @stepMaxValue = NULL
            SET @stepStatus = NULL

            SET @addStepIndex = @addStepIndex + 1
          END
      END

    SET @result = 1
    GOTO Exit_Procedure;

    Error_ShardMapNotFound:
    SET @result = 102
    GOTO Exit_Procedure;

    Error_MissingParameters:
    SET @result = 50
    EXEC __ShardManagement.spGetStoreVersionGlobalHelper
    GOTO Exit_Procedure;

    Error_GSMVersionMismatch:
    SET @result = 51
    EXEC __ShardManagement.spGetStoreVersionGlobalHelper
    GOTO Exit_Procedure;

    Exit_Procedure:
  END
GO

-- Copyright (c) Microsoft. All rights reserved.
-- Licensed under the MIT license. See LICENSE file in the project root for full license information.

---------------------------------------------------------------------------------------------------
-- Script to upgrade Local Shard Map from version 1.0 to 1.1
---------------------------------------------------------------------------------------------------

-- drop ShardMapManagerLocal table 
DROP TABLE __ShardManagement.ShardMapManagerLocal

-- create ShardMapManagerLocal table with new column names
CREATE TABLE __ShardManagement.ShardMapManagerLocal (
  StoreVersionMajor INT NOT NULL,
  StoreVersionMinor INT NOT NULL
)
-- add primary key constraint
ALTER TABLE __ShardManagement.ShardMapManagerLocal
  ADD CONSTRAINT pkShardMapManagerLocal_StoreVersionMajor PRIMARY KEY (StoreVersionMajor)

-- drop extra objects from version 1.0

IF object_id(N'__ShardManagement.fnGetStoreVersionLocal', N'FN') IS NOT NULL
  BEGIN
    DROP FUNCTION __ShardManagement.fnGetStoreVersionLocal
  END
GO

IF object_id(N'__ShardManagement.spGetStoreVersionLocalHelper', N'P') IS NOT NULL
  BEGIN
    DROP PROCEDURE __ShardManagement.spGetStoreVersionLocalHelper
  END
GO

IF object_id(N'__ShardManagement.spGetAllShardsLocal', N'P') IS NOT NULL
  BEGIN
    DROP PROCEDURE __ShardManagement.spGetAllShardsLocal
  END
GO

IF object_id(N'__ShardManagement.spValidateShardLocal', N'P') IS NOT NULL
  BEGIN
    DROP PROCEDURE __ShardManagement.spValidateShardLocal
  END
GO

IF object_id(N'__ShardManagement.spAddShardLocal', N'P') IS NOT NULL
  BEGIN
    DROP PROCEDURE __ShardManagement.spAddShardLocal
  END
GO

IF object_id(N'__ShardManagement.spRemoveShardLocal', N'P') IS NOT NULL
  BEGIN
    DROP PROCEDURE __ShardManagement.spRemoveShardLocal
  END
GO

IF object_id(N'__ShardManagement.spUpdateShardLocal', N'P') IS NOT NULL
  BEGIN
    DROP PROCEDURE __ShardManagement.spUpdateShardLocal
  END
GO

IF object_id(N'__ShardManagement.spGetAllShardMappingsLocal', N'P') IS NOT NULL
  BEGIN
    DROP PROCEDURE __ShardManagement.spGetAllShardMappingsLocal
  END
GO

IF object_id(N'__ShardManagement.spFindShardMappingByKeyLocal', N'P') IS NOT NULL
  BEGIN
    DROP PROCEDURE __ShardManagement.spFindShardMappingByKeyLocal
  END
GO

IF object_id(N'__ShardManagement.spValidateShardMappingLocal', N'P') IS NOT NULL
  BEGIN
    DROP PROCEDURE __ShardManagement.spValidateShardMappingLocal
  END
GO

IF object_id(N'__ShardManagement.spBulkOperationShardMappingsLocal', N'P') IS NOT NULL
  BEGIN
    DROP PROCEDURE __ShardManagement.spBulkOperationShardMappingsLocal
  END
GO

IF object_id(N'__ShardManagement.spKillSessionsForShardMappingLocal', N'P') IS NOT NULL
  BEGIN
    DROP PROCEDURE __ShardManagement.spKillSessionsForShardMappingLocal
  END
GO

-- create new objects for version 1.1

---------------------------------------------------------------------------------------------------
-- __ShardManagement.fnGetStoreVersionMajorLocal
---------------------------------------------------------------------------------------------------
CREATE FUNCTION __ShardManagement.fnGetStoreVersionMajorLocal()
  RETURNS INT
AS
  BEGIN
    RETURN (SELECT StoreVersionMajor
            FROM __ShardManagement.ShardMapManagerLocal)
  END
GO

---------------------------------------------------------------------------------------------------
-- __ShardManagement.spGetStoreVersionLocalHelper
---------------------------------------------------------------------------------------------------
CREATE PROCEDURE __ShardManagement.spGetStoreVersionLocalHelper
AS
  BEGIN
    SELECT
      5,
      StoreVersionMajor,
      StoreVersionMinor
    FROM
      __ShardManagement.ShardMapManagerLocal
  END
GO

---------------------------------------------------------------------------------------------------
-- __ShardManagement.spGetAllShardsLocal
-- Constraints:
---------------------------------------------------------------------------------------------------
CREATE PROCEDURE __ShardManagement.spGetAllShardsLocal
    @input  XML,
    @result INT OUTPUT
AS
  BEGIN
    DECLARE @lsmVersionMajorClient INT,
    @lsmVersionMinorClient INT

    SELECT
      @lsmVersionMajorClient = x.value('(LsmVersion/MajorVersion)[1]', 'int'),
      @lsmVersionMinorClient = x.value('(LsmVersion/MinorVersion)[1]', 'int')
    FROM
      @input.nodes('/GetAllShardsLocal') AS t(x)

    IF (@lsmVersionMajorClient IS NULL OR @lsmVersionMinorClient IS NULL)
      GOTO Error_MissingParameters;

    IF (@lsmVersionMajorClient <> __ShardManagement.fnGetStoreVersionMajorLocal())
      GOTO Error_LSMVersionMismatch;

    -- shard maps
    SELECT
      1,
      ShardMapId,
      Name,
      MapType,
      KeyType
    FROM
      __ShardManagement.ShardMapsLocal

    -- shards
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
      __ShardManagement.ShardsLocal

    GOTO Success_Exit;

    Error_MissingParameters:
    SET @result = 50
    EXEC __ShardManagement.spGetStoreVersionLocalHelper
    GOTO Exit_Procedure;

    Error_LSMVersionMismatch:
    SET @result = 51
    EXEC __ShardManagement.spGetStoreVersionLocalHelper
    GOTO Exit_Procedure;

    Success_Exit:
    SET @result = 1
    GOTO Exit_Procedure;

    Exit_Procedure:
  END
GO

---------------------------------------------------------------------------------------------------
-- __ShardManagement.spValidateShardLocal
-- Constraints:
---------------------------------------------------------------------------------------------------
CREATE PROCEDURE __ShardManagement.spValidateShardLocal
    @input  XML,
    @result INT OUTPUT
AS
  BEGIN
    DECLARE @lsmVersionMajorClient INT,
    @lsmVersionMinorClient INT,
    @shardMapId UNIQUEIDENTIFIER,
    @shardId UNIQUEIDENTIFIER,
    @shardVersion UNIQUEIDENTIFIER
    SELECT
      @lsmVersionMajorClient = x.value('(LsmVersion/MajorVersion)[1]', 'int'),
      @lsmVersionMinorClient = x.value('(LsmVersion/MinorVersion)[1]', 'int'),
      @shardMapId = x.value('(ShardMapId)[1]', 'uniqueidentifier'),
      @shardId = x.value('(ShardId)[1]', 'uniqueidentifier'),
      @shardVersion = x.value('(ShardVersion)[1]', 'uniqueidentifier')
    FROM
      @input.nodes('/ValidateShardLocal') AS t(x)

    IF (@lsmVersionMajorClient IS NULL OR @lsmVersionMinorClient IS NULL OR @shardMapId IS NULL OR @shardId IS NULL OR
        @shardVersion IS NULL)
      GOTO Error_MissingParameters;

    IF (@lsmVersionMajorClient <> __ShardManagement.fnGetStoreVersionMajorLocal())
      GOTO Error_LSMVersionMismatch;

    -- find shard map
    DECLARE @currentShardMapId UNIQUEIDENTIFIER

    SELECT @currentShardMapId = ShardMapId
    FROM
      __ShardManagement.ShardMapsLocal
    WHERE
      ShardMapId = @shardMapId

    IF (@currentShardMapId IS NULL)
      GOTO Error_ShardMapNotFound;

    DECLARE @currentShardVersion UNIQUEIDENTIFIER

    SELECT @currentShardVersion = Version
    FROM
      __ShardManagement.ShardsLocal
    WHERE
      ShardMapId = @shardMapId AND ShardId = @shardId

    IF (@currentShardVersion IS NULL)
      GOTO Error_ShardDoesNotExist;

    IF (@currentShardVersion <> @shardVersion)
      GOTO Error_ShardVersionMismatch;

    SET @result = 1
    GOTO Exit_Procedure;

    Error_ShardMapNotFound:
    SET @result = 102
    GOTO Exit_Procedure;

    Error_ShardDoesNotExist:
    SET @result = 202
    GOTO Exit_Procedure;

    Error_ShardVersionMismatch:
    SET @result = 204
    GOTO Exit_Procedure;

    Error_MissingParameters:
    SET @result = 50
    EXEC __ShardManagement.spGetStoreVersionLocalHelper
    GOTO Exit_Procedure;

    Error_LSMVersionMismatch:
    SET @result = 51
    EXEC __ShardManagement.spGetStoreVersionLocalHelper
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
    @shardStatus INT

    SELECT
      @lsmVersionMajorClient = x.value('(LsmVersion/MajorVersion)[1]', 'int'),
      @lsmVersionMinorClient = x.value('(LsmVersion/MinorVersion)[1]', 'int'),
      @operationId = x.value('(@OperationId)[1]', 'uniqueidentifier'),
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
      @lsmVersionMajorClient IS NULL OR @lsmVersionMinorClient IS NULL OR @shardMapId IS NULL OR @operationId IS NULL OR
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

    -- add shard map row
    INSERT INTO
      __ShardManagement.ShardMapsLocal
      (ShardMapId, Name, MapType, KeyType, LastOperationId)
    VALUES
      (@shardMapId, @name, @sm_kind, @sm_keykind, @operationId)

    -- add shard row
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

    GOTO Success_Exit;

    Error_MissingParameters:
    SET @result = 50
    EXEC __ShardManagement.spGetStoreVersionLocalHelper
    GOTO Exit_Procedure;

    Error_LSMVersionMismatch:
    SET @result = 51
    EXEC __ShardManagement.spGetStoreVersionLocalHelper
    GOTO Exit_Procedure;

    Success_Exit:
    SET @result = 1
    GOTO Exit_Procedure;

    Exit_Procedure:
  END
GO

---------------------------------------------------------------------------------------------------
-- __ShardManagement.spRemoveShardLocal
-- Constraints:
---------------------------------------------------------------------------------------------------
CREATE PROCEDURE __ShardManagement.spRemoveShardLocal
    @input  XML,
    @result INT OUTPUT
AS
  BEGIN
    DECLARE @lsmVersionMajorClient INT,
    @lsmVersionMinorClient INT,
    @operationId UNIQUEIDENTIFIER,
    @shardMapId UNIQUEIDENTIFIER,
    @shardId UNIQUEIDENTIFIER

    SELECT
      @lsmVersionMajorClient = x.value('(LsmVersion/MajorVersion)[1]', 'int'),
      @lsmVersionMinorClient = x.value('(LsmVersion/MinorVersion)[1]', 'int'),
      @operationId = x.value('(@OperationId)[1]', 'uniqueidentifier'),
      @shardMapId = x.value('(ShardMap/Id)[1]', 'uniqueidentifier'),
      @shardId = x.value('(Shard/Id)[1]', 'uniqueidentifier')
    FROM
      @input.nodes('/RemoveShardLocal') AS t(x)

    IF (@lsmVersionMajorClient IS NULL OR @lsmVersionMinorClient IS NULL OR @operationId IS NULL OR @shardMapId IS NULL
        OR @shardId IS NULL)
      GOTO Error_MissingParameters;

    IF (@lsmVersionMajorClient <> __ShardManagement.fnGetStoreVersionMajorLocal())
      GOTO Error_LSMVersionMismatch;

    -- remove shard row
    DELETE FROM
      __ShardManagement.ShardsLocal
    WHERE
      ShardMapId = @shardMapId AND ShardId = @shardId

    -- remove shard map row
    DELETE FROM
      __ShardManagement.ShardMapsLocal
    WHERE
      ShardMapId = @shardMapId

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

    Exit_Procedure:
  END
GO

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
    @shardStatus INT

    SELECT
      @lsmVersionMajorClient = x.value('(LsmVersion/MajorVersion)[1]', 'int'),
      @lsmVersionMinorClient = x.value('(LsmVersion/MinorVersion)[1]', 'int'),
      @operationId = x.value('(@OperationId)[1]', 'uniqueidentifier'),
      @shardMapId = x.value('(ShardMap/Id)[1]', 'uniqueidentifier'),
      @shardId = x.value('(Shard/Id)[1]', 'uniqueidentifier'),
      @shardVersion = x.value('(Shard/Version)[1]', 'uniqueidentifier'),
      @shardStatus = x.value('(Shard/Status)[1]', 'int')
    FROM
      @input.nodes('/UpdateShardLocal') AS t(x)

    IF (
      @lsmVersionMajorClient IS NULL OR @lsmVersionMinorClient IS NULL OR @operationId IS NULL OR @shardMapId IS NULL OR
      @shardId IS NULL OR @shardVersion IS NULL OR @shardStatus IS NULL)
      GOTO Error_MissingParameters;

    IF (@lsmVersionMajorClient <> __ShardManagement.fnGetStoreVersionMajorLocal())
      GOTO Error_LSMVersionMismatch;

    UPDATE
      __ShardManagement.ShardsLocal
    SET
      Version         = @shardVersion,
      Status          = @shardStatus,
      LastOperationId = @operationId
    WHERE
      ShardMapId = @shardMapId AND ShardId = @shardId

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

    Exit_Procedure:
  END
GO

---------------------------------------------------------------------------------------------------
-- __ShardManagement.spGetAllShardMappingsLocal
-- Constraints:
---------------------------------------------------------------------------------------------------
CREATE PROCEDURE __ShardManagement.spGetAllShardMappingsLocal
    @input  XML,
    @result INT OUTPUT
AS
  BEGIN
    DECLARE @lsmVersionMajorClient INT,
    @lsmVersionMinorClient INT,
    @shardMapId UNIQUEIDENTIFIER,
    @shardId UNIQUEIDENTIFIER,
    @minValue VARBINARY(128),
    @maxValue VARBINARY(128)

    SELECT
      @lsmVersionMajorClient = x.value('(LsmVersion/MajorVersion)[1]', 'int'),
      @lsmVersionMinorClient = x.value('(LsmVersion/MinorVersion)[1]', 'int'),
      @shardMapId = x.value('(ShardMap/Id)[1]', 'uniqueidentifier'),
      @shardId = x.value('(Shard/Id)[1]', 'uniqueidentifier'),
      @minValue = convert(VARBINARY(128), x.value('(Range[@Null="0"]/MinValue)[1]', 'varchar(258)'), 1),
      @maxValue = convert(VARBINARY(128), x.value('(Range[@Null="0"]/MaxValue[@Null="0"])[1]', 'varchar(258)'), 1)
    FROM
      @input.nodes('/GetAllShardMappingsLocal') AS t(x)

    IF (@lsmVersionMajorClient IS NULL OR @lsmVersionMinorClient IS NULL OR @shardMapId IS NULL OR @shardId IS NULL)
      GOTO Error_MissingParameters;

    IF (@lsmVersionMajorClient <> __ShardManagement.fnGetStoreVersionMajorLocal())
      GOTO Error_LSMVersionMismatch;

    DECLARE @mapType INT

    SELECT @mapType = MapType
    FROM
      __ShardManagement.ShardMapsLocal
    WHERE
      ShardMapId = @shardMapId

    IF (@mapType IS NULL)
      GOTO Error_ShardMapNotFound;

    DECLARE @minValueCalculated VARBINARY(128) = 0x,
    @maxValueCalculated VARBINARY (128) = NULL

    -- check if range is supplied and update accordingly.
    IF (@minValue IS NOT NULL)
      SET @minValueCalculated = @minValue

    IF (@maxValue IS NOT NULL)
      SET @maxValueCalculated = @maxValue

    IF (@mapType = 1)
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
          s.ShardMapId,
          s.Protocol,
          s.ServerName,
          s.Port,
          s.DatabaseName,
          s.Status -- fields for SqlShard, ShardMapId is repeated here
        FROM
          __ShardManagement.ShardMappingsLocal m
          JOIN
          __ShardManagement.ShardsLocal s
            ON
              m.ShardId = s.ShardId
        WHERE
          m.ShardMapId = @shardMapId AND
          m.ShardId = @shardId AND
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
          s.ShardMapId,
          s.Protocol,
          s.ServerName,
          s.Port,
          s.DatabaseName,
          s.Status -- fields for SqlShard, ShardMapId is repeated here
        FROM
          __ShardManagement.ShardMappingsLocal m
          JOIN
          __ShardManagement.ShardsLocal s
            ON
              m.ShardId = s.ShardId
        WHERE
          m.ShardMapId = @shardMapId AND
          m.ShardId = @shardId AND
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

    Error_MissingParameters:
    SET @result = 50
    EXEC __ShardManagement.spGetStoreVersionLocalHelper
    GOTO Exit_Procedure;

    Error_LSMVersionMismatch:
    SET @result = 51
    EXEC __ShardManagement.spGetStoreVersionLocalHelper
    GOTO Exit_Procedure;

    Exit_Procedure:
  END
GO

---------------------------------------------------------------------------------------------------
-- __ShardManagement.spFindShardMappingByKeyLocal
-- Constraints:
---------------------------------------------------------------------------------------------------
CREATE PROCEDURE __ShardManagement.spFindShardMappingByKeyLocal
    @input  XML,
    @result INT OUTPUT
AS
  BEGIN
    DECLARE @lsmVersionMajorClient INT,
    @lsmVersionMinorClient INT,
    @shardMapId UNIQUEIDENTIFIER,
    @keyValue VARBINARY(128)

    SELECT
      @lsmVersionMajorClient = x.value('(LsmVersion/MajorVersion)[1]', 'int'),
      @lsmVersionMinorClient = x.value('(LsmVersion/MinorVersion)[1]', 'int'),
      @shardMapId = x.value('(ShardMap/Id)[1]', 'uniqueidentifier'),
      @keyValue = convert(VARBINARY(128), x.value('(Key/Value)[1]', 'varchar(258)'), 1)
    FROM
      @input.nodes('/FindShardMappingByKeyLocal') AS t(x)

    IF (@lsmVersionMajorClient IS NULL OR @lsmVersionMinorClient IS NULL OR @shardMapId IS NULL OR @keyValue IS NULL)
      GOTO Error_MissingParameters;

    IF (@lsmVersionMajorClient <> __ShardManagement.fnGetStoreVersionMajorLocal())
      GOTO Error_LSMVersionMismatch;

    DECLARE @mapType INT

    SELECT @mapType = MapType
    FROM
      __ShardManagement.ShardMapsLocal
    WHERE
      ShardMapId = @shardMapId

    IF (@mapType IS NULL)
      GOTO Error_ShardMapNotFound;

    IF (@mapType = 1)
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
          s.ShardMapId,
          s.Protocol,
          s.ServerName,
          s.Port,
          s.DatabaseName,
          s.Status -- fields for SqlShard, ShardMapId is repeated here
        FROM
          __ShardManagement.ShardMappingsLocal m
          JOIN
          __ShardManagement.ShardsLocal s
            ON
              m.ShardId = s.ShardId
        WHERE
          m.ShardMapId = @shardMapId AND
          m.MinValue = @keyValue
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
          s.ShardMapId,
          s.Protocol,
          s.ServerName,
          s.Port,
          s.DatabaseName,
          s.Status -- fields for SqlShard, ShardMapId is repeated here
        FROM
          __ShardManagement.ShardMappingsLocal m
          JOIN
          __ShardManagement.ShardsLocal s
            ON
              m.ShardId = s.ShardId
        WHERE
          m.ShardMapId = @shardMapId AND
          m.MinValue <= @keyValue AND (m.MaxValue IS NULL OR m.MaxValue > @keyValue)
      END

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
    EXEC __ShardManagement.spGetStoreVersionLocalHelper
    GOTO Exit_Procedure;

    Error_LSMVersionMismatch:
    SET @result = 51
    EXEC __ShardManagement.spGetStoreVersionLocalHelper
    GOTO Exit_Procedure;

    Exit_Procedure:
  END
GO

---------------------------------------------------------------------------------------------------
-- __ShardManagement.spValidateShardMappingLocal
-- Constraints:
---------------------------------------------------------------------------------------------------
CREATE PROCEDURE __ShardManagement.spValidateShardMappingLocal
    @input  XML,
    @result INT OUTPUT
AS
  BEGIN
    DECLARE @lsmVersionMajorClient INT,
    @lsmVersionMinorClient INT,
    @shardMapId UNIQUEIDENTIFIER,
    @mappingId UNIQUEIDENTIFIER

    SELECT
      @lsmVersionMajorClient = x.value('(LsmVersion/MajorVersion)[1]', 'int'),
      @lsmVersionMinorClient = x.value('(LsmVersion/MinorVersion)[1]', 'int'),
      @shardMapId = x.value('(ShardMapId)[1]', 'uniqueidentifier'),
      @mappingId = x.value('(MappingId)[1]', 'uniqueidentifier')
    FROM
      @input.nodes('/ValidateShardMappingLocal') AS t(x)

    IF (@lsmVersionMajorClient IS NULL OR @lsmVersionMinorClient IS NULL OR @shardMapId IS NULL OR @mappingId IS NULL)
      GOTO Error_MissingParameters;

    IF (@lsmVersionMajorClient <> __ShardManagement.fnGetStoreVersionMajorLocal())
      GOTO Error_LSMVersionMismatch;

    -- find shard map
    DECLARE @currentShardMapId UNIQUEIDENTIFIER

    SELECT @currentShardMapId = ShardMapId
    FROM
      __ShardManagement.ShardMapsLocal
    WHERE
      ShardMapId = @shardMapId

    IF (@currentShardMapId IS NULL)
      GOTO Error_ShardMapNotFound;

    DECLARE @m_status_current INT

    SELECT @m_status_current = Status
    FROM
      __ShardManagement.ShardMappingsLocal
    WHERE
      ShardMapId = @shardMapId AND MappingId = @mappingId

    IF (@m_status_current IS NULL)
      GOTO Error_MappingDoesNotExist;

    IF (@m_status_current <> 1)
      GOTO Error_MappingIsOffline;

    SET @result = 1
    GOTO Exit_Procedure;

    Error_ShardMapNotFound:
    SET @result = 102
    GOTO Exit_Procedure;

    Error_MappingDoesNotExist:
    SET @result = 301
    GOTO Exit_Procedure;

    Error_MappingIsOffline:
    SET @result = 309
    GOTO Exit_Procedure;

    Error_MissingParameters:
    SET @result = 50
    EXEC __ShardManagement.spGetStoreVersionLocalHelper
    GOTO Exit_Procedure;

    Error_LSMVersionMismatch:
    SET @result = 51
    EXEC __ShardManagement.spGetStoreVersionLocalHelper
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
      @stepsCount = x.value('(@StepsCount)[1]', 'int'),
      @shardMapId = x.value('(ShardMap/Id)[1]', 'uniqueidentifier'),
      @shardId = x.value('(Shard/Id)[1]', 'uniqueidentifier'),
      @shardVersion = x.value('(Shard/Version)[1]', 'uniqueidentifier')
    FROM
      @input.nodes('/BulkOperationShardMappingsLocal') AS t(x)

    IF (
      @lsmVersionMajorClient IS NULL OR @lsmVersionMinorClient IS NULL OR @operationId IS NULL OR @stepsCount IS NULL OR
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
                @stepMinValue = convert(VARBINARY(128), x.value('(Mapping/MinValue)[1]', 'varchar(258)'), 1),
                @stepMaxValue = convert(VARBINARY(128), x.value('(Mapping/MaxValue[@Null="0"])[1]', 'varchar(258)'), 1),
                @stepMappingStatus = x.value('(Mapping/Status)[1]', 'int')
              FROM
                @currentStep.nodes('./Step') AS t(x)

              IF (@stepMinValue IS NULL OR @stepMappingStatus IS NULL)
                GOTO Error_MissingParameters;

              -- add mapping
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

    Success_Exit:
    SET @result = 1
    GOTO Exit_Procedure;

    Exit_Procedure:
  END
GO

---------------------------------------------------------------------------------------------------
-- __ShardManagement.spKillSessionsForShardMappingLocal
-- Constraints:
---------------------------------------------------------------------------------------------------
CREATE PROCEDURE __ShardManagement.spKillSessionsForShardMappingLocal
    @input  XML,
    @result INT OUTPUT
AS
  BEGIN
    DECLARE @lsmVersionMajorClient INT,
    @lsmVersionMinorClient INT,
    @patternForKill NVARCHAR(128)

    -- get operation information as well as number of steps information
    SELECT
      @lsmVersionMajorClient = x.value('(LsmVersion/MajorVersion)[1]', 'int'),
      @lsmVersionMinorClient = x.value('(LsmVersion/MinorVersion)[1]', 'int'),
      @patternForKill = x.value('(Pattern)[1]', 'nvarchar(128)')
    FROM
      @input.nodes('/KillSessionsForShardMappingLocal') AS t(x)

    IF (@lsmVersionMajorClient IS NULL OR @lsmVersionMinorClient IS NULL OR @patternForKill IS NULL)
      GOTO Error_MissingParameters;

    IF (@lsmVersionMajorClient <> __ShardManagement.fnGetStoreVersionMajorLocal())
      GOTO Error_LSMVersionMismatch;

    DECLARE @tvKillCommands TABLE(spid SMALLINT PRIMARY KEY, commandForKill NVARCHAR(10))

    -- insert empty row
    INSERT INTO
      @tvKillCommands (spid, commandForKill)
    VALUES
      (0, N'')

    INSERT INTO
      @tvKillCommands(spid, commandForKill)
      SELECT
        session_id,
        'kill ' + convert(NVARCHAR(10), session_id)
      FROM
        sys.dm_exec_sessions
      WHERE
        session_id > 50 AND program_name LIKE '%' + @patternForKill + '%'

    DECLARE @currentSpid INT,
    @currentCommandForKill NVARCHAR(10)

    DECLARE @current_error INT

    SELECT TOP 1
      @currentSpid = spid,
      @currentCommandForKill = commandForKill
    FROM
      @tvKillCommands
    ORDER BY
      spid DESC

    WHILE (@currentSpid > 0)
      BEGIN
        BEGIN TRY
        -- kill the current spid
        EXEC (@currentCommandForKill)

        -- remove the current row
        DELETE
          @tvKillCommands
        WHERE
          spid = @currentSpid

        -- get next row
        SELECT TOP 1
          @currentSpid = spid,
          @currentCommandForKill = commandForKill
        FROM
          @tvKillCommands
        ORDER BY
          spid DESC
        END TRY
        BEGIN CATCH
        -- if the process is no longer valid, assume that it is gone
        IF (error_number() <> 6106)
          GOTO Error_UnableToKillSessions;
        END CATCH
      END

    SET @result = 1
    GOTO Exit_Procedure;

    Error_UnableToKillSessions:
    SET @result = 305
    GOTO Exit_Procedure;

    Error_MissingParameters:
    SET @result = 50
    EXEC __ShardManagement.spGetStoreVersionLocalHelper
    GOTO Exit_Procedure;

    Error_LSMVersionMismatch:
    SET @result = 51
    EXEC __ShardManagement.spGetStoreVersionLocalHelper
    GOTO Exit_Procedure;

    Exit_Procedure:
  END
GO

-- update version as 1.1
INSERT INTO __ShardManagement.ShardMapManagerLocal VALUES (1, 1)
GO

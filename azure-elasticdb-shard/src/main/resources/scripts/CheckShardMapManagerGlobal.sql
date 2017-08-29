-- Copyright (c) Microsoft. All rights reserved.
-- Licensed under the MIT license. See LICENSE file in the project root for full license information.

---------------------------------------------------------------------------------------------------
-- Reads from shard map manager version information table if it exists.
---------------------------------------------------------------------------------------------------

DECLARE @stmt VARCHAR(128)
IF object_id(N'__ShardManagement.ShardMapManagerGlobal', N'U') IS NOT NULL
  BEGIN
    IF exists(SELECT Name
              FROM sys.columns
              WHERE Name = N'StoreVersion' AND
                    object_id = object_id(N'__ShardManagement.ShardMapManagerGlobal'))
      BEGIN
        SET @stmt = 'select 5, StoreVersion from __ShardManagement.ShardMapManagerGlobal'
      END
    ELSE
      BEGIN
        SET @stmt = 'select 5, StoreVersionMajor, StoreVersionMinor  from __ShardManagement.ShardMapManagerGlobal'
      END
    EXEC (@stmt)
  END
GO

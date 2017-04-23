-- Copyright (c) Microsoft. All rights reserved.
-- Licensed under the MIT license. See LICENSE file in the project root for full license information.

---------------------------------------------------------------------------------------------------
-- Script to upgrade Local Shard Map from version 1000.0 to 1000.1
---------------------------------------------------------------------------------------------------

-- drop extra column from ShardMapManagerLocal table which was added as first step to hold SCH-M lock during upgrade
IF exists(SELECT *
          FROM sys.columns
          WHERE Name = N'UpgradeLock' AND object_id = object_id(N'__ShardManagement.ShardMapManagerLocal'))
  BEGIN
    ALTER TABLE __ShardManagement.ShardMapManagerLocal
      DROP COLUMN UpgradeLock
  END
GO

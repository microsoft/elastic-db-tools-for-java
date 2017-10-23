-- Copyright (c) Microsoft. All rights reserved.
-- Licensed under the MIT license. See LICENSE file in the project root for full license information.

---------------------------------------------------------------------------------------------------
-- Script to upgrade Global Shard Map from version 0.0 to 1.0
---------------------------------------------------------------------------------------------------

-- add a column to ShardMapManagerGlobal table to hold SCH-M lock during upgrade
ALTER TABLE __ShardManagement.ShardMapManagerGlobal
  ADD UpgradeLock INT NULL
GO

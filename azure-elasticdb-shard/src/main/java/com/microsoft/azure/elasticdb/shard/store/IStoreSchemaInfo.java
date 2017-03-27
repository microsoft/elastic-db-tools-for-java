package com.microsoft.azure.elasticdb.shard.store;

// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

import java.sql.SQLXML;

/**
 * Storage representation of a shard schema info.
 */
public interface IStoreSchemaInfo {
    /**
     * Schema info name.
     */
    String getName();

    /**
     * Schema info represented in XML.
     */
    SQLXML getShardingSchemaInfo();
}
package com.microsoft.azure.elasticdb.shard.base;

import java.sql.Connection;
import java.util.concurrent.Callable;

/*
 * Elastic database tools for Azure SQL Database.
 * 
 * Copyright(c) Microsoft Corporation All rights reserved.
 * 
 * This program is made available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

import com.microsoft.azure.elasticdb.shard.store.StoreShardMap;

/**
 * Represents capabilities to provide a Shard along with an associated value.
 */
public interface IShardProvider<ValueT> {

    /**
     * Shard for the ShardProvider object.
     */
    Shard getShardInfo();

    /**
     * Value corresponding to the Shard. Represents traits of the Shard object provided by the ShardInfo property.
     */
    ValueT getValue();

    /**
     * Performs validation that the local representation is as up-to-date as the representation on the backing data store.
     *
     * @param shardMap
     *            Shard map to which the shard provider belongs.
     * @param conn
     *            Connection used for validation.
     */
    void validate(StoreShardMap shardMap,
            Connection conn);

    /**
     * Asynchronously performs validation that the local representation is as up-to-date as the representation on the backing data store.
     *
     * @param shardMap
     *            Shard map to which the shard provider belongs.
     * @param conn
     *            Connection used for validation.
     * @return A task to await validation completion
     */
    Callable validateAsync(StoreShardMap shardMap,
            Connection conn);
}

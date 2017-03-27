package com.microsoft.azure.elasticdb.shard.base;

// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

import com.microsoft.azure.elasticdb.shard.store.IStoreShardMap;
import com.microsoft.sqlserver.jdbc.SQLServerConnection;
import javafx.concurrent.Task;

/**
 * Represents capabilities to provide a Shard along with an associated value.
 */
public interface IShardProvider<TValue> {
    /**
     * Shard for the ShardProvider object.
     */
    Shard getShardInfo();

    /**
     * Value corresponding to the Shard. Represents traits of the Shard
     * object provided by the ShardInfo property.
     */
    TValue getValue();

    /**
     * Performs validation that the local representation is as
     * up-to-date as the representation on the backing
     * data store.
     *
     * @param shardMap Shard map to which the shard provider belongs.
     * @param conn     Connection used for validation.
     */
    void Validate(IStoreShardMap shardMap, SQLServerConnection conn);

    /**
     * Asynchronously performs validation that the local representation is as
     * up-to-date as the representation on the backing
     * data store.
     *
     * @param shardMap Shard map to which the shard provider belongs.
     * @param conn     Connection used for validation.
     * @return A task to await validation completion
     */
    Task ValidateAsync(IStoreShardMap shardMap, SQLServerConnection conn);
}

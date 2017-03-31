package com.microsoft.azure.elasticdb.shard.base;

// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

import com.microsoft.azure.elasticdb.shard.mapmanager.ShardMapManager;
import com.microsoft.azure.elasticdb.shard.store.IStoreShardMap;
import com.microsoft.azure.elasticdb.shard.storeops.base.ShardLocation;
import com.microsoft.azure.elasticdb.shard.utils.ICloneable;
import com.microsoft.sqlserver.jdbc.SQLServerConnection;
import javafx.concurrent.Task;

import java.util.UUID;

/**
 * Representation of a single shard. Shards are basically locators for
 * data sources i.e. <see cref="ShardLocation"/>s that have been
 * registered with a shard map. Shards are used in
 * mapping as targets of mappings (see <see cref="PointMapping{TKey}"/>
 * and <see cref="RangeMapping{TKey}"/>).
 */
//TODO: In .NET this class implements IEquatable<Shard>, But the same is not possible in Java?
public final class Shard implements IShardProvider<ShardLocation>, ICloneable<Shard> {
    private UUID _shardMapId;
    private ShardMapManager _manager;

    public UUID getShardMapId() {
        return this._shardMapId;
    }

    public Shard getShardInfo() {
        return null;
    }

    public ShardLocation getValue() {
        return null;
    }

    public void Validate(IStoreShardMap shardMap, SQLServerConnection conn) {

    }

    public Task ValidateAsync(IStoreShardMap shardMap, SQLServerConnection conn) {
        return null;
    }

    public Shard Clone() {
        return null;
    }

    public ShardMapManager getManager() {
        return _manager;
    }
}

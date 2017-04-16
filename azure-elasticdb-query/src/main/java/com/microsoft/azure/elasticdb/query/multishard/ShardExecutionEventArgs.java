package com.microsoft.azure.elasticdb.query.multishard;

// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

import com.microsoft.azure.elasticdb.core.commons.helpers.EventArgs;
import com.microsoft.azure.elasticdb.shard.base.ShardLocation;

/**
 * Input to be passed to per-shard event handlers.
 */
public class ShardExecutionEventArgs extends EventArgs {
    /**
     * The exception to process, if applicable. Null if no exception was thrown.
     */
    private RuntimeException ex;
    /**
     * The location of the shard on which the MultiShardCommand is currently executing.
     */
    private ShardLocation shardLocation;
    /**
     * FOR INTERNAL USE ONLY:
     * The returned input reader.
     */
    private LabeledDbDataReader reader;

    public final RuntimeException getException() {
        return ex;
    }

    public final void setException(RuntimeException value) {
        ex = value;
    }

    public final ShardLocation getShardLocation() {
        return shardLocation;
    }

    public final void setShardLocation(ShardLocation value) {
        shardLocation = value;
    }

    public final LabeledDbDataReader getReader() {
        return reader;
    }

    public final void setReader(LabeledDbDataReader value) {
        reader = value;
    }
}
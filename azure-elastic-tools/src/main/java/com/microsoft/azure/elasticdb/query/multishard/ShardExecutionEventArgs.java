package com.microsoft.azure.elasticdb.query.multishard;

/*
 * Elastic database tools for Azure SQL Database.
 * 
 * Copyright(c) Microsoft Corporation All rights reserved.
 * 
 * This program is made available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

import com.microsoft.azure.elasticdb.core.commons.helpers.EventArgs;
import com.microsoft.azure.elasticdb.shard.base.ShardLocation;

/**
 * Input to be passed to per-shard event handlers.
 */
public class ShardExecutionEventArgs extends EventArgs {

    /**
     * The exception to process, if applicable. Null if no exception was thrown.
     */
    private Exception ex;
    /**
     * The location of the shard on which the MultiShardStatement is currently executing.
     */
    private ShardLocation shardLocation;
    /**
     * FOR INTERNAL USE ONLY: The returned input reader.
     */
    private LabeledResultSet reader;

    public final Exception getException() {
        return ex;
    }

    public final void setException(Exception value) {
        ex = value;
    }

    public final ShardLocation getShardLocation() {
        return shardLocation;
    }

    public final void setShardLocation(ShardLocation value) {
        shardLocation = value;
    }

    public final LabeledResultSet getReader() {
        return reader;
    }

    public final void setReader(LabeledResultSet value) {
        reader = value;
    }
}
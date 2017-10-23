package com.microsoft.azure.elasticdb.query.exception;

import java.io.Serializable;

/*
 * Elastic database tools for Azure SQL Database.
 * 
 * Copyright(c) Microsoft Corporation All rights reserved.
 * 
 * This program is made available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

import com.microsoft.azure.elasticdb.shard.base.ShardLocation;

/**
 * Custom exception thrown when the schema on at least one of the shards participating in the overall query does not conform to the expected schema
 * for the multi-shard query as a whole. Purpose: Custom exception to throw when the schema from a ResultSet from a given shard does not conform to
 * the expected schema for the fanout query as a whole.
 */
public class MultiShardSchemaMismatchException extends MultiShardException implements Serializable {

    public MultiShardSchemaMismatchException(ShardLocation shardLocation,
            String message) {
        super(shardLocation, message);
    }

    /**
     * Initializes a new instance of the MultiShardSchemaMismatchException class with the specified error message and the reference to the inner
     * exception that is the cause of this exception.
     *
     * @param message
     *            specifies the message that explains the reason for the exception.
     * @param innerException
     *            specifies the exception encountered at the shard.
     */
    public MultiShardSchemaMismatchException(String message,
            RuntimeException innerException) {
        super(message, innerException);
    }

    /**
     * Initializes a new instance of the MultiShardSchemaMismatchException class with the specified error message.
     *
     * @param message
     *            specifies the message that explains the reason for the exception.
     */
    public MultiShardSchemaMismatchException(String message) {
        super(message);
    }

    /**
     * Initializes a new instance of the MultiShardSchemaMismatchException class.
     */
    public MultiShardSchemaMismatchException() {
        super();
    }
}
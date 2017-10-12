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
 * The <see cref="MultiShardResultSet"/> throws this exception when an exception has been hit reading data from one of the underlying shards. This
 * indicates that not all rows have been successfully retrieved from the targeted shard(s). Users can then take the steps necessary to decide whether
 * to re-run the query, or whether to continue working with the rows that have already been retrieved. This exception is only thrown with the partial
 * results policy. Purpose: Custom exception to throw when the MultiShardResultSet hits an exception during a next() call to one of the underlying
 * ResultSets. When that happens all we know is that we were not able to read all the results from that shard, so we need to notify the user somehow.
 */
public class MultiShardPartialReadException extends MultiShardException implements Serializable {

    public MultiShardPartialReadException(ShardLocation shardLocation,
            String message,
            RuntimeException inner) {
        super(shardLocation, message, inner);
    }

    /**
     * Initializes a new instance of the MultiShardPartialReadException class with the specified error message and reference to the inner exception
     * causing the MultiShardPartialReadException.
     *
     * @param message
     *            specifies the message that explains the reason for the exception.
     * @param innerException
     *            specifies the exception encountered at the shard.
     */
    public MultiShardPartialReadException(String message,
            RuntimeException innerException) {
        super(message, innerException);
    }

    /**
     * Initializes a new instance of the MultiShardPartialReadException class with the specified error message.
     *
     * @param message
     *            specifies the message that explains the reason for the exception.
     */
    public MultiShardPartialReadException(String message) {
        super(message);
    }

    /**
     * Initializes a new instance of the MultiShardPartialReadException class.
     */
    public MultiShardPartialReadException() {
        super();
    }

}
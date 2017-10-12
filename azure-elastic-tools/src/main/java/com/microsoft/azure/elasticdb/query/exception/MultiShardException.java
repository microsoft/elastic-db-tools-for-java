package com.microsoft.azure.elasticdb.query.exception;

import java.io.Serializable;
import java.util.Locale;

/*
 * Elastic database tools for Azure SQL Database.
 * 
 * Copyright(c) Microsoft Corporation All rights reserved.
 * 
 * This program is made available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */
//
// Purpose:
// Public type to communicate failures when performing operations against a shard

// Suppression rationale: "Multi" is the spelling we want here.
//

import com.microsoft.azure.elasticdb.shard.base.ShardLocation;

/**
 * DEVNOTE: Encapsulate SMM ShardLocation type for now since Shard isn't Serializable Support for serialization of ShardLocation is in the works. A
 * MultiShardException represents an exception that occurred when performing operations against a shard. It provides information about both the
 * identity of the shard and the exception that occurred. Depending on the nature of the exception, one can try re-running the multi-shard query,
 * execute a separate query targeted directly at the shard(s) on that yielded the exception, or lastly execute the query manually against the shard
 * using a common tool such as SSMS.
 */
public class MultiShardException extends Exception implements Serializable {

    private ShardLocation shardLocation;

    /**
     * Initializes a new instance of the MultiShardException class.
     */
    public MultiShardException() {
        this(dummyShardLocation());
    }

    /**
     * Initializes a new instance of the MultiShardException class with the specified error message.
     *
     * @param message
     *            specifies the exception encountered at the shard.
     */
    public MultiShardException(String message) {
        this(dummyShardLocation(), message);
    }

    /**
     * Initializes a new instance of the MultiShardException class with the specified error message and the reference to the inner exception that is
     * the cause of this exception.
     *
     * @param message
     *            specifies the message that explains the reason for the exception.
     * @param innerException
     *            specifies the exception encountered at the shard.
     */
    public MultiShardException(String message,
            Exception innerException) {
        this(dummyShardLocation(), message, innerException);
    }

    /**
     * Initializes a new instance of the <see cref="MultiShardException"/> class with the specified shard location.
     *
     * @param shardLocation
     *            specifies the location of the shard where the exception occurred.
     */
    public MultiShardException(ShardLocation shardLocation) {
        this(shardLocation, String.format("Exception encountered on shard: %1$s", shardLocation));
    }

    /**
     * Initializes a new instance of the <see cref="MultiShardException"/> class with the specified shard location and error message.
     *
     * @param shardLocation
     *            specifies the location of the shard where the exception occurred.
     * @param message
     *            specifies the message that explains the reason for the exception.
     */
    public MultiShardException(ShardLocation shardLocation,
            String message) {
        this(shardLocation, message, null);
    }

    /**
     * Initializes a new instance of the <see cref="MultiShardException"/> class with the specified shard location and exception.
     *
     * @param shardLocation
     *            specifies the location of the shard where the exception occurred.
     * @param inner
     *            specifies the exception encountered at the shard.
     */
    public MultiShardException(ShardLocation shardLocation,
            Exception inner) {
        this(shardLocation, String.format("Exception encountered on shard: %1$s", shardLocation), inner);
    }

    /**
     * Initializes a new instance of the <see cref="MultiShardException"/> class with the specified shard location, error message and exception
     * encountered.
     *
     * @param shardLocation
     *            specifies the location of the shard where the exception occurred.
     * @param message
     *            specifies the message that explains the reason for the exception.
     * @param inner
     *            specifies the exception encountered at the shard.
     * @throws IllegalArgumentException
     *             The <paramref name="shardLocation"/> is null
     */
    public MultiShardException(ShardLocation shardLocation,
            String message,
            Exception inner) {
        super(message, inner);
        if (null == shardLocation) {
            throw new IllegalArgumentException("shardLocation");
        }

        this.shardLocation = shardLocation;
    }

    private static ShardLocation dummyShardLocation() {
        return new ShardLocation("unknown", "unknown");
    }

    /**
     * The shard associated with this exception.
     */
    public final ShardLocation getShardLocation() {
        return shardLocation;
    }

    /**
     * Creates and returns a string representation of the current <see cref="MultiShardException"/>.
     *
     * @return String representation of the current exception.
     */
    @Override
    public String toString() {
        String text = super.toString();
        return String.format(Locale.getDefault(), "MultiShardException encountered on shard: %1$s %2$s %3$s", getShardLocation(),
                System.lineSeparator(), text);
    }
}
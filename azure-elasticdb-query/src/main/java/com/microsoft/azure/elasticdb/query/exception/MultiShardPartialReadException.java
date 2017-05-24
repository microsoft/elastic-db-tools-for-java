package com.microsoft.azure.elasticdb.query.exception;

/* Copyright (c) Microsoft. All rights reserved.
Licensed under the MIT license. See LICENSE file in the project root for full license information.*/
//
// Purpose:
// Custom exception to throw when the MultiShardDataReader hits an exception
// during a Read() call to one of the underlying SqlDataReaders.  When that happens
// all we know is that we were not able to read all the results from that shard, so
// we need to notify the user somehow.
//
// Notes:

// Suppression rationale: "Multi" is the spelling we want here.
//

import com.microsoft.azure.elasticdb.shard.base.ShardLocation;
import java.io.Serializable;

/**
 * The <see cref="MultiShardDataReader"/> throws this exception when an exception has been hit
 * reading data from one of the underlying shards. This indicates that not all rows have been
 * successfully retrieved from the targeted shard(s). Users can then take the steps necessary to
 * decide whether to re-run the query, or whether to continue working with the rows that have
 * already been retrieved. This exception is only thrown with the partial results policy.
 */
public class MultiShardPartialReadException extends MultiShardException implements Serializable {
  ///#region Custom Constructors

  public MultiShardPartialReadException(ShardLocation shardLocation, String message,
      RuntimeException inner) {
    super(shardLocation, message, inner);
  }

  ///#endregion Custom Constructors

  ///#region Standard Exception Constructors

  /**
   * Initializes a new instance of the MultiShardPartialReadException class with the specified error
   * message and reference to the inner exception causing the MultiShardPartialReadException.
   *
   * @param message specifies the message that explains the reason for the exception.
   * @param innerException specifies the exception encountered at the shard.
   */
  public MultiShardPartialReadException(String message, RuntimeException innerException) {
    super(message, innerException);
  }

  /**
   * Initializes a new instance of the MultiShardPartialReadException class with the specified error
   * message.
   *
   * @param message specifies the message that explains the reason for the exception.
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
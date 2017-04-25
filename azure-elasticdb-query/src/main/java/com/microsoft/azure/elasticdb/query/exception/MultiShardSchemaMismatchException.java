package com.microsoft.azure.elasticdb.query.exception;

// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.
//
// Purpose:
// Custom exception to throw when the schema from a DbDataReader from a given shard
// does not conform to the expected schema for the fanout query as a whole.
//
// Notes:

// Suppression rationale: "Multi" is the spelling we want here.
//

import com.microsoft.azure.elasticdb.shard.base.ShardLocation;
import java.io.Serializable;

/**
 * Custom exception thrown when the schema on at least one of the shards
 * participating in the overall query does not conform to the expected schema
 * for the multi-shard query as a whole.
 */
public class MultiShardSchemaMismatchException extends MultiShardException implements Serializable {
  ///#region Custom Constructors

  public MultiShardSchemaMismatchException(ShardLocation shardLocation, String message) {
    super(shardLocation, message);
  }

  ///#endregion Custom Constructors

  ///#region Standard Exception Constructors

  /**
   * Initializes a new instance of the MultiShardSchemaMismatchException class with the specified
   * error message and the reference to the inner exception that is the cause of this exception.
   *
   * @param message specifices the message that explains the reason for the exception.
   * @param innerException specifies the exception encountered at the shard.
   */
  public MultiShardSchemaMismatchException(String message, RuntimeException innerException) {
    super(message, innerException);
  }

  /**
   * Initializes a new instance of the MultiShardSchemaMismatchException class with the specified
   * error message.
   *
   * @param message specifices the message that explains the reason for the exception.
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

  /**
   * Initializes a new instance of the MultiShardSchemaMismatchException class with serialized data.
   *
   * @param info    The <see cref="SerializationInfo"/> see that holds the serialized object data about the exception being thrown.
   * @param context The <see cref="StreamingContext"/> that contains contextual information about the source or destination.
   */
    /*protected MultiShardSchemaMismatchException(SerializationInfo info, StreamingContext context) {
        super(info, context);
    }*/

  ///#endregion Standard Exception Constructors
}
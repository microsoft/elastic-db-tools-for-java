package com.microsoft.azure.elasticdb.query.exception;

/* Copyright (c) Microsoft. All rights reserved.
Licensed under the MIT license. See LICENSE file in the project root for full license information.*/
//
// Purpose:
// Public type to communicate failures when performing operations against a shard

// Suppression rationale: "Multi" is the spelling we want here.
//

import com.microsoft.azure.elasticdb.shard.base.ShardLocation;
import java.io.Serializable;
import java.util.Locale;

/**
 * DEVNOTE: Encapsulate SMM ShardLocation type for now since Shard isn't Serializable Support for
 * serialization of ShardLocation is in the works. A MultiShardException represents an exception
 * that occurred when performing operations against a shard. It provides information about both the
 * identity of the shard and the exception that occurred. Depending on the nature of the exception,
 * one can try re-running the multi-shard query, execute a separate query targeted directly at the
 * shard(s) on that yielded the exception, or lastly execute the query manually against the shard
 * using a common tool such as SSMS.
 */
public class MultiShardException extends RuntimeException implements Serializable {

  private ShardLocation shardLocation;

  ///#region Custom Constructors

  /**
   * Initializes a new instance of the <see cref="MultiShardException"/> class with
   * the specified shard location.
   *
   * @param shardLocation specifies the location of the shard where the exception occurred.
   */
  public MultiShardException(ShardLocation shardLocation) {
    this(shardLocation, String.format("Exception encountered on shard: %1$s", shardLocation));
  }

  /**
   * Initializes a new instance of the <see cref="MultiShardException"/> class with
   * the specified shard location and error message.
   *
   * @param shardLocation specifies the location of the shard where the exception occurred.
   * @param message specifies the message that explains the reason for the exception.
   */
  public MultiShardException(ShardLocation shardLocation, String message) {
    this(shardLocation, message, null);
  }

  /**
   * Initializes a new instance of the <see cref="MultiShardException"/> class with
   * the specified shard location and exception.
   *
   * @param shardLocation specifies the location of the shard where the exception occurred.
   * @param inner specifies the exception encountered at the shard.
   */
  public MultiShardException(ShardLocation shardLocation, RuntimeException inner) {
    this(shardLocation, String.format("Exception encountered on shard: %1$s", shardLocation),
        inner);
  }

  /**
   * Initializes a new instance of the <see cref="MultiShardException"/> class with
   * the specified shard location, error message and exception encountered.
   *
   * @param shardLocation specifies the location of the shard where the exception occurred.
   * @param message specifies the message that explains the reason for the exception.
   * @param inner specifies the exception encountered at the shard.
   * @throws IllegalArgumentException The <paramref name="shardLocation"/> is null
   */
  public MultiShardException(ShardLocation shardLocation, String message, RuntimeException inner) {
    super(message, inner);
    if (null == shardLocation) {
      throw new IllegalArgumentException("shardLocation");
    }

    this.shardLocation = shardLocation;
  }

  ///#endregion Custom Constructors

  ///#region Standard Exception Constructors

  /**
   * Initializes a new instance of the MultiShardException class with the specified error message
   * and the reference to the inner exception that is the cause of this exception.
   *
   * @param message specifies the message that explains the reason for the exception.
   * @param innerException specifies the exception encountered at the shard.
   */
  public MultiShardException(String message, RuntimeException innerException) {
    this(dummyShardLocation(), message, innerException);
  }

  /**
   * Initializes a new instance of the MultiShardException class with the specified error message.
   *
   * @param message specifies the exception encountered at the shard.
   */
  public MultiShardException(String message) {
    this(dummyShardLocation(), message);
  }

  /**
   * Initializes a new instance of the MultiShardException class.
   */
  public MultiShardException() {
    this(dummyShardLocation());
  }

  /**
   * Initializes a new instance of the MultiShardException class with serialized data.
   *
   * @param info    The <see cref="SerializationInfo"/> see that holds the serialized object data
   * about the exception being thrown.
   * @param context The <see cref="StreamingContext"/> that contains contextual information about
   * the source or destination.
   */
  /*protected MultiShardException(SerializationInfo info, StreamingContext context) {
    super(info, context);
    shardLocation = (ShardLocation) (info.GetValue("ShardLocation", ShardLocation.class));
  }*/

  ///#endregion Standard Exception Constructors

  ///#region Serialization Methods

  /**
   * Populates the provided <see cref="SerializationInfo"/> parameter with the data needed to
   * serialize the target object.
   * //@param info <see cref="SerializationInfo"/> object to populate with data.
   * //@param context The destination <see cref=" StreamingContext"/> object for this serialization.
   */
  /*@Override
  public void GetObjectData(SerializationInfo info, StreamingContext context) {
    super.GetObjectData(info, context);
    info.AddValue("ShardLocation", shardLocation);
  }*/

  ///#endregion Serialization Methods
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
    return String
        .format(Locale.getDefault(), "MultiShardException encountered on shard: %1$s %2$s %3$s",
            getShardLocation(), System.lineSeparator(), text);
  }
}
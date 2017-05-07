package com.microsoft.azure.elasticdb.query.multishard;

/* Copyright (c) Microsoft. All rights reserved.
Licensed under the MIT license. See LICENSE file in the project root for full license information.*/

import com.microsoft.azure.elasticdb.query.exception.MultiShardException;
import com.microsoft.azure.elasticdb.shard.base.ShardLocation;
import java.io.Reader;
import java.sql.Connection;

/**
 * Simple, immutable class for affiliating a DbDataReader with additional information related to the
 * reader (e.g. DbCommand, shard, exceptions encountered etc) Useful when grabbing DbDataReaders
 * asynchronously.
 * Purpose: Convenience class that holds a DbDataReader along with a string label for the shard that
 * the data underlying the DbDataReader came from.
 * Notes: This is useful for keeping the DbDataReader and the label together when executing
 * asynchronously.
 */
public class LabeledDbDataReader implements java.io.Closeable {

  /**
   * Whether DbDataReader has been disposed or not.
   */
  private boolean disposed;

  ///#region Constructors
  /**
   * The location of the shard.
   */
  private ShardLocation shardLocation;
  /**
   * The Shard location information.
   */
  private String shardLabel;

  ///#endregion Constructors

  ///#region Internal Properties
  /**
   * The exception encountered when trying to execute against this reader
   * Could be null if the DbDataReader was instantiated successfully for this Shard.
   */
  private MultiShardException exception;
  /**
   * The DbDataReader to keep track of.
   * Could be null if we encountered an exception whilst executing the command against this shard.
   */
  private Reader dbDataReader;
  /**
   * The command object that produces this reader.
   */
  private DbCommand command;

  /**
   * Simple constructor to set up an immutable LabeledDbDataReader object.
   *
   * @param shardLocation The Shard this reader belongs to
   * @param cmd The command object that produced ther reader.
   * @throws IllegalArgumentException If either of the arguments is null.
   */
  public LabeledDbDataReader(MultiShardException exception, ShardLocation shardLocation,
      DbCommand cmd) {
    this(shardLocation, cmd);
    if (null == exception) {
      throw new IllegalArgumentException("exception");
    }

    this.setException(exception);
  }

  private LabeledDbDataReader(ShardLocation shardLocation, DbCommand cmd) {
    if (null == shardLocation) {
      throw new IllegalArgumentException("shardLocation");
    }

    if (null == cmd) {
      throw new IllegalArgumentException("cmd");
    }

    this.setShardLocation(shardLocation);
    this.setShardLabel(getShardLocation().toString());
    this.setCommand(cmd);
  }

  public final ShardLocation getShardLocation() {
    return shardLocation;
  }

  public final void setShardLocation(ShardLocation value) {
    shardLocation = value;
  }

  public final String getShardLabel() {
    return shardLabel;
  }

  public final void setShardLabel(String value) {
    shardLabel = value;
  }

  public final MultiShardException getException() {
    return exception;
  }

  public final void setException(MultiShardException value) {
    exception = value;
  }

  public final Reader getDbDataReader() {
    return dbDataReader;
  }

  public final void setDbDataReader(Reader value) {
    dbDataReader = value;
  }

  /**
   * The DbConnection associated with this reader.
   */
  public final Connection getConnection() {
    return this.getCommand().getConnection();
  }

  public final DbCommand getCommand() {
    return command;
  }

  public final void setCommand(DbCommand value) {
    command = value;
  }

  ///#endregion Internal Properties

  ///#region IDisposable

  /**
   * AutoClosable Implementation.
   */
  public final void close() throws java.io.IOException {
    if (!disposed) {
      //TODO: this.getDbDataReader().Dispose();
      disposed = true;
    }
  }

  ///#endregion IDisposable
}
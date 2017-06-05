package com.microsoft.azure.elasticdb.query.multishard;

/* Copyright (c) Microsoft. All rights reserved.
Licensed under the MIT license. See LICENSE file in the project root for full license information.*/

import com.microsoft.azure.elasticdb.query.exception.MultiShardException;
import com.microsoft.azure.elasticdb.shard.base.ShardLocation;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * Simple, immutable class for affiliating a DbDataReader with additional information related to the
 * reader (e.g. Statement, shard, exceptions encountered etc) Useful when grabbing DbDataReaders
 * asynchronously.
 * Purpose: Convenience class that holds a DbDataReader along with a string label for the shard that
 * the data underlying the DbDataReader came from.
 * Notes: This is useful for keeping the DbDataReader and the label together when executing
 * asynchronously.
 */
public class LabeledResultSet implements AutoCloseable {

  /**
   * Whether DbDataReader has been disposed or not.
   */
  private boolean disposed;

  /**
   * The location of the shard.
   */
  private ShardLocation shardLocation;

  /**
   * The Shard location information.
   */
  private String shardLabel;

  /**
   * The exception encountered when trying to execute against this reader
   * Could be null if the DbDataReader was instantiated successfully for this Shard.
   */
  private MultiShardException exception;

  /**
   * The DbDataReader to keep track of.
   * Could be null if we encountered an exception whilst executing the statement against this shard.
   */
  private ResultSet resultSet;

  /**
   * The statement object that produces this reader.
   */
  private Statement statement;

  /**
   * Simple constructor to set up an immutable LabeledResultSet object.
   *
   * @param shardLocation The Shard this reader belongs to
   * @param statement The statement object that produced the reader.
   * @throws IllegalArgumentException If either of the arguments is null.
   */
  public LabeledResultSet(ResultSet resultSet, ShardLocation shardLocation, Statement statement) {
    this(shardLocation, statement);
    if (null == resultSet) {
      throw new IllegalArgumentException("resultSet");
    }

    this.resultSet = resultSet;
  }

  /**
   * Simple constructor to set up an immutable LabeledResultSet object.
   *
   * @param shardLocation The Shard this reader belongs to
   * @param statement The statement object that produced the reader.
   * @throws IllegalArgumentException If either of the arguments is null.
   */
  public LabeledResultSet(MultiShardException exception, ShardLocation shardLocation,
      Statement statement) {
    this(shardLocation, statement);
    if (null == exception) {
      throw new IllegalArgumentException("exception");
    }

    this.exception = exception;
  }

  /**
   * Simple constructor to set up an immutable LabeledResultSet object.
   *
   * @param shardLocation The Shard this reader belongs to
   * @param statement The statement object that produced the reader.
   * @throws IllegalArgumentException If either of the arguments is null.
   */
  public LabeledResultSet(ShardLocation shardLocation, Statement statement) {
    if (null == shardLocation) {
      throw new IllegalArgumentException("shardLocation");
    }

    if (null == statement) {
      throw new IllegalArgumentException("statement");
    }

    this.shardLocation = shardLocation;
    this.shardLabel = getShardLocation().toString();
    this.statement = statement;
  }

  public final ShardLocation getShardLocation() {
    return shardLocation;
  }

  public final String getShardLabel() {
    return shardLabel;
  }

  public final void setShardLabel(String value) {
    this.shardLabel = value;
  }

  public final MultiShardException getException() {
    return exception;
  }

  public final ResultSet getResultSet() {
    return resultSet;
  }

  /**
   * The DbConnection associated with this reader.
   */
  public final Connection getConnection() {
    try {
      return this.statement.getConnection();
    } catch (SQLException e) {
      e.printStackTrace();
      return null;
    }
  }

  public final Statement getStatement() {
    return statement;
  }

  /**
   * AutoClosable Implementation.
   */
  public final void close() throws SQLException {
    if (!disposed) {
      this.getResultSet().close();
      disposed = true;
    }
  }
}
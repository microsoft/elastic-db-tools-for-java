package com.microsoft.azure.elasticdb.query.exception;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;

/* Copyright (c) Microsoft. All rights reserved.
Licensed under the MIT license. See LICENSE file in the project root for full license information.*/
//
// Purpose:
// Public type that communicates errors that occured across multiple shards

// Suppression rationale: "Multi" is the correct spelling.
//

/**
 * Represents one or more <see cref="Exception"/> errors that occured
 * when executing a query across a shard set. The InnerExceptions field collects
 * these exceptions and one can iterate through the InnerExceptions
 * for further inspection or processing.
 */
public class MultiShardAggregateException extends RuntimeException implements Serializable {

  private ArrayList<RuntimeException> innerExceptions;

  ///#region Standard Exception Constructors

  /**
   * Initializes a new instance of the <see cref="MultiShardAggregateException"/> class.
   */
  public MultiShardAggregateException() {
    this("One or more errors occured across shards");
  }

  /**
   * Initializes a new instance of the <see cref="MultiShardAggregateException"/> class.
   *
   * @param message The error message that explains the reason for the exception
   */
  public MultiShardAggregateException(String message) {
    super(message);
    innerExceptions = new ArrayList();
    innerExceptions.add(new RuntimeException());
  }

  /**
   * Initializes a new instance of the <see cref="MultiShardAggregateException"/> class.
   *
   * @param innerException The <see cref="Exception"/> that caused the current exception
   */
  public MultiShardAggregateException(RuntimeException innerException) {
    this(Collections.singletonList(innerException));
  }

  ///#endregion Standard Exception Constructors

  ///#region Additional Constructors

  /**
   * Initializes a new instance of the <see cref="MultiShardAggregateException"/> class.
   *
   * @param message The error message that explains the reason for the exception
   * @param innerException A list of <see cref="Exception"/> that caused the current exception
   */
  public MultiShardAggregateException(String message, RuntimeException innerException) {
    super(message);
    if (innerExceptions == null) {
      innerExceptions = new ArrayList();
    }
    innerExceptions.add(new RuntimeException());
  }

  /**
   * Initializes a new instance of the <see cref="MultiShardAggregateException"/> class.
   *
   * @param innerExceptions A list of <see cref="Exception"/> that caused the current exception
   */
  public MultiShardAggregateException(List<RuntimeException> innerExceptions) {
    this("One or more errors occured across shards", innerExceptions);
  }

  /**
   * Initializes a new instance of the <see cref="MultiShardAggregateException"/> class.
   *
   * @param message The error message that explains the reason for the exception
   * @param innerExceptions A list of <see cref="Exception"/> that caused the current exception
   * @throws IllegalArgumentException The <paramref name="innerExceptions"/> is null
   */
  public MultiShardAggregateException(String message, List<RuntimeException> innerExceptions) {
    super(message, innerExceptions != null ? innerExceptions.get(0) : null);
    if (null == innerExceptions) {
      throw new IllegalArgumentException("innerExceptions");
    }

    // Put them in a readonly collection
    ArrayList<RuntimeException> exceptions = new ArrayList<>();
    for (RuntimeException exception : innerExceptions) {
      exceptions.add(exception);
    }

    this.innerExceptions = new ArrayList(exceptions);
  }

  ///#endregion Additional Constructors

  ///#region Serialization Support

  /**
   * Populates a SerializationInfo with the data needed to serialize the target object.
   *
   * @param info    The SerializationInfo to populate with data.
   * @param context The destination (see StreamingContext) for this serialization.
   */
  /*@Override
  public void GetObjectData(SerializationInfo info, StreamingContext context) {
    super.GetObjectData(info, context);
    info.AddValue("InnerExceptions", innerExceptions);
  }*/

  ///#endregion Serialization Support

  /**
   * Gets a read-only collection of the <see cref="Exception"/> instances that caused the current
   * exception.
   */
  public final ArrayList<RuntimeException> getInnerExceptions() {
    return innerExceptions;
  }

  /**
   * Provides a string representation of this exception including its inner exceptions.
   */
  @Override
  public String toString() {
    String text = super.toString();

    for (int i = 0; i < innerExceptions.size(); i++) {
      text = String
          .format(Locale.getDefault(), "%1$s%2$s---> (Inner Exception #%3$s) %4$s%5$s%6$s", text,
              System.lineSeparator(), i, innerExceptions.get(i).toString(), "<---",
              System.lineSeparator());
    }

    return text;
  }
}
package com.microsoft.azure.elasticdb.core.commons.logging;

/* Copyright (c) Microsoft. All rights reserved.
Licensed under the MIT license. See LICENSE file in the project root for full license information.*/

/**
 * Summary:
 * Identifies the type of event that has caused the trace.
 */
public enum TraceEventType {
  /**
   * Summary:
   * Fatal error or application crash.
   */
  Critical(1),
  /**
   * Summary:
   * Recoverable error.
   */
  Error(2),
  /**
   * Summary:
   * Noncritical problem.
   */
  Warning(4),
  /**
   * Summary:
   * Informational message.
   */
  Information(8),
  /**
   * Summary:
   * Debugging trace.
   */
  Verbose(16),
  /**
   * Summary:
   * Starting of a logical operation.
   */
  Start(256),
  /**
   * Summary:
   * Stopping of a logical operation.
   */
  Stop(512),
  /**
   * Summary:
   * Suspension of a logical operation.
   */
  Suspend(1024),
  /**
   * Summary:
   * Resumption of a logical operation.
   */
  Resume(2048),
  /**
   * Summary:
   * Changing of correlation identity.
   */
  Transfer(4096);

  public static final int SIZE = java.lang.Integer.SIZE;
  private static java.util.HashMap<Integer, TraceEventType> mappings;
  private int intValue;

  private TraceEventType(int value) {
    intValue = value;
    getMappings().put(value, this);
  }

  private static java.util.HashMap<Integer, TraceEventType> getMappings() {
    if (mappings == null) {
      synchronized (TraceEventType.class) {
        if (mappings == null) {
          mappings = new java.util.HashMap<Integer, TraceEventType>();
        }
      }
    }
    return mappings;
  }

  public static TraceEventType forValue(int value) {
    return getMappings().get(value);
  }

  public int getValue() {
    return intValue;
  }
}

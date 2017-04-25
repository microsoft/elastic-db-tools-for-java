package com.microsoft.azure.elasticdb.core.commons.logging;

// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

/**
 * Summary:
 * Specifies the levels of trace messages filtered by the source switch and event
 * type filter.
 */
public enum SourceLevels {
  /**
   * Summary:
   * Allows all events through.
   */
  All(-1),
  /**
   * Summary:
   * Does not allow any events through.
   */
  Off(0),
  /**
   * Summary:
   * Allows only System.Diagnostics.TraceEventType.Critical events through.
   */
  Critical(1),
  /**
   * Summary:
   * Allows System.Diagnostics.TraceEventType.Critical and System.Diagnostics.TraceEventType.Error
   * events through.
   */
  Error(3),
  /**
   * Summary:
   * Allows System.Diagnostics.TraceEventType.Critical, System.Diagnostics.TraceEventType.Error,
   * and System.Diagnostics.TraceEventType.Warning events through.
   */
  Warning(7),
  /**
   * Summary:
   * Allows System.Diagnostics.TraceEventType.Critical, System.Diagnostics.TraceEventType.Error,
   * System.Diagnostics.TraceEventType.Warning, and System.Diagnostics.TraceEventType.Information
   * events through.
   */
  Information(15),
  /**
   * Summary:
   * Allows System.Diagnostics.TraceEventType.Critical, System.Diagnostics.TraceEventType.Error,
   * System.Diagnostics.TraceEventType.Warning, System.Diagnostics.TraceEventType.Information,
   * and System.Diagnostics.TraceEventType.Verbose events through.
   */
  Verbose(31),
  /**
   * Summary:
   * Allows the System.Diagnostics.TraceEventType.Stop, System.Diagnostics.TraceEventType.Start,
   * System.Diagnostics.TraceEventType.Suspend, System.Diagnostics.TraceEventType.Transfer,
   * and System.Diagnostics.TraceEventType.Resume events through.
   */
  ActivityTracing(65280);

  public static final int SIZE = java.lang.Integer.SIZE;
  private static java.util.HashMap<Integer, SourceLevels> mappings;
  private int intValue;

  private SourceLevels(int value) {
    intValue = value;
    getMappings().put(value, this);
  }

  private static java.util.HashMap<Integer, SourceLevels> getMappings() {
    if (mappings == null) {
      synchronized (SourceLevels.class) {
        if (mappings == null) {
          mappings = new java.util.HashMap<Integer, SourceLevels>();
        }
      }
    }
    return mappings;
  }

  public static SourceLevels forValue(int value) {
    return getMappings().get(value);
  }

  public int getValue() {
    return intValue;
  }
}

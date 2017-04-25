package com.microsoft.azure.elasticdb.core.commons.logging;

/* Copyright (c) Microsoft. All rights reserved.
Licensed under the MIT license. See LICENSE file in the project root for full license information.*/

import java.util.UUID;

public class TraceSource {

  public TraceSource(String traceSourceName) {
    this(traceSourceName, SourceLevels.All);
  }

  public TraceSource(String traceSourceName, SourceLevels defaultLevel) {
  }

  public void TraceInformation(String message) {
  }

  public void TraceInformation(String format, Object[] vars) {
  }

  public void TraceEvent(TraceEventType eventType, int i, String message) {
  }

  public void TraceEvent(TraceEventType eventType, int i, String format, Object[] vars) {
  }

  public void TraceEvent(TraceEventType eventType, int i, String s, String message,
      RuntimeException exception) {
  }

  public void TraceEvent(TraceEventType eventType, int i, String s, String message,
      UUID activityId) {
  }

  public void TraceEvent(TraceEventType start, int i, String s, String method, String fmtMessage,
      UUID activityId) {
  }
}

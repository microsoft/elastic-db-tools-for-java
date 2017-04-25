package com.microsoft.azure.elasticdb.core.commons.logging;

/* Copyright (c) Microsoft. All rights reserved.
Licensed under the MIT license. See LICENSE file in the project root for full license information.*/

/**
 * Simple class that encapsulates an ILogger and allows for custom tracing over an ILogger
 * Trace helper for CrossShardQuery
 */
public final class TraceHelper {

  /**
   * The trace source name for cross shard query
   */
  private static final String MultiShardQueryTraceSourceName = "MultiShardQueryTraceSource";
  public static ILogger Tracer;

  public static void TraceInfo(ILogger logger, String methodName, String message, Object... vars) {
    String fmtMessage = String.format(message, vars);
    logger.Info("Method: {0}; {1}; ActivityId: {2};", methodName, fmtMessage,
        Trace.CorrelationManager.ActivityId);
  }

  public static void TraceWarning(ILogger logger, String methodName, String message,
      Object... vars) {
    String fmtMessage = String.format(message, vars);
    logger.Warning("Method: {0}; {1}; ActivityId: {2};", methodName, fmtMessage,
        Trace.CorrelationManager.ActivityId);
  }

  public static void TraceError(ILogger logger, String methodName, RuntimeException ex,
      String message, Object... vars) {
    String fmtMessage = String.format(message, vars);
    logger.Error("Method: {0}; {1}; Exception: {2}; ActivityId: {3};", methodName, fmtMessage,
        ex.toString(), Trace.CorrelationManager.ActivityId);
  }

  /**
   * Helper to trace at the Verbose TraceLevel to the ILogger
   *
   * @param logger The logger
   * @param componentName The component name
   * @param methodName The method name
   * @param message The formatted message
   * @param vars The args
   */
  public static void TraceVerbose(ILogger logger, String componentName, String methodName,
      String message, Object... vars) {
    String fmtMessage = String.format(message, vars);
    logger.Verbose("{0}.{1}; {2}; ActivityId: {3};", componentName, methodName, fmtMessage,
        Trace.CorrelationManager.ActivityId);
  }

  /**
   * /// Helper to trace at the Information TraceLevel to the ILogger
   *
   * @param logger The logger
   * @param componentName The component name
   * @param methodName The method name
   * @param message The formatted message
   * @param vars The args
   */
  public static void TraceInfo(ILogger logger, String componentName, String methodName,
      String message, Object... vars) {
    String fmtMessage = String.format(message, vars);
    logger.Info("{0}.{1}; {2}; ActivityId: {3};", componentName, methodName, fmtMessage,
        Trace.CorrelationManager.ActivityId);
  }

  /**
   * Helper to trace at the Warning TraceLevel to the ILogger
   *
   * @param logger The logger
   * @param componentName The component name
   * @param methodName The method name
   * @param message The formatted message
   * @param vars The args
   */
  public static void TraceWarning(ILogger logger, String componentName, String methodName,
      String message, Object... vars) {
    String fmtMessage = String.format(message, vars);
    logger.Warning("{0}.{1}; {2}; ActivityId: {3};", componentName, methodName, fmtMessage,
        Trace.CorrelationManager.ActivityId);
  }

  /**
   * Helper to trace at the Error TraceLevel to the ILogger
   *
   * @param logger The logger
   * @param componentName The component name
   * @param methodName The method name
   * @param message The formatted message
   * @param vars The args
   */
  public static void TraceError(ILogger logger, String componentName, String methodName,
      String message, Object... vars) {
    String fmtMessage = String.format(message, vars);
    logger.Error("{0}.{1}; {2}; ActivityId: {3};", componentName, methodName, fmtMessage,
        Trace.CorrelationManager.ActivityId);
  }
}
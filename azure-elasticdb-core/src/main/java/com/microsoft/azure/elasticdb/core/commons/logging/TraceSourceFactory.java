package com.microsoft.azure.elasticdb.core.commons.logging;

// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

/**
 * Concrete implementation of ILogFactory that creates trace sources
 */
public final class TraceSourceFactory implements ILogFactory {

  /**
   * The default source name
   */
  private static final String DefaultTraceSourceName = "DiagnosticsTraceSource";

  /**
   * The source level of the default tracer
   */
  private static final SourceLevels s_defaultSourceLevels = SourceLevels.Information;

  /**
   * The default <see cref="TraceSourceWrapper"/> instance
   */
  private TraceSourceWrapper _defaultDianosticsTraceSource;

  /**
   * Keeps track of various TraceSources
   */
  private java.util.concurrent.ConcurrentHashMap<String, TraceSourceWrapper> _traceSourceDictionary;

  /**
   * Initializes an instance of the <see cref="TraceSourceFactory"/> class
   */
  public TraceSourceFactory() {
    _traceSourceDictionary = new java.util.concurrent.ConcurrentHashMap<String, TraceSourceWrapper>();

    _defaultDianosticsTraceSource = new TraceSourceWrapper(DefaultTraceSourceName,
        s_defaultSourceLevels);
//TODO: There is no Java ConcurrentHashMap equivalent to this .NET ConcurrentDictionary method -> _traceSourceDictionary.put(DefaultTraceSourceName, new TraceSourceWrapper(() -> _defaultDianosticsTraceSource));
  }

  /**
   * Returns the default <see cref="TraceSourceWrapper"/> instance
   *
   * @return An instance of <see cref="TraceSourceWrapper"/>
   */
  public ILogger Create() {
    return _defaultDianosticsTraceSource;
  }

  /**
   * Creates and returns an instance of <see cref="TraceSourceWrapper"/>
   *
   * @param name The name of the TraceSource
   */
  public ILogger Create(String name) {
    return null; // TODO
  }
  // TODO : Add timer based runtime config refresh
}

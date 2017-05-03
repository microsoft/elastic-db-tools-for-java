package com.microsoft.azure.elasticdb.shard.cache;

/* Copyright (c) Microsoft. All rights reserved.
Licensed under the MIT license. See LICENSE file in the project root for full license information.*/

import com.microsoft.azure.elasticdb.core.commons.logging.TraceSourceConstants;
import com.microsoft.azure.elasticdb.shard.utils.StringUtilsLocal;
import java.lang.invoke.MethodHandles;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Wrapper class around PerformanceCounter to catch and trace all exceptions.
 */
public class PerformanceCounterWrapper implements java.io.Closeable {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  public boolean isValid;

  //TODO: private PerformanceCounter _counter;
  private String counterName;
  private String instanceName;
  private String categoryName;

  /**
   * Create and wrap performance counter object.
   *
   * @param categoryName Counter catatory.
   * @param instanceName Instance name to create.
   * @param counterName Counter name to create.
   */
  public PerformanceCounterWrapper(String categoryName, String instanceName, String counterName) {
    isValid = false;
    this.categoryName = categoryName;
    this.instanceName = instanceName;
    this.counterName = counterName;

    // Check if counter exists in the specified category and then create its instance
    //TODO:
    /*if (PerformanceCounterCategory.CounterExists(counterName, categoryName)) {
      try {
        _counter = new PerformanceCounter();
        _counter.InstanceLifetime = PerformanceCounterInstanceLifetime.Process;
        _counter.CategoryName = categoryName;
        _counter.InstanceName = instanceName;
        _counter.CounterName = counterName;
        _counter.ReadOnly = false;

        _counter.RawValue = 0;

        isValid = true;
      } catch (RuntimeException e) {
        PerformanceCounterWrapper.TraceException("initialize",
            "Performance counter initialization failed, no data will be collected.", e);
      }
    } else {
      getTracer().TraceWarning(TraceSourceConstants.ComponentNames.PerfCounter, "initialize",
          "Performance counter {0} does not exist in shard management catagory.", counterName);
    }*/
  }

  /**
   * Log exceptions using Tracer.
   *
   * @param method Method name
   * @param message Custom message
   * @param e Exception to trace out
   */
  private static void traceException(String method, String message, RuntimeException e) {
    log.warn(TraceSourceConstants.ComponentNames.PerfCounter,
        StringUtilsLocal.formatInvariant("Method: %s Message: %s. Exception: %s",
            method, message, e.getMessage()));
  }

  /**
   * Increment counter value by 1.
   */
  public final void increment() {
    if (isValid) {
      try {
        //TODO: _counter.Increment();
      } catch (RuntimeException e) {
        PerformanceCounterWrapper.traceException("increment", "counter increment failed.", e);
      }
    }
  }

  /**
   * Set raw value of this performance counter.
   *
   * @param value Value to set.
   */
  public final void setRawValue(long value) {
    if (isValid) {
      try {
        //TODO: _counter.RawValue = value;
      } catch (RuntimeException e) {
        PerformanceCounterWrapper.traceException("SetRawValue", "failed to set raw value", e);
      }
    }
  }

  /**
   * Dispose performance counter.
   */
  public final void close() throws java.io.IOException {
    //TODO: _counter.Dispose();
  }
}
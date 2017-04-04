package com.microsoft.azure.elasticdb.shard.cache;

// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

import com.microsoft.azure.elasticdb.core.commons.logging.TraceSourceConstants;
import lombok.extern.slf4j.Slf4j;

/**
 * Wrapper class around PerformanceCounter to catch and trace all exceptions.
 */
@Slf4j
public class PerformanceCounterWrapper implements java.io.Closeable {
    public boolean _isValid;

    //TODO: private PerformanceCounter _counter;
    private String _counterName;
    private String _instanceName;
    private String _categoryName;

    /**
     * Create and wrap performance counter object.
     *
     * @param categoryName Counter catatory.
     * @param instanceName Instance name to create.
     * @param counterName  Counter name to create.
     */
    public PerformanceCounterWrapper(String categoryName, String instanceName, String counterName) {
        _isValid = false;
        this._categoryName = categoryName;
        this._instanceName = instanceName;
        this._counterName = counterName;

        // Check if counter exists in the specified category and then create its instance
        //TODO:
        /*if (PerformanceCounterCategory.CounterExists(_counterName, _categoryName)) {
            try {
				_counter = new PerformanceCounter();
				_counter.InstanceLifetime = PerformanceCounterInstanceLifetime.Process;
				_counter.CategoryName = _categoryName;
				_counter.InstanceName = _instanceName;
				_counter.CounterName = _counterName;
				_counter.ReadOnly = false;

				_counter.RawValue = 0;

				_isValid = true;
			} catch (RuntimeException e) {
				PerformanceCounterWrapper.TraceException("initialize", "Performance counter initialization failed, no data will be collected.", e);
			}
		} else {
			getTracer().TraceWarning(TraceSourceConstants.ComponentNames.PerfCounter, "initialize", "Performance counter {0} does not exist in shard management catagory.", counterName);
		}*/
    }

    /**
     * Log exceptions using Tracer
     *
     * @param method  Method name
     * @param message Custom message
     * @param e       Exception to trace out
     */
    private static void TraceException(String method, String message, RuntimeException e) {
        log.warn(TraceSourceConstants.ComponentNames.PerfCounter, String.format("Method:{} Message: {}. Exception: {}", method, message, e.getMessage()));
    }

    /**
     * Close performance counter, if initialized earlier. Counter will be removed when we delete instance.
     */
    public final void Close() {
        if (_isValid) {
            //TODO: _counter.Close();
        }
    }

    /**
     * Increment counter value by 1.
     */
    public final void Increment() {
        if (_isValid) {
            try {
                //TODO: _counter.Increment();
            } catch (RuntimeException e) {
                PerformanceCounterWrapper.TraceException("increment", "counter increment failed.", e);
            }
        }
    }

    /**
     * Set raw value of this performance counter.
     *
     * @param value Value to set.
     */
    public final void SetRawValue(long value) {
        if (_isValid) {
            try {
                //TODO: _counter.RawValue = value;
            } catch (RuntimeException e) {
                PerformanceCounterWrapper.TraceException("SetRawValue", "failed to set raw value", e);
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
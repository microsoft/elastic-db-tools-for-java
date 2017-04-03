package com.microsoft.azure.elasticdb.core.commons.logging;

// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

/**
 * Generic pluggable logger factory for retrieving configured logging objects
 */
public final class LoggerFactory {
    ///#region "Singleton Implementation"

    private static TraceSourceFactory s_loggerFactory = null;

    private static Object s_lockObj = new Object();

    private static ILogFactory get_factory() {
        if (s_loggerFactory == null) {
            synchronized (s_lockObj) {
                if (s_loggerFactory == null) {
                    s_loggerFactory = new TraceSourceFactory();
                }
            }
        }

        return s_loggerFactory;
    }

    ///#endregion

    public static <T> ILogger GetLoggerGeneric() {
        // TODO: Get Generic class name
        return get_factory().Create("");
    }

    public static ILogger GetLogger() {
        return get_factory().Create();
    }

    public static ILogger GetLogger(String logName) {
        return get_factory().Create(logName);
    }
}
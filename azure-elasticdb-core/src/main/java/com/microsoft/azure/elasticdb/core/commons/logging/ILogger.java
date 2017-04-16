package com.microsoft.azure.elasticdb.core.commons.logging;

// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

import java.util.UUID;

/**
 * Definition of a generic logging interface to abstract
 * implementation details.  Includes api trace methods.
 */
public interface ILogger {
    void Info(String message);

    void Info(String format, Object... vars);

    void Verbose(String message);

    void Verbose(String format, Object... vars);

    void Warning(String message);

    void Warning(RuntimeException exception, String message);

    void Warning(String format, Object... vars);

    void Warning(RuntimeException exception, String format, Object... vars);

    void Error(String message);

    void Error(RuntimeException exception, String message);

    void Error(String format, Object... vars);

    void Error(RuntimeException exception, String format, Object... vars);

    void Critical(String message);

    void Critical(RuntimeException exception, String message);

    void Critical(String format, Object... vars);

    void Critical(RuntimeException exception, String format, Object... vars);

    void TraceIn(String method, UUID activityId);

    void TraceOut(String method, UUID activityId);

    void TraceIn(String method, UUID activityId, String format, Object... vars);

    void TraceOut(String method, UUID activityId, String format, Object... vars);

    void TraceInfo(String shardMapManagerFactory, String createSqlShardMapManager, String s);

    void TraceWarning(String perfCounter, String method, String format);

    void TraceVerbose(String shardMapManagerFactory, String operationName, String s);

    void TraceVerbose(String shardMapManager, String onAddOrUpdateShardMap, String s, String name);

    void TraceVerbose(String shardMapManager, String onAddOrUpdateShardMap, String s, UUID id);

    void TraceVerbose(String shardMapManager, String lookupShardMapByNameInCache, String s, String s1, String shardMapName);
}

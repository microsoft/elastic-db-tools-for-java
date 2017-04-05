package com.microsoft.azure.elasticdb.core.commons.logging;

// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

import java.util.UUID;

/**
 * System.Diagnostics TraceSource implementation of the ILogger interface
 */
public class TraceSourceWrapper implements ILogger {
    /**
     * The trace source instance
     */
    private TraceSource _traceSource;

    ///#region Ctors

    /**
     * Creates an instance of the <see cref="TraceSourceWrapper"/>
     *
     * @param traceSourceName The TraceSource name
     */
    public TraceSourceWrapper(String traceSourceName) {
        _traceSource = new TraceSource(traceSourceName);
    }

    /**
     * Creates an instance of the <see cref="TraceSourceWrapper"/>
     *
     * @param traceSourceName The TraceSource name
     * @param defaultLevel    The default TraceSource level to use
     */
    public TraceSourceWrapper(String traceSourceName, SourceLevels defaultLevel) {
        _traceSource = new TraceSource(traceSourceName, defaultLevel);
    }

    ///#endregion

    ///#region Information

    /**
     * Traces an informational message to the trace source
     *
     * @param message The trace source
     */
    public final void Info(String message) {
        _traceSource.TraceInformation(message);
    }

    /**
     * Traces an informational message to the trace source
     *
     * @param format The format
     * @param vars   The args
     */
    public final void Info(String format, Object... vars) {
        _traceSource.TraceInformation(format, vars);
    }

    ///#endregion Information

    ///#region Verbose

    /**
     * Traces a Verbose message to the trace source
     *
     * @param message
     */
    public final void Verbose(String message) {
        _traceSource.TraceEvent(TraceEventType.Verbose, 0, message);
    }

    /**
     * Traces a Verbose message to the trace source
     *
     * @param format
     * @param vars
     */
    public final void Verbose(String format, Object... vars) {
        _traceSource.TraceEvent(TraceEventType.Verbose, 0, format, vars);
    }

    ///#endregion

    ///#region Warning

    /**
     * Traces a message at the Warning level to the trace source
     *
     * @param message
     */
    public final void Warning(String message) {
        _traceSource.TraceEvent(TraceEventType.Warning, 0, message);
    }

    /**
     * Traces a message at the Warning level to the trace source
     *
     * @param format
     * @param vars
     */
    public final void Warning(String format, Object... vars) {
        _traceSource.TraceEvent(TraceEventType.Warning, 0, format, vars);
    }

    /**
     * Traces an exception and a message at the Warning trace level
     * to the trace source
     *
     * @param exception
     * @param message
     */
    public final void Warning(RuntimeException exception, String message) {
        _traceSource.TraceEvent(TraceEventType.Warning, 0, "{0}. Encountered exception: {1}", message, exception);
    }

    /**
     * Traces an exception and a message at the Warning trace level
     * to the trace source
     *
     * @param exception
     * @param format
     * @param vars
     */
    public final void Warning(RuntimeException exception, String format, Object... vars) {
        String fmtMessage = String.format(format, vars);
        _traceSource.TraceEvent(TraceEventType.Warning, 0, "{0}. Encountered exception: {1}", fmtMessage, exception);
    }

    ///#endregion Warning

    ///#region Error

    /**
     * Traces the message at the Error trace level to the trace source
     *
     * @param message
     */
    public final void Error(String message) {
        _traceSource.TraceEvent(TraceEventType.Error, 0, message);
    }

    /**
     * Traces the message at the Error trace level to the trace source
     *
     * @param format
     * @param vars
     */
    public final void Error(String format, Object... vars) {
        _traceSource.TraceEvent(TraceEventType.Error, 0, format, vars);
    }

    /**
     * Traces the exception and message
     * at the Error level to the trace source
     *
     * @param exception
     * @param message
     */
    public final void Error(RuntimeException exception, String message) {
        _traceSource.TraceEvent(TraceEventType.Error, 0, "{0}. Encountered exception: {1}", message, exception);
    }

    /**
     * Traces the exception and message at
     * the Error level to the trace source
     *
     * @param exception
     * @param format
     * @param vars
     */
    public final void Error(RuntimeException exception, String format, Object... vars) {
        String fmtMessage = String.format(format, vars);
        _traceSource.TraceEvent(TraceEventType.Error, 0, "{0}. Encountered exception: {1}", fmtMessage, exception);
    }

    ///#endregion Error

    ///#region Critical

    /**
     * Traces the message at the Critical source level to the trace source
     *
     * @param message
     */
    public final void Critical(String message) {
        _traceSource.TraceEvent(TraceEventType.Critical, 0, message);
    }

    /**
     * Traces the message at the Critical source level to the trace source
     *
     * @param format
     * @param vars
     */
    public final void Critical(String format, Object... vars) {
        _traceSource.TraceEvent(TraceEventType.Critical, 0, format, vars);
    }

    /**
     * Traces the message and exception at the Critical source level
     * to the trace source
     *
     * @param exception
     * @param message
     */
    public final void Critical(RuntimeException exception, String message) {
        _traceSource.TraceEvent(TraceEventType.Critical, 0, "{0}. Exception encountered: {1}", message, exception);
    }

    /**
     * Traces the message and exception at the Critical source level
     * to the trace source
     *
     * @param exception
     * @param format
     * @param vars
     */
    public final void Critical(RuntimeException exception, String format, Object... vars) {
        String fmtMessage = String.format(format, vars);
        _traceSource.TraceEvent(TraceEventType.Critical, 0, "{0}. Exception encountered: {1}", fmtMessage, exception);
    }

    ///#endregion Critical

    ///#region Enter/Exit

    /**
     * Traces the entry of the method
     *
     * @param method
     * @param activityId
     */
    public final void TraceIn(String method, UUID activityId) {
        _traceSource.TraceEvent(TraceEventType.Start, 0, "Start.{0}. ActivityId: {1}", method, activityId);
    }

    /**
     * Traces the exit of the method
     *
     * @param method
     * @param activityId
     */
    public final void TraceOut(String method, UUID activityId) {
        _traceSource.TraceEvent(TraceEventType.Stop, 0, "Stop.{0}. ActivityId: {1}", method, activityId);
    }

    /**
     * Traces the entry of the method
     *
     * @param method
     * @param activityId
     * @param format
     * @param vars
     */
    public final void TraceIn(String method, UUID activityId, String format, Object... vars) {
        String fmtMessage = String.format(format, vars);
        _traceSource.TraceEvent(TraceEventType.Start, 0, "Start.{0}. {1}. ActivityId: {2}", method, fmtMessage, activityId);
    }

    /**
     * Traces the exit of the method
     *
     * @param method
     * @param activityId
     * @param format
     * @param vars
     */
    public final void TraceOut(String method, UUID activityId, String format, Object... vars) {
        String fmtMessage = String.format(format, vars);
        _traceSource.TraceEvent(TraceEventType.Stop, 0, "Stop.{0}. {1}. ActivityId: {2}", method, fmtMessage, activityId);
    }

    @Override
    public void TraceInfo(String shardMapManagerFactory, String createSqlShardMapManager, String s) {

    }

    @Override
    public void TraceWarning(String perfCounter, String method, String format) {

    }

    @Override
    public void TraceVerbose(String shardMapManagerFactory, String operationName, String s) {

    }

    @Override
    public void TraceVerbose(String shardMapManager, String onAddOrUpdateShardMap, String s, String name) {

    }

    @Override
    public void TraceVerbose(String shardMapManager, String onAddOrUpdateShardMap, String s, UUID id) {

    }

    @Override
    public void TraceVerbose(String shardMapManager, String lookupShardMapByNameInCache, String s, String s1, String shardMapName) {

    }

    ///#endregion
}
package com.microsoft.azure.elasticdb.core.commons.logging;

/* Copyright (c) Microsoft. All rights reserved.
Licensed under the MIT license. See LICENSE file in the project root for full license information.*/

/**
 * Encapsulates various constants related to TraceSource
 */
public final class TraceSourceConstants {

  /**
   * The TraceSource name for the ShardMapManager library
   */
  public static final String ShardManagementTraceSource = "ShardManagementTraceSource";

  /**
   * Component names to use while tracing
   */
  public static class ComponentNames {

    /**
     * The ShardMapManagerFactory component name
     */
    public static final String ShardMapManagerFactory = "ShardMapManagerFactory";

    /**
     * The ShardMapManager component name
     */
    public static final String ShardMapManager = "ShardMapManager";

    /**
     * The ShardMap component name
     */
    public static final String ShardMap = "ShardMap";

    /**
     * The ListShardMap component name
     */
    public static final String ListShardMap = "ListShardMap";

    /**
     * The RangeShardMap component name
     */
    public static final String RangeShardMap = "RangeShardMap";

    /**
     * The DefaultShardMapper component name
     */
    public static final String DefaultShardMapper = "DefaultShardMapper";

    /**
     * The BaseShardMapper name
     */
    public static final String BaseShardMapper = "BaseShardMapper";

    /**
     * The ListShardMaper name
     */
    public static final String ListShardMapper = "ListShardMapper";

    /**
     * The RangeShardMapper name
     */
    public static final String RangeShardMapper = "RangeShardMapper";

    /**
     * The SqlStore component name
     */
    public static final String SqlStore = "SqlStore";

    /**
     * The Cache component name
     */
    public static final String Cache = "Cache";

    /**
     * The Shard Component name
     */
    public static final String Shard = "Shard";

    /**
     * The PointMapping component name
     */
    public static final String PointMapping = "PointMapping";

    /**
     * The RangeMapping component name
     */
    public static final String RangeMapping = "RangeMapping";

    /**
     * The performance counter component name
     */
    public static final String PerfCounter = "PerfCounter";
  }
}
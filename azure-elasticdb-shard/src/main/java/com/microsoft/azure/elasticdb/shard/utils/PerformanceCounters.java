package com.microsoft.azure.elasticdb.shard.utils;

/* Copyright (c) Microsoft. All rights reserved.
Licensed under the MIT license. See LICENSE file in the project root for full license information.*/

public class PerformanceCounters {

  public static String DdrOperationsPerSecDisplayName = "DDR operations/sec";
  public static String DdrOperationsPerSecHelpText = "Rate of data dependent routing (DDR)"
      + " operations for this shard map";
  public static String MappingsAddOrUpdatePerSecDisplayName = "Mappings added or updated in"
      + " cache/sec";
  public static String MappingsAddOrUpdatePerSecHelpText = "Rate at which mappings are being added"
      + " or updated in cache for this shard map";
  public static String MappingsCountDisplayName = "Cached mappings";
  public static String MappingsCountHelpText = "Number of mappings cached for this shard map";
  public static String MappingsLookupFailedPerSecDisplayName = "Mapping lookup cache misses/sec";
  public static String MappingsLookupFailedPerSecHelpText = "Rate of failed cache lookup operations"
      + " for mappings in this shard map";
  public static String MappingsLookupSucceededPerSecDisplayName = "Mapping lookup cache hits/sec";
  public static String MappingsLookupSucceededPerSecHelpText = "Rate of successful cache lookup"
      + " operations for mappings in this shard map";
  public static String MappingsRemovePerSecDisplayName = "Mappings removed from cache/sec";
  public static String MappingsRemovePerSecHelpText = "Rate at which mappings are being removed"
      + " from cache for this shard map";
  public static String PerformanceMonitorUsersGroupName = "Performance Monitor Users";
  public static String ShardManagementPerformanceCounterCategory = "Elastic Database:"
      + " Shard Management";
  public static String ShardManagementPerformanceCounterCategoryHelp = "Performance counters for"
      + " tracking shard management operations and caching";

}

package com.microsoft.azure.elasticdb.shard.utils;

// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

/**
 * Definition of globally useful constants.
 */
public class GlobalConstants {
    /**
     * Default locking timeout value for application locks.
     */
    public static final int DefaultLockTimeOut = 60 * 1000;
    /**
     * Maximum length of shard map name.
     */
    public static final int MaximumShardMapNameLength = 50;
    /**
     * Maximum length of ApplicationName.
     */
    public static final int MaximumApplicationNameLength = 128;
    /**
     * Maximum size of shard map name.
     */
    public static final int MaximumShardMapNameSize = MaximumShardMapNameLength * 2;
    /**
     * Maximum length of shard key.
     */
    public static final int MaximumShardKeyLength = 128;
    /**
     * Maximum size of shard key.
     */
    public static final int MaximumShardKeySize = MaximumShardKeyLength;
    /**
     * Maximum length for a server.
     */
    public static final int MaximumServerLength = 128;
    /**
     * Maximum size for a server.
     */
    public static final int MaximumServerSize = MaximumServerLength * 2;
    /**
     * Maximum length for a database.
     */
    public static final int MaximumDatabaseLength = 128;
    /**
     * Maximum size for a database.
     */
    public static final int MaximumDatabaseSize = MaximumDatabaseLength * 2;
    /**
     * GSM version of store supported by this library.
     */
    public static Version GsmVersionClient = new Version(1, 2);
    /**
     * LSM version of store supported by this library.
     */
    public static Version LsmVersionClient = new Version(1, 2);
    /**
     * Version information for Microsoft.Azure.SqlDatabase.ElasticScale.ShardManagement code
     */
    //TODO: ElasticScaleVersionInfo.ProductVersion
    public static String ShardManagementVersionInfo = "1.0.0.0";
    /**
     * Prefix for ShardMapManager in ApplicationName for user connections.
     */
    public static final String ShardMapManagerPrefix = "ESC_SMMv" + ShardManagementVersionInfo + "_User";
    /**
     * ShardMapManager ApplicationName for Storage connections.
     */
    public static final String ShardMapManagerInternalConnectionSuffixGlobal = "ESC_SMMv" + ShardManagementVersionInfo + "_GSM";
    /**
     * ShardMapManager ApplicationName for Storage connections.
     */
    public static final String ShardMapManagerInternalConnectionSuffixLocal = "ESC_SMMv" + ShardManagementVersionInfo + "_LSM";
}
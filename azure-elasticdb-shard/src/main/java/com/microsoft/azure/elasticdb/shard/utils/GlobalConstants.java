package com.microsoft.azure.elasticdb.shard.utils;

public class GlobalConstants {
    //TODO: Need to implement our own Version
//    /// <summary>
//    /// GSM version of store supported by this library.
//    /// </summary>
//    public final static Version GsmVersionClient = new Version(1, 2);

    //    /// <summary>
//    /// LSM version of store supported by this library.
//    /// </summary>
//    public final static Version LsmVersionClient = new Version(1, 2);

    /// <summary>
    /// Version information for Microsoft.Azure.SqlDatabase.ElasticScale.ShardManagement code
    /// </summary>
    public final static String ShardManagementVersionInfo = "1.0.0";//ElasticScaleVersionInfo.ProductVersion; //TODO:AssemblyInfo.cs

    /// <summary>
    /// Default locking timeout value for application locks.
    /// </summary>
    public final static int DefaultLockTimeOut = 60 * 1000;

    /// <summary>
    /// Prefix for ShardMapManager in ApplicationName for user connections.
    /// </summary>
    public final static String ShardMapManagerPrefix = "ESC_SMMv" + ShardManagementVersionInfo + "_User";

    /// <summary>
    /// ShardMapManager ApplicationName for Storage connections.
    /// </summary>
    public final static String ShardMapManagerInternalConnectionSuffixGlobal = "ESC_SMMv" + ShardManagementVersionInfo + "_GSM";

    /// <summary>
    /// ShardMapManager ApplicationName for Storage connections.
    /// </summary>
    public final static String ShardMapManagerInternalConnectionSuffixLocal = "ESC_SMMv" + ShardManagementVersionInfo + "_LSM";

    /// <summary>
    /// Maximum length of shard map name.
    /// </summary>
    public final static int MaximumShardMapNameLength = 50;

    /// <summary>
    /// Maximum length of ApplicationName.
    /// </summary>
    public final static int MaximumApplicationNameLength = 128;

    /// <summary>
    /// Maximum size of shard map name.
    /// </summary>
    public final static int MaximumShardMapNameSize = MaximumShardMapNameLength * 2;

    /// <summary>
    /// Maximum length of shard key.
    /// </summary>
    public final static int MaximumShardKeyLength = 128;

    /// <summary>
    /// Maximum size of shard key.
    /// </summary>
    public final static int MaximumShardKeySize = MaximumShardKeyLength;

    /// <summary>
    /// Maximum length for a server.
    /// </summary>
    public final static int MaximumServerLength = 128;

    /// <summary>
    /// Maximum size for a server.
    /// </summary>
    public final static int MaximumServerSize = MaximumServerLength * 2;

    /// <summary>
    /// Maximum length for a database.
    /// </summary>
    public final static int MaximumDatabaseLength = 128;

    /// <summary>
    /// Maximum size for a database.
    /// </summary>
    public final static int MaximumDatabaseSize = MaximumDatabaseLength * 2;
}

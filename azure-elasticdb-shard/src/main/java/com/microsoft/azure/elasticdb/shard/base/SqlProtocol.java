package com.microsoft.azure.elasticdb.shard.base;

/// <summary>
/// Types of transport protocols supported in SQL Server connections.
/// </summary>
public enum SqlProtocol
{
    /// <summary>
    /// Default protocol.
    /// </summary>
    Default,

    /// <summary>
    /// TCP/IP protocol.
    /// </summary>
    Tcp,

    /// <summary>
    /// Named pipes protocol.
    /// </summary>
    NamedPipes,

    /// <summary>
    /// Shared memory protocol.
    /// </summary>
    SharedMemory
}

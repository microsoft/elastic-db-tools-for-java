package com.microsoft.azure.elasticdb.shard.base;

/**
 * Types of transport protocols supported in SQL Server connections.
 */
public enum SqlProtocol
{
    Default,

    Tcp,

    NamedPipes,

    SharedMemory
}

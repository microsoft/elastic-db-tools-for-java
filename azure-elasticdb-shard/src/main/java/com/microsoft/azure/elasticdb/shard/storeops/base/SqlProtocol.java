package com.microsoft.azure.elasticdb.shard.storeops.base;

// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

/// <summary>
/// Types of transport protocols supported in SQL Server connections.
/// </summary>
public enum SqlProtocol {
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

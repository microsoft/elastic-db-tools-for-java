package com.microsoft.azure.elasticdb.core.commons.logging;

// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

/**
 * ILogFactory interface to be implemented for
 * each logger
 */
public interface ILogFactory {
    ILogger Create();

    ILogger Create(String name);
}
package com.microsoft.azure.elasticdb.core.commons.helpers;

// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

@FunctionalInterface
public interface ActionGeneric<TResult> {
    TResult invoke();
}
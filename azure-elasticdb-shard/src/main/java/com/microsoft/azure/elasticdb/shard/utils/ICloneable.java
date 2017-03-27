package com.microsoft.azure.elasticdb.shard.utils;

// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

/**
 * Represents objects that can clone themselves.
 * <typeparam name="T">Type of object</typeparam>
 */
public interface ICloneable<T extends ICloneable<T>> {
    /**
     * Clones the instance which implements the interface.
     *
     * @return Clone of the instance.
     */
    T Clone();
}
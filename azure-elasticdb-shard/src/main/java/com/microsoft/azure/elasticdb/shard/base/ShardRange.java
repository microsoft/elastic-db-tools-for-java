package com.microsoft.azure.elasticdb.shard.base;

// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

/**
 * A range of shard keys between a low key and a high key.
 * <p>
 * The low key is inclusive (part of the range) while the high key is exclusive
 * (not part of the range). The ShardRange class is immutable.
 */
public final class ShardRange implements java.lang.Comparable<ShardRange> {

    public int compareTo(ShardRange o) {
        return 0;
    }
}

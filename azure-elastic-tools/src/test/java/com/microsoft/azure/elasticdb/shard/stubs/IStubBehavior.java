package com.microsoft.azure.elasticdb.shard.stubs;

/*
 * Copyright (c) Microsoft. All rights reserved. Licensed under the MIT license. See LICENSE file in the project root for full license information.
 */

public class IStubBehavior {

    public final <T, ReturnT> ReturnT result(T obj,
            String name) {
        throw new UnsupportedOperationException();
    }

    public final <T> void voidResult(T obj,
            String name) {
        throw new UnsupportedOperationException();
    }
}
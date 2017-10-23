package com.microsoft.azure.elasticdb.shard.stubhelper;

/*
 * Copyright (c) Microsoft. All rights reserved. Licensed under the MIT license. See LICENSE file in the project root for full license information.
 */

@FunctionalInterface
public interface Func0Param<ResultT> {

    ResultT invoke();
}
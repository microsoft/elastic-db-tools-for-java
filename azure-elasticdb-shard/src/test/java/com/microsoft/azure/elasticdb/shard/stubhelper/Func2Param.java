package com.microsoft.azure.elasticdb.shard.stubhelper;

/*Copyright (c) Microsoft. All rights reserved.
Licensed under the MIT license. See LICENSE file in the project root for full license information.*/

@FunctionalInterface
public interface Func2Param<T1, T2, TResult> {

  TResult invoke(T1 t1, T2 t2);
}
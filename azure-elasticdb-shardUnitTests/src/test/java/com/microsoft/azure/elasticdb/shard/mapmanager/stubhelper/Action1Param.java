package com.microsoft.azure.elasticdb.shard.mapmanager.stubhelper;

/*Copyright (c) Microsoft. All rights reserved.
Licensed under the MIT license. See LICENSE file in the project root for full license information.*/

@FunctionalInterface
public interface Action1Param<T> {

  void invoke(T t);
}
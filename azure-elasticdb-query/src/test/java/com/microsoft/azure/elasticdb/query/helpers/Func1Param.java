package com.microsoft.azure.elasticdb.query.helpers;

/*Copyright (c) Microsoft. All rights reserved.
Licensed under the MIT license. See LICENSE file in the project root for full license information.*/

@FunctionalInterface
public interface Func1Param<T, ResultT> {

  ResultT invoke(T t);
}
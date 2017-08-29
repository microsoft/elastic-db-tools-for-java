package com.microsoft.azure.elasticdb.core.commons.helpers;

/* Copyright (c) Microsoft. All rights reserved.
Licensed under the MIT license. See LICENSE file in the project root for full license information.*/

@FunctionalInterface
public interface ActionGeneric3Param<T1, T2, T3, ResultT> {

  ResultT invoke(T1 t1, T2 t2, T3 t3);
}
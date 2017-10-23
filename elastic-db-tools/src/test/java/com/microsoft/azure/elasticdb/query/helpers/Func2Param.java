package com.microsoft.azure.elasticdb.query.helpers;

/*
 * Elastic database tools for Azure SQL Database.
 * 
 * Copyright(c) Microsoft Corporation All rights reserved.
 * 
 * This program is made available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

@FunctionalInterface
public interface Func2Param<T1, T2, ResultT> {

    ResultT invoke(T1 t1,
            T2 t2);
}
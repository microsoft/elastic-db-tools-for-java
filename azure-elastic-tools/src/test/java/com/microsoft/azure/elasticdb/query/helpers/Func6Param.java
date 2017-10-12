package com.microsoft.azure.elasticdb.query.helpers;

/*
 * Elastic database tools for Azure SQL Database.
 * 
 * Copyright(c) Microsoft Corporation All rights reserved.
 * 
 * This program is made available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

@FunctionalInterface
public interface Func6Param<T1, T2, T3, T4, T5, T6, ResultT> {

    ResultT invoke(T1 t1,
            T2 t2,
            T3 t3,
            T4 t4,
            T5 t5,
            T6 t6);
}
package com.microsoft.azure.elasticdb.shard.recovery;

/*
 * Elastic database tools for Azure SQL Database.
 * 
 * Copyright(c) Microsoft Corporation All rights reserved.
 * 
 * This program is made available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

/**
 * Type of mapping difference. Useful for down casting.
 */
public enum MappingDifferenceType {
    /**
     * Violation associated with ListShardMap.
     */
    List,

    /**
     * Violation associated with RangeShardMap.
     */
    Range;

}

package com.microsoft.azure.elasticdb.shard.schema;

/*
 * Elastic database tools for Azure SQL Database.
 * 
 * Copyright(c) Microsoft Corporation All rights reserved.
 * 
 * This program is made available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

/**
 * Possible errors encountered by SchemaInfoCollection.
 */
public enum SchemaInfoErrorCode {
    /**
     * No <see cref="SchemaInfo"/> exists with the given name.
     */
    SchemaInfoNameDoesNotExist,

    /**
     * A <see cref="SchemaInfo"/> entry with the given name already exists.
     */
    SchemaInfoNameConflict,

    /**
     * An entry for the given table already exists in the <see cref="SchemaInfo"/> object.
     */
    TableInfoAlreadyPresent;

}
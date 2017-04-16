package com.microsoft.azure.elasticdb.shard.utils;

// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

//
// Summary:
//     Specifies serialization options.
public enum SaveOptions {
    //
    // Summary:
    //     Format (indent) the XML while serializing.
    None(0),
    //
    // Summary:
    //     Preserve all insignificant white space while serializing.
    DisableFormatting(1),
    //
    // Summary:
    //     Remove the duplicate namespace declarations while serializing.
    OmitDuplicateNamespaces(2);

    public static final int SIZE = java.lang.Integer.SIZE;
    private static java.util.HashMap<Integer, SaveOptions> mappings;
    private int intValue;

    private SaveOptions(int value) {
        intValue = value;
        getMappings().put(value, this);
    }

    private static java.util.HashMap<Integer, SaveOptions> getMappings() {
        if (mappings == null) {
            synchronized (SaveOptions.class) {
                if (mappings == null) {
                    mappings = new java.util.HashMap<Integer, SaveOptions>();
                }
            }
        }
        return mappings;
    }

    public static SaveOptions forValue(int value) {
        return getMappings().get(value);
    }

    public int getValue() {
        return intValue;
    }
}

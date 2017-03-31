package com.microsoft.azure.elasticdb.shard.utils;

// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

//
// Summary:
//     Represents a name of an XML element or attribute.
public final class XName {
    /**
     * Summary:
     * Gets the local (unqualified) part of the name.
     * <p>
     * Returns:
     * A String that contains the local (unqualified) part of the name.
     */
    private String LocalName;


    /**
     * Summary:
     * Initializes a new instance of the XName class with the specified name.
     * <p>
     * Parameters:
     * name:
     * A String that contains the name of the element.
     */
    public XName(String name) {
        this.LocalName = name;
    }

    public String getLocalName() {
        return LocalName;
    }
}
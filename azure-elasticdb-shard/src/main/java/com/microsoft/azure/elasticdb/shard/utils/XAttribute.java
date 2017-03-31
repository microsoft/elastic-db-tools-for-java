package com.microsoft.azure.elasticdb.shard.utils;

// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

import java.util.*;

//
// Summary:
//     Represents an XML attribute.
public class XAttribute {
    /**
     * Summary:
     * Gets an empty collection of attributes.
     * <p>
     * Returns:
     * An System.Collections.Generic.IEnumerable`1 of System.Xml.Linq.XAttribute containing
     * an empty collection.
     */
    private static List<XAttribute> EmptySequence;
    /**
     * Summary:
     * Determines if this attribute is a namespace declaration.
     * <p>
     * Returns:
     * true if this attribute is a namespace declaration; otherwise false.
     */
    private boolean IsNamespaceDeclaration;
    /**
     * Summary:
     * Gets the expanded name of this attribute.
     * <p>
     * Returns:
     * An System.Xml.Linq.XName containing the name of this attribute.
     */
    private XName Name;
    /**
     * Summary:
     * Gets the next attribute of the parent element.
     * <p>
     * Returns:
     * An System.Xml.Linq.XAttribute containing the next attribute of the parent element.
     */
    private XAttribute NextAttribute;
    /**
     * Summary:
     * Gets the previous attribute of the parent element.
     * <p>
     * Returns:
     * An System.Xml.Linq.XAttribute containing the previous attribute of the parent
     * element.
     */
    private XAttribute PreviousAttribute;
    /**
     * Summary:
     * Gets or sets the value of this attribute.
     * <p>
     * Returns:
     * A System.String containing the value of this attribute.
     * <p>
     * Exceptions:
     * T:System.ArgumentNullException:
     * When setting, the value is null.
     */
    private String Value;

    /**
     * Summary:
     * Initializes a new instance of the System.Xml.Linq.XAttribute class from another
     * System.Xml.Linq.XAttribute object.
     * <p>
     * Parameters:
     * other:
     * An System.Xml.Linq.XAttribute object to copy from.
     * <p>
     * Exceptions:
     * T:System.ArgumentNullException:
     * The other parameter is null.
     */
    public XAttribute(XAttribute other) {
    }

    /**
     * Summary:
     * Initializes a new instance of the System.Xml.Linq.XAttribute class from the specified
     * name and value.
     * <p>
     * Parameters:
     * name:
     * The System.Xml.Linq.XName of the attribute.
     * <p>
     * value:
     * An System.Object containing the value of the attribute.
     * <p>
     * Exceptions:
     * T:System.ArgumentNullException:
     * The name or value parameter is null.
     */
    public XAttribute(XName name, Object value) {
    }

    public static List<XAttribute> getEmptySequence() {
        return EmptySequence;
    }

    public final boolean getIsNamespaceDeclaration() {
        return IsNamespaceDeclaration;
    }

    public final XName getName() {
        return Name;
    }

    public final XAttribute getNextAttribute() {
        return NextAttribute;
    }

    public final XAttribute getPreviousAttribute() {
        return PreviousAttribute;
    }

    public final String getValue() {
        return Value;
    }

    public final void setValue(String value) {
        Value = value;
    }

    /**
     * Summary:
     * Removes this attribute from its parent element.
     * <p>
     * Exceptions:
     * T:System.InvalidOperationException:
     * The parent element is null.
     */
    public void Remove() {
    }

    /**
     * Summary:
     * Sets the value of this attribute.
     * <p>
     * Parameters:
     * value:
     * The value to assign to this attribute.
     * <p>
     * Exceptions:
     * T:System.ArgumentNullException:
     * The value parameter is null.
     * <p>
     * T:System.ArgumentException:
     * The value is an System.Xml.Linq.XObject.
     */
    public void SetValue(Object value) {
    }

    /**
     * Summary:
     * Converts the current System.Xml.Linq.XAttribute object to a string representation.
     * <p>
     * Returns:
     * A System.String containing the XML text representation of an attribute and its
     * value.
     */
    public String toString() {
        return "";
    }
}
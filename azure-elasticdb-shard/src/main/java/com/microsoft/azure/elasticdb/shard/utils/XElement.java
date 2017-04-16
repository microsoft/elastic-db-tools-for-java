package com.microsoft.azure.elasticdb.shard.utils;

// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

import java.io.InputStream;
import java.io.OutputStream;
import java.io.Reader;
import java.io.Writer;
import java.util.List;

//
// Summary:
//     Represents an XML element.
public class XElement {
    //extends XContainer implements IXmlSerializable {

    /**
     * Summary:
     * Gets an empty collection of elements.
     * <p>
     * Returns:
     * An System.Collections.Generic.IEnumerable`1 of XElement that
     * contains an empty collection.
     */
    private static List<XElement> EmptySequence;
    /**
     * Summary:
     * Gets the first attribute of this element.
     * <p>
     * Returns:
     * An XAttribute that contains the first attribute of this element.
     */
    private XAttribute FirstAttribute;
    /**
     * Summary:
     * Gets a value indicating whether this element as at least one attribute.
     * <p>
     * Returns:
     * true if this element has at least one attribute; otherwise false.
     */
    private boolean HasAttributes;
    /**
     * Summary:
     * Gets a value indicating whether this element has at least one child element.
     * <p>
     * Returns:
     * true if this element has at least one child element; otherwise false.
     */
    private boolean HasElements;
    /**
     * Summary:
     * Gets a value indicating whether this element contains no content.
     * <p>
     * Returns:
     * true if this element contains no content; otherwise false.
     */
    private boolean IsEmpty;
    /**
     * Summary:
     * Gets the last attribute of this element.
     * <p>
     * Returns:
     * An XAttribute that contains the last attribute of this element.
     */
    private XAttribute LastAttribute;
    /**
     * Summary:
     * Gets or sets the name of this element.
     * <p>
     * Returns:
     * An XName that contains the name of this element.
     */
    private XName Name;
    /**
     * Summary:
     * Gets or sets the concatenated text contents of this element.
     * <p>
     * Returns:
     * A System.String that contains all of the text content of this element. If there
     * are multiple text nodes, they will be concatenated.
     */
    private String Value;

    /**
     * Summary:
     * Initializes a new instance of the XElement class with the specified name.
     * <p>
     * Parameters:
     * name:
     * An XName that contains the name of the element.
     */
    public XElement(XName name) {
        this.Name = name;
    }

    /**
     * Summary:
     * Initializes a new instance of the XElement class with the specified name.
     * <p>
     * Parameters:
     * name:
     * A String that contains the name of the element.
     */
    public XElement(String name) {
        this.Name = new XName(name);
    }

    /**
     * Summary:
     * Initializes a new instance of the XElement class from another XElement object.
     * <p>
     * Parameters:
     * other:
     * An XElement object to copy from.
     */
    public XElement(XElement other) {

    }

    /**
     * Summary:
     * Initializes a new instance of the XElement class with the specified name and content.
     * <p>
     * Parameters:
     * name:
     * An XName that contains the element name.
     * <p>
     * content:
     * The contents of the element.
     */
    public XElement(XName name, Object content) {
    }

    /**
     * Summary:
     * Initializes a new instance of the XElement class with the specified name and content.
     * <p>
     * Parameters:
     * name:
     * An XName that contains the element name.
     * <p>
     * content:
     * The initial content of the element.
     */
    public XElement(XName name, Object[] content) {
    }

    /**
     * Summary:
     * Initializes a new instance of the XElement class with the specified name and content.
     * <p>
     * Parameters:
     * name:
     * A String that contains the element name.
     * <p>
     * content:
     * The initial content of the element.
     */
    public XElement(String name, int content) {
    }

    /**
     * Summary:
     * Initializes a new instance of the XElement class with the specified name and content.
     * <p>
     * Parameters:
     * name:
     * A String that contains the element name.
     * <p>
     * content:
     * The initial content of the element.
     */
    public XElement(String name, Object... content) {
    }

    public static List<XElement> getEmptySequence() {
        return EmptySequence;
    }

    /**
     * Summary:
     * Loads an XElement from a file.
     * <p>
     * Parameters:
     * uri:
     * A URI string referencing the file to load into a new XElement.
     * <p>
     * Returns:
     * An XElement that contains the contents of the specified file.
     */
    public static XElement Load(String uri) {
        return null;
    }

    /**
     * Summary:
     * Creates a new XElement instance by using the specified stream.
     * <p>
     * Parameters:
     * stream:
     * The stream that contains the XML data.
     * <p>
     * Returns:
     * An XElement object used to read the data that is contained in
     * the stream.
     */
    public static XElement Load(InputStream stream) {
        return null;
    }

    /**
     * Summary:
     * Loads an XElement from a System.IO.Reader.
     * <p>
     * Parameters:
     * Reader:
     * A System.IO.Reader that will be read for the XElement content.
     * <p>
     * Returns:
     * An XElement that contains the XML that was read from the specified
     * System.IO.Reader.
     */
    public static XElement Load(Reader Reader) {
        return null;
    }

    /**
     * Summary:
     * Loads an XElement from a file, optionally preserving white space,
     * setting the base URI, and retaining line information.
     * <p>
     * Parameters:
     * uri:
     * A URI string referencing the file to load into an XElement.
     * <p>
     * options:
     * A LoadOptions that specifies white space behavior, and whether
     * to load base URI and line information.
     * <p>
     * Returns:
     * An XElement that contains the contents of the specified file.
     */
    public static XElement Load(String uri, LoadOptions options) {
        return null;
    }

    /**
     * Summary:
     * Creates a new XElement instance by using the specified stream,
     * optionally preserving white space, setting the base URI, and retaining line information.
     * <p>
     * Parameters:
     * stream:
     * The stream containing the XML data.
     * <p>
     * options:
     * A LoadOptions object that specifies whether to load base URI
     * and line information.
     * <p>
     * Returns:
     * An XElement object used to read the data that the stream contains.
     */
    public static XElement Load(InputStream stream, LoadOptions options) {
        return null;
    }

    /**
     * Summary:
     * Loads an XElement from a System.IO.Reader, optionally preserving
     * white space and retaining line information.
     * <p>
     * Parameters:
     * Reader:
     * A System.IO.Reader that will be read for the XElement content.
     * <p>
     * options:
     * A LoadOptions that specifies white space behavior, and whether
     * to load base URI and line information.
     * <p>
     * Returns:
     * An XElement that contains the XML that was read from the specified
     * System.IO.Reader.
     */
    public static XElement Load(Reader Reader, LoadOptions options) {
        return null;
    }

    /**
     * Summary:
     * Load an XElement from a string that contains XML.
     * <p>
     * Parameters:
     * text:
     * A System.String that contains XML.
     * <p>
     * Returns:
     * An XElement populated from the string that contains XML.
     */
    public static XElement Parse(String text) {
        return null;
    }

    /**
     * Summary:
     * Load an XElement from a string that contains XML, optionally
     * preserving white space and retaining line information.
     * <p>
     * Parameters:
     * text:
     * A System.String that contains XML.
     * <p>
     * options:
     * A LoadOptions that specifies white space behavior, and whether
     * to load base URI and line information.
     * <p>
     * Returns:
     * An XElement populated from the string that contains XML.
     */
    public static XElement Parse(String text, LoadOptions options) {
        return null;
    }

    public final XAttribute getFirstAttribute() {
        return FirstAttribute;
    }

    public final boolean getHasAttributes() {
        return HasAttributes;
    }

    public final boolean getHasElements() {
        return HasElements;
    }

    public final boolean getIsEmpty() {
        return IsEmpty;
    }

    public final XAttribute getLastAttribute() {
        return LastAttribute;
    }

    public final XName getName() {
        return Name;
    }

    public final void setName(XName value) {
        Name = value;
    }

    public final String getValue() {
        return Value;
    }

    public final void setValue(String value) {
        Value = value;
    }

    /**
     * Summary:
     * Returns a collection of elements that contain this element, and the ancestors
     * of this element.
     * <p>
     * Returns:
     * An System.Collections.Generic.IEnumerable`1 of XElement of elements
     * that contain this element, and the ancestors of this element.
     */
    public List<XElement> AncestorsAndSelf() {
        return null;
    }

    /**
     * Summary:
     * Returns a filtered collection of elements that contain this element, and the
     * ancestors of this element. Only elements that have a matching XName
     * are included in the collection.
     * <p>
     * Parameters:
     * name:
     * The XName to match.
     * <p>
     * Returns:
     * An System.Collections.Generic.IEnumerable`1 of XElement that
     * contain this element, and the ancestors of this element. Only elements that have
     * a matching XName are included in the collection.
     */
    public List<XElement> AncestorsAndSelf(XName name) {
        return null;
    }

    /**
     * Summary:
     * Returns the XAttribute of this XElement that
     * has the specified XName.
     * <p>
     * Parameters:
     * name:
     * The XName of the XAttribute to get.
     * <p>
     * Returns:
     * An XAttribute that has the specified XName; null
     * if there is no attribute with the specified name.
     */
    public XAttribute Attribute(XName name) {
        return null;
    }

    /**
     * Summary:
     * Returns a collection of attributes of this element.
     * <p>
     * Returns:
     * An System.Collections.Generic.IEnumerable`1 of XAttribute of
     * attributes of this element.
     */
    public List<XAttribute> Attributes() {
        return null;
    }

    /**
     * Summary:
     * Returns a filtered collection of attributes of this element. Only elements that
     * have a matching XName are included in the collection.
     * <p>
     * Parameters:
     * name:
     * The XName to match.
     * <p>
     * Returns:
     * An System.Collections.Generic.IEnumerable`1 of XAttribute that
     * contains the attributes of this element. Only elements that have a matching XName
     * are included in the collection.
     */
    public List<XAttribute> Attributes(XName name) {
        return null;
    }

    /**
     * Summary:
     * Returns a collection of elements that contain this element, and all descendant
     * elements of this element, in document order.
     * <p>
     * Returns:
     * An System.Collections.Generic.IEnumerable`1 of XElement of elements
     * that contain this element, and all descendant elements of this element, in document
     * order.
     */
    public List<XElement> DescendantsAndSelf() {
        return null;
    }

    /**
     * Summary:
     * Returns a filtered collection of elements that contain this element, and all
     * descendant elements of this element, in document order. Only elements that have
     * a matching XName are included in the collection.
     * <p>
     * Parameters:
     * name:
     * The XName to match.
     * <p>
     * Returns:
     * An System.Collections.Generic.IEnumerable`1 of XElement that
     * contain this element, and all descendant elements of this element, in document
     * order. Only elements that have a matching XName are included
     * in the collection.
     */
    public List<XElement> DescendantsAndSelf(XName name) {
        return null;
    }

    /**
     * Summary:
     * Removes nodes and attributes from this XElement.
     */
    public void RemoveAll() {
    }

    /**
     * Summary:
     * Removes the attributes of this XElement.
     */
    public void RemoveAttributes() {
    }

    /**
     * Summary:
     * Replaces the child nodes and the attributes of this element with the specified
     * content.
     * <p>
     * Parameters:
     * content:
     * A parameter list of content objects.
     */
    public void ReplaceAll(Object[] content) {
    }

    /**
     * Summary:
     * Replaces the child nodes and the attributes of this element with the specified
     * content.
     * <p>
     * Parameters:
     * content:
     * The content that will replace the child nodes and attributes of this element.
     */
    public void ReplaceAll(Object content) {
    }

    /**
     * Summary:
     * Replaces the attributes of this element with the specified content.
     * <p>
     * Parameters:
     * content:
     * The content that will replace the attributes of this element.
     */
    public void ReplaceAttributes(Object content) {
    }

    /**
     * Summary:
     * Replaces the attributes of this element with the specified content.
     * <p>
     * Parameters:
     * content:
     * A parameter list of content objects.
     */
    public void ReplaceAttributes(Object[] content) {
    }

    /**
     * Summary:
     * Serialize this element to a file.
     * <p>
     * Parameters:
     * fileName:
     * A System.String that contains the name of the file.
     */
    public void Save(String fileName) {
    }

    /**
     * Summary:
     * Outputs this XElement to the specified System.IO.Stream.
     * <p>
     * Parameters:
     * stream:
     * The stream to output this XElement to.
     */
    public void Save(OutputStream stream) {
    }

    /**
     * Summary:
     * Serialize this element to a System.IO.Writer.
     * <p>
     * Parameters:
     * Writer:
     * A System.IO.Writer that the XElement will be written to.
     */
    public void Save(Writer Writer) {
    }

    /**
     * Summary:
     * Serialize this element to a file, optionally disabling formatting.
     * <p>
     * Parameters:
     * fileName:
     * A System.String that contains the name of the file.
     * <p>
     * options:
     * A SaveOptions that specifies formatting behavior.
     */
    public void Save(String fileName, SaveOptions options) {
    }

    /**
     * Summary:
     * Outputs this XElement to the specified System.IO.Stream, optionally
     * specifying formatting behavior.
     * <p>
     * Parameters:
     * stream:
     * The stream to output this XElement to.
     * <p>
     * options:
     * A SaveOptions object that specifies formatting behavior.
     */
    public void Save(OutputStream stream, SaveOptions options) {
    }

    /**
     * Summary:
     * Serialize this element to a System.IO.Writer, optionally disabling formatting.
     * <p>
     * Parameters:
     * Writer:
     * The System.IO.Writer to output the XML to.
     * <p>
     * options:
     * A SaveOptions that specifies formatting behavior.
     */
    public void Save(Writer Writer, SaveOptions options) {
    }

    /**
     * Summary:
     * Sets the value of an attribute, adds an attribute, or removes an attribute.
     * <p>
     * Parameters:
     * name:
     * An XName that contains the name of the attribute to change.
     * <p>
     * value:
     * The value to assign to the attribute. The attribute is removed if the value is
     * null. Otherwise, the value is converted to its string representation and assigned
     * to the XAttribute.Value property of the attribute.
     * <p>
     * Exceptions:
     * T:System.ArgumentException:
     * The value is an instance of XObject.
     */
    public void SetAttributeValue(XName name, Object value) {
    }

    /**
     * Summary:
     * Sets the value of a child element, adds a child element, or removes a child element.
     * <p>
     * Parameters:
     * name:
     * An XName that contains the name of the child element to change.
     * <p>
     * value:
     * The value to assign to the child element. The child element is removed if the
     * value is null. Otherwise, the value is converted to its string representation
     * and assigned to the XElement.Value property of the child element.
     * <p>
     * Exceptions:
     * T:System.ArgumentException:
     * The value is an instance of XObject.
     */
    public void SetElementValue(XName name, Object value) {
    }

    /**
     * Summary:
     * Sets the value of this element.
     * <p>
     * Parameters:
     * value:
     * The value to assign to this element. The value is converted to its string representation
     * and assigned to the XElement.Value property.
     * <p>
     * Exceptions:
     * T:System.ArgumentNullException:
     * The value is null.
     * <p>
     * T:System.ArgumentException:
     * The value is an XObject.
     */
    public void SetValue(Object value) {
    }
}
package com.microsoft.azure.elasticdb.shard.utils;

// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

//
// Summary:
//     Specifies load options when parsing XML.
public enum LoadOptions {
  //
  // Summary:
  //     Does not preserve insignificant white space or load base URI and line information.
  None(0),
  //
  // Summary:
  //     Preserves insignificant white space while parsing.
  PreserveWhitespace(1),
  //
  // Summary:
  //     Requests the base URI information from the System.Xml.XmlReader, and makes it
  //     available via the System.Xml.Linq.XObject.BaseUri property.
  SetBaseUri(2),
  //
  // Summary:
  //     Requests the line information from the System.Xml.XmlReader and makes it available
  //     via properties on System.Xml.Linq.XObject.
  SetLineInfo(4);

  public static final int SIZE = Integer.SIZE;
  private static java.util.HashMap<Integer, LoadOptions> mappings;
  private int intValue;

  private LoadOptions(int value) {
    intValue = value;
    getMappings().put(value, this);
  }

  private static java.util.HashMap<Integer, LoadOptions> getMappings() {
    if (mappings == null) {
      synchronized (LoadOptions.class) {
        if (mappings == null) {
          mappings = new java.util.HashMap<Integer, LoadOptions>();
        }
      }
    }
    return mappings;
  }

  public static LoadOptions forValue(int value) {
    return getMappings().get(value);
  }

  public int getValue() {
    return intValue;
  }
}

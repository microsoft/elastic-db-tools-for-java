package com.microsoft.azure.elasticdb.core.commons.helpers;

/* Copyright (c) Microsoft. All rights reserved.
Licensed under the MIT license. See LICENSE file in the project root for full license information.*/

public class ReferenceObjectHelper<ValueT> {

  public ValueT outValue;
  public ValueT argValue;

  public ReferenceObjectHelper(ValueT referenceValue) {
    outValue = referenceValue;
  }
}
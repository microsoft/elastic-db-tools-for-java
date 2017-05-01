package com.microsoft.azure.elasticdb.core.commons.helpers;

/* Copyright (c) Microsoft. All rights reserved.
Licensed under the MIT license. See LICENSE file in the project root for full license information.*/

public class ReferenceObjectHelper<TValue> {

  public TValue outValue;
  public TValue argValue;

  public ReferenceObjectHelper(TValue referenceValue) {
    outValue = referenceValue;
  }
}

package com.microsoft.azure.elasticdb.shard.mapmanager.stubs;

/*Copyright (c) Microsoft. All rights reserved.
Licensed under the MIT license. See LICENSE file in the project root for full license information.*/

public class IStubBehavior {

  public final <T, TReturn> TReturn Result(T obj, String name) {
    throw new UnsupportedOperationException();
  }

  public final <T> void VoidResult(T obj, String name) {
    throw new UnsupportedOperationException();
  }
}
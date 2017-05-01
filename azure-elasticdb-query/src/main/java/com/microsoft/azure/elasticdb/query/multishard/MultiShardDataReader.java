package com.microsoft.azure.elasticdb.query.multishard;

/* Copyright (c) Microsoft. All rights reserved.
Licensed under the MIT license. See LICENSE file in the project root for full license information.*/

import java.sql.ResultSetMetaData;

public class MultiShardDataReader implements AutoCloseable {

  public int fieldCount;

  @Override
  public void close() throws Exception {

  }

  public ResultSetMetaData getMetaData() {
    ResultSetMetaData metaData = null;
    return metaData;
  }

  public boolean read() {
    return false;
  }

  public void getValues(Object[] values) {
  }
}

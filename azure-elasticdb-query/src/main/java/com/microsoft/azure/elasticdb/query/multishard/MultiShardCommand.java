package com.microsoft.azure.elasticdb.query.multishard;

/* Copyright (c) Microsoft. All rights reserved.
Licensed under the MIT license. See LICENSE file in the project root for full license information.*/

import com.microsoft.azure.elasticdb.query.logging.MultiShardExecutionOptions;
import com.microsoft.azure.elasticdb.query.logging.MultiShardExecutionPolicy;

public class MultiShardCommand implements AutoCloseable {

  public String commandText;
  public MultiShardExecutionOptions executionOptions;
  public MultiShardExecutionPolicy executionPolicy;
  public int commandTimeout;

  @Override
  public void close() throws Exception {

  }

  public MultiShardDataReader executeReader() {
    return new MultiShardDataReader();
  }
}

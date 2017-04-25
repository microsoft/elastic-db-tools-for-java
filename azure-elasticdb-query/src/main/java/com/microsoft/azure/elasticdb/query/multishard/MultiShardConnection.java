package com.microsoft.azure.elasticdb.query.multishard;

/* Copyright (c) Microsoft. All rights reserved.
Licensed under the MIT license. See LICENSE file in the project root for full license information.*/

import com.microsoft.azure.elasticdb.shard.base.Shard;
import java.util.List;

public class MultiShardConnection implements AutoCloseable {

  public MultiShardConnection(List<Shard> shards, String credConnString) {

  }

  @Override
  public void close() throws Exception {

  }

  public MultiShardCommand CreateCommand() {
    return new MultiShardCommand();
  }
}

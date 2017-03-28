package com.microsoft.azure.elasticdb.shard.mapmanager;

// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

import com.microsoft.azure.elasticdb.core.commons.transientfaulthandling.RetryPolicy;
import com.microsoft.azure.elasticdb.shard.storeops.base.IStoreOperationFactory;

/**
 * Serves as the entry point for creation, management and lookup operations over shard maps.
 */
public final class ShardMapManager {
    /**
     * Factory for store operations.
     */
    private IStoreOperationFactory StoreOperationFactory;
    /**
     * Policy for performing retries on connections to shard map manager database.
     */
    private RetryPolicy RetryPolicy;

    public final IStoreOperationFactory getStoreOperationFactory() {
        return StoreOperationFactory;
    }

    public final void setStoreOperationFactory(IStoreOperationFactory value) {
        StoreOperationFactory = value;
    }

    public final RetryPolicy getRetryPolicy() {
        return RetryPolicy;
    }

    public final void setRetryPolicy(RetryPolicy value) {
        RetryPolicy = value;
    }

}

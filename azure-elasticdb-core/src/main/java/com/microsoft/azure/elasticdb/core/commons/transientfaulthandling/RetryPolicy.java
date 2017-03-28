package com.microsoft.azure.elasticdb.core.commons.transientfaulthandling;

// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

import com.microsoft.azure.elasticdb.core.commons.helpers.Action;
import com.microsoft.azure.elasticdb.core.commons.helpers.ActionGeneric;

/**
 * Provides the base implementation of the retry mechanism for unreliable actions and transient conditions.
 */
public class RetryPolicy {
    /**
     * Repetitively executes the specified action while it satisfies the current retry policy.
     *
     * @param action A delegate that represents the executable action that doesn't return any results.
     */
    public void ExecuteAction(Action action) {
        //Guard.ArgumentNotNull(action, "action");

        this.ExecuteAction(() -> {
            action.invoke();
        });
    }

    public <TResult> TResult ExecuteAction(ActionGeneric<TResult> action) {
        //Guard.ArgumentNotNull(action, "action");

        this.ExecuteAction(() -> {
            action.invoke();
            return null;
        });
        return null;
    }
}

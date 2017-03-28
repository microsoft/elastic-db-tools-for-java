package com.microsoft.azure.elasticdb.core.commons.helpers;

@FunctionalInterface
public interface ActionGeneric<TResult> {
    TResult invoke();
}
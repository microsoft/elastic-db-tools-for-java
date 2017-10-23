package com.microsoft.azure.elasticdb.core.commons.helpers;

/*
 * Elastic database tools for Azure SQL Database.
 * 
 * Copyright(c) Microsoft Corporation All rights reserved.
 * 
 * This program is made available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public final class Event<T> {

    private Map<String, T> namedListeners = new HashMap<>();

    private List<T> anonymousListeners = new ArrayList<>();

    /**
     * Add an Event Listener.
     *
     * @param methodName
     *            Name of the Method
     * @param namedEventHandlerMethod
     *            Named handler method
     */
    public void addListener(String methodName,
            T namedEventHandlerMethod) {
        if (!namedListeners.containsKey(methodName)) {
            namedListeners.put(methodName, namedEventHandlerMethod);
        }
    }

    /**
     * Add an Event Listener.
     *
     * @param unnamedEventHandlerMethod
     *            Unnamed handler method
     */
    public void addListener(T unnamedEventHandlerMethod) {
        anonymousListeners.add(unnamedEventHandlerMethod);
    }

    /**
     * Remove the Event Listener.
     *
     * @param methodName
     *            Name of the method
     */
    public void removeListener(String methodName) {
        if (namedListeners.containsKey(methodName)) {
            namedListeners.remove(methodName);
        }
    }

    /**
     * Remove the Event Listener.
     *
     * @param unnamedEventHandlerMethod
     *            Unnamed handler method
     */
    public void removeListener(T unnamedEventHandlerMethod) {
        anonymousListeners.remove(unnamedEventHandlerMethod);
    }

    /**
     * List of named and unnamed Listeners.
     *
     * @return ArrayList of named and unnamed Listeners
     */
    public List<T> listeners() {
        List<T> allListeners = new ArrayList<>();
        allListeners.addAll(namedListeners.values());
        allListeners.addAll(anonymousListeners);
        return allListeners;
    }
}
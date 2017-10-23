package com.microsoft.azure.elasticdb.core.commons.helpers;

/*
 * Elastic database tools for Azure SQL Database.
 * 
 * Copyright(c) Microsoft Corporation All rights reserved.
 * 
 * This program is made available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

import org.apache.commons.lang3.StringUtils;

public final class ApplicationNameHelper {

    private ApplicationNameHelper() {
    }

    public static final int MAX_APPLICATION_NAME_LENGTH = 128;

    /**
     * Adds suffix to the application name, but not exceeding certain length().
     *
     * @param originalApplicationName
     *            Application provided application name.
     * @param suffixToAppend
     *            Suffix to append to the application name.
     * @return Application name with suffix appended.
     */
    public static String addApplicationNameSuffix(String originalApplicationName,
            String suffixToAppend) {
        if (originalApplicationName == null || StringUtils.isEmpty(originalApplicationName)) {
            return suffixToAppend;
        }

        if (suffixToAppend == null || StringUtils.isEmpty(suffixToAppend)) {
            return originalApplicationName;
        }

        int maxAppNameSubStringAllowed = MAX_APPLICATION_NAME_LENGTH - suffixToAppend.length();

        if (originalApplicationName.length() <= maxAppNameSubStringAllowed) {
            return originalApplicationName + suffixToAppend;
        }
        else {
            // Take the substring of application name that will be fit within the 'program_name' column in dm_exec_sessions.
            return originalApplicationName.substring(0, maxAppNameSubStringAllowed) + suffixToAppend;
        }
    }
}

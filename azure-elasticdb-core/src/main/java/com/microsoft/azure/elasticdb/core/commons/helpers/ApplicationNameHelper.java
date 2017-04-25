package com.microsoft.azure.elasticdb.core.commons.helpers;

/* Copyright (c) Microsoft. All rights reserved.
Licensed under the MIT license. See LICENSE file in the project root for full license information.*/

import org.apache.commons.lang3.StringUtils;

public final class ApplicationNameHelper {

  public static final int MaxApplicationNameLength = 128;

  /// <summary>
  /// Adds suffix to the application name, but not exceeding certain length().
  /// </summary>
  /// <param name="originalApplicationName">Application provided application name.</param>
  /// <param name="suffixToAppend">Suffix to append to the application name.</param>
  /// <returns>Application name with suffix appended.</returns>
  public static String AddApplicationNameSuffix(String originalApplicationName,
      String suffixToAppend) {
    if (StringUtils.isEmpty(originalApplicationName)) {
      return suffixToAppend;
    }

    if (StringUtils.isEmpty(suffixToAppend)) {
      return originalApplicationName;
    }

    int maxAppNamesubstringAllowed = MaxApplicationNameLength - suffixToAppend.length();

    if (originalApplicationName.length() <= maxAppNamesubstringAllowed) {
      return originalApplicationName + suffixToAppend;
    } else {
      // Take the substring of application name that will be fit within the 'program_name' column in dm_exec_sessions.
      return originalApplicationName.substring(0, maxAppNamesubstringAllowed) + suffixToAppend;
    }
  }
}

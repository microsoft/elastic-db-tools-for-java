package com.microsoft.azure.elasticdb.shard.utils;

/* Copyright (c) Microsoft. All rights reserved.
Licensed under the MIT license. See LICENSE file in the project root for full license information.*/

import java.util.Locale;

/**
 * Utility methods for string manipulation.
 */
public final class StringUtilsLocal {

  /**
   * Lookup array for converting byte[] to string.
   */
  private static char[] byteToCharLookup = {'0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a',
      'b', 'c', 'd', 'e', 'f'};

  /**
   * Creates a formatted String which is culture invariant with given arguments.
   *
   * @param input Input String.
   * @param args Collection of formatting arguments.
   * @return Formatted String.
   */
  public static String formatInvariant(String input, Object... args) {
    return String.format(Locale.getDefault(), input, args);
  }

  /**
   * Converts the given byte array to its string representation.
   *
   * @param input Input byte array.
   * @return String representation of the byte array.
   */
  public static String byteArrayToString(byte[] input) {
    assert input != null;

    StringBuilder result = new StringBuilder((input.length + 1) * 2).append("0x");

    for (byte b : input) {
      result.append(byteToCharLookup[b >> 4]).append(byteToCharLookup[b & 0x0f]);
    }

    return result.toString();
  }

  /**
   * This method replaces the .NET static string method 'IsNullOrEmpty'.
   */
  public static boolean isNullOrEmpty(String string) {
    return string == null || string.length() == 0;
  }

  /**
   * This method replaces the .NET string method 'Remove' (1 parameter version).
   */
  public static String remove(String string, int start) {
    return string.substring(0, start);
  }

  /**
   * This method replaces the .NET string method 'Remove' (2 parameter version).
   */
  public static String remove(String string, int start, int count) {
    return string.substring(0, start) + string.substring(start + count);
  }

}

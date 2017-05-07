package com.microsoft.azure.elasticdb.core.commons.exceptionhandling;

/*Copyright (c) Microsoft. All rights reserved.
Licensed under the MIT license. See LICENSE file in the project root for full license information.*/

import java.util.List;

/**
 * Extension methods for System.Exception.
 */
public final class ExceptionExtensionMethods {

  /**
   * Checks if this exception's type is the same, or a sub-class, of any of the specified types.
   *
   * @param ex This instance.
   * @param types Types to be matched against.
   * @return Whether or not this exception matched any of the specified types.
   */
  public static boolean isAnyOf(RuntimeException ex, List<Class> types) {
    if (ex == null) {
      throw new IllegalArgumentException("ex");
    }
    if (types == null) {
      throw new IllegalArgumentException("types");
    }

    Class exceptionType = ex.getClass();
    return types.stream().anyMatch(type -> type.isAssignableFrom(exceptionType));
  }
}
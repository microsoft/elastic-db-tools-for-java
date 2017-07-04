package com.microsoft.azure.elasticdb.query.unittests;

import static org.junit.Assert.fail;

import com.microsoft.azure.elasticdb.query.helpers.Action0Param;
import java.lang.invoke.MethodHandles;
import java.util.Arrays;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class AssertExtensions {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  static void assertSequenceEqual(byte[] expectedArray, byte[] actualArray) {
    if (Arrays.equals(expectedArray, actualArray)) {
      return;
    }

    log.info("Expected:[{}]", toCommaSeparatedString(expectedArray));
    log.info("Actual: [{}]", toCommaSeparatedString(actualArray));
    fail("Sequences were not equal");
  }

  private static String toCommaSeparatedString(byte[] collections) {
    StringBuilder result = new StringBuilder();
    for (byte element : collections) {
      result.append(element).append(",");
    }
    return result.toString();
  }

  public static <ExceptionT extends Exception> ExceptionT assertThrows(Action0Param action) {
    if (action == null) {
      throw new IllegalArgumentException("action");
    }

    try {
      action.invoke();

      // Exception not thrown
      fail("Exception was expected, but no exception was thrown");

      // Next line will never execute, it is required by the compiler
      return null;
    } catch (Exception e) {
      return (ExceptionT) e;
    }
  }
}
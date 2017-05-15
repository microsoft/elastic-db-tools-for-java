package com.microsoft.azure.elasticdb.shard.unittests;

import static org.junit.Assert.fail;

import com.microsoft.azure.elasticdb.shard.stubhelper.Action0Param;
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
    String result = "";
    for (byte element : collections) {
      result += element + ",";
    }
    return result;
  }

  public static <TException extends Exception> TException AssertThrows(Action0Param action) {
    if (action == null) {
      throw new IllegalArgumentException("action");
    }

    try {
      action.invoke();

      // Exception not thrown
      //TODO : TException.class
      fail("Exception of type {0} was expected, but no exception was thrown");

      // Next line will never execute, it is required by the compiler
      return null;
    }
    //TODO
//              catch (TException e)
//              {
//                  // Success
//                  return e;
//              }
    catch (Exception e) {
      // Wrong exception thrown
      //TODO:fail("Exception of type {0} was expected, exception of type {1} was thrown: {2}", e.getClass(), e.toString());
      //fail("Exception of type {0} was expected, exception of type {1} was thrown: {2}");
      // Next line will never execute, it is required by the compiler
      return (TException) e;
    }
  }
}
package com.microsoft.azure.elasticdb.shard.mapmanager.unittests;

import static org.junit.Assert.fail;

import java.lang.invoke.MethodHandles;
import java.util.Arrays;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AssertExtensions {

  private final static Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());


  public static void assertSequenceEqual(byte[] expected, byte[] actual) {
    byte[] expectedArray = expected;
    byte[] actualArray = actual;

    if (Arrays.equals(expectedArray, actualArray)) {
      return;
    }

    log.info("Expected:[{}]", ToCommaSeparatedString(expectedArray));
    log.info("Actual: [{}]", ToCommaSeparatedString(actualArray));
    fail("Sequences were not equal");
  }

  public static <T> String ToCommaSeparatedString(byte[] collections) {
    String result = "";
    for (byte element : collections) {
      result += element + ",";
    }
    return result;

  }

}

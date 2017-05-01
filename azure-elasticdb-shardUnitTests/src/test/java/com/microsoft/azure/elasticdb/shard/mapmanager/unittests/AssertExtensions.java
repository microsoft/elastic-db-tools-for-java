package com.microsoft.azure.elasticdb.shard.mapmanager.unittests;

import static org.junit.Assert.fail;

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

}

package com.microsoft.azure.elasticdb.shard.utils;

import java.io.IOException;
import java.io.Writer;
import java.util.List;

public class CsvUtils {

  private static final char DEFAULT_SEPARATOR = ',';

  /**
   * Write a single line to CSV File.
   *
   * @param w Writer to write to
   * @param values List of values to write
   * @throws IOException Exception thrown by writer
   */
  public static void writeLine(Writer w, List<String> values) throws IOException {
    writeLine(w, values, DEFAULT_SEPARATOR, ' ');
  }

  /**
   * Write a single line to CSV File.
   *
   * @param w Writer to write to
   * @param values List of values to write
   * @param separators List of separators
   * @throws IOException Exception thrown by writer
   */
  public static void writeLine(Writer w, List<String> values, char separators) throws IOException {
    writeLine(w, values, separators, ' ');
  }

  /**
   * Write a single line to CSV File.
   *
   * @param w Writer to write to
   * @param values List of values to write
   * @param separators List of separators
   * @param customQuote Custom quotation symbol
   * @throws IOException Exception thrown by writer
   */
  public static void writeLine(Writer w, List<String> values, char separators, char customQuote)
      throws IOException {
    boolean first = true;

    //default customQuote is empty
    if (separators == ' ') {
      separators = DEFAULT_SEPARATOR;
    }

    StringBuilder sb = new StringBuilder();
    for (String value : values) {
      if (!first) {
        sb.append(separators);
      }
      if (customQuote == ' ') {
        sb.append(followCsvFormat(value));
      } else {
        sb.append(customQuote).append(followCsvFormat(value)).append(customQuote);
      }

      first = false;
    }
    sb.append("\n");
    w.append(sb.toString());
  }

  private static String followCsvFormat(String value) {
    String result = value;
    if (result.contains("\"")) {
      result = result.replace("\"", "\"\"");
    }
    return result;
  }
}
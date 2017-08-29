package com.microsoft.azure.elasticdb.samples.elasticscalestarterkit;

/* Copyright (c) Microsoft. All rights reserved.
Licensed under the MIT license. See LICENSE file in the project root for full license information.*/

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;

/**
 * Stores tabular data and formats it for writing to output.
 */
public class TableFormatter {

  /**
   * Table column names.
   */
  private String[] columnNames;

  /**
   * Table rows.
   */
  private ArrayList<String[]> rows;

  public TableFormatter(String[] columnNames) {
    this.columnNames = columnNames;
    rows = new ArrayList<>();
  }

  /**
   * Add a new row.
   *
   * @param values Array of objects to add
   */
  public final void addRow(Object[] values) {
    if (values.length != columnNames.length) {
      throw new IllegalArgumentException(String
          .format("Incorrect number of fields. Expected %1$s, actual %2$s", columnNames.length,
              values.length));
    }

    String[] valueStrings = Arrays.copyOf(values, values.length, String[].class);
    rows.add(valueStrings);
  }

  @Override
  public String toString() {
    StringBuilder output = new StringBuilder();

    // Determine column widths
    int[] columnWidths = new int[columnNames.length];
    for (int c = 0; c < columnNames.length; c++) {
      int maxValueLength = 0;

      if (!rows.isEmpty()) {
        int i = c;
        maxValueLength = rows.stream().map(r -> r[i].length()).max(Comparator.naturalOrder()).get();
      }

      columnWidths[c] = Math.max(maxValueLength, columnNames[c].length()) + 2;
    }

    // Build format strings that are used to format the column names and fields
    String[] formatStrings = new String[columnNames.length];
    for (int c = 0; c < columnNames.length; c++) {
      formatStrings[c] = " %1$" + columnWidths[c] + "s";
    }

    // Write header
    for (int c = 0; c < columnNames.length; c++) {
      output.append(String.format(formatStrings[c], columnNames[c]));
    }

    output.append("\r\n");

    // Write separator
    for (int c = 0; c < columnNames.length; c++) {
      StringBuilder innerBuilder = new StringBuilder(columnNames[c].length());
      for (int i = 0; i < columnNames[c].length(); i++) {
        innerBuilder.append("-");
      }
      output.append(String.format(formatStrings[c], innerBuilder.toString()));
    }

    output.append("\r\n");

    // Write rows
    for (String[] row : rows) {
      for (int c = 0; c < columnNames.length; c++) {
        output.append(String.format(formatStrings[c], row[c]));
      }

      output.append("\r\n");
    }

    return output.toString();
  }
}
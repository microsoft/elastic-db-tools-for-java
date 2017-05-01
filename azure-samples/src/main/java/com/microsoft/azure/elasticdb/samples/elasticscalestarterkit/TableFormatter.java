package com.microsoft.azure.elasticdb.samples.elasticscalestarterkit;

/* Copyright (c) Microsoft. All rights reserved.
Licensed under the MIT license. See LICENSE file in the project root for full license information.*/

import java.util.ArrayList;
import java.util.Arrays;

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
    rows = new ArrayList<String[]>();
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
    /*int[] columnWidths = new int[columnNames.length];
    for (int c = 0; c < columnNames.length; c++) {
      int maxValueLength = 0;

      if (rows.Any()) {
        maxValueLength = rows.Select(r -> r[c].getLength()).max();
      }

      columnWidths[c] = Math.max(maxValueLength, columnNames[c].length());
    }

    // Build format strings that are used to format the column names and fields
    String[] formatStrings = new String[columnNames.length];
    for (int c = 0; c < columnNames.length; c++) {
      formatStrings[c] = String.format(" {0,-%1$s} ", columnWidths[c]);
    }

    // Write header
    for (int c = 0; c < columnNames.length; c++) {
      output.AppendFormat(formatStrings[c], columnNames[c]);
    }

    output.append("\r\n");

    // Write separator
    for (int c = 0; c < columnNames.length; c++) {
      output.AppendFormat(formatStrings[c], new String('-', columnNames[c].length()));
    }

    output.append("\r\n");

    // Write rows
    for (String[] row : rows) {
      for (int c = 0; c < columnNames.length; c++) {
        output.AppendFormat(formatStrings[c], row[c]);
      }

      output.append("\r\n");
    }*/

    return output.toString();
  }
}
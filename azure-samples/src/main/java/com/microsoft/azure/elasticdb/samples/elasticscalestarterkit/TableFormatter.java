package com.microsoft.azure.elasticdb.samples.elasticscalestarterkit;

// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

import java.util.ArrayList;

/**
 * Stores tabular data and formats it for writing to output.
 */
public class TableFormatter {

  /**
   * Table column names.
   */
  private String[] _columnNames;

  /**
   * Table rows.
   */
  private ArrayList<String[]> _rows;

  public TableFormatter(String[] columnNames) {
    _columnNames = columnNames;
    _rows = new ArrayList<String[]>();
  }

  public final void AddRow(Object[] values) {
    if (values.length != _columnNames.length) {
      throw new IllegalArgumentException(String
          .format("Incorrect number of fields. Expected %1$s, actual %2$s", _columnNames.length,
              values.length));
    }

        /*String[] valueStrings = values.Select(o -> o.toString()).ToArray();
        _rows.add(valueStrings);*/
    //TODO
  }

  @Override
  public String toString() {
    StringBuilder output = new StringBuilder();

    //TODO
        /*// Determine column widths
        int[] columnWidths = new int[_columnNames.length];
        for (int c = 0; c < _columnNames.length; c++) {
            int maxValueLength = 0;

            if (_rows.Any()) {
                maxValueLength = _rows.Select(r -> r[c].getLength()).max();
            }

            columnWidths[c] = Math.max(maxValueLength, _columnNames[c].length());
        }

        // Build format strings that are used to format the column names and fields
        String[] formatStrings = new String[_columnNames.length];
        for (int c = 0; c < _columnNames.length; c++) {
            formatStrings[c] = String.format(" {0,-%1$s} ", columnWidths[c]);
        }

        // Write header
        for (int c = 0; c < _columnNames.length; c++) {
            output.AppendFormat(formatStrings[c], _columnNames[c]);
        }

        output.append("\r\n");

        // Write separator
        for (int c = 0; c < _columnNames.length; c++) {
            output.AppendFormat(formatStrings[c], new String('-', _columnNames[c].length()));
        }

        output.append("\r\n");

        // Write rows
        for (String[] row : _rows) {
            for (int c = 0; c < _columnNames.length; c++) {
                output.AppendFormat(formatStrings[c], row[c]);
            }

            output.append("\r\n");
        }*/

    return output.toString();
  }
}
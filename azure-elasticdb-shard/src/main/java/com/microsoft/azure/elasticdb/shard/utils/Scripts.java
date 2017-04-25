package com.microsoft.azure.elasticdb.shard.utils;

// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

import static java.text.MessageFormat.format;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

public class Scripts {

  private static String scriptsSubfolder = "scripts";

  public static String getCheckShardMapManagerGlobal() {
    return buildResourcePath("CheckShardMapManagerGlobal.sql");
  }

  public static String getCreateShardMapManagerGlobal() {
    return buildResourcePath("CreateShardMapManagerGlobal.sql");
  }

  public static String getDropShardMapManagerGlobal() {
    return buildResourcePath("DropShardMapManagerGlobal.sql");
  }

  public static String getCheckShardMapManagerLocal() {
    return buildResourcePath("CheckShardMapManagerLocal.sql");
  }

  public static String getCreateShardMapManagerLocal() {
    return buildResourcePath("CreateShardMapManagerLocal.sql");
  }

  public static String getDropShardMapManagerLocal() {
    return buildResourcePath("DropShardMapManagerLocal.sql");
  }

  public static String buildResourcePath(String fileName) {
    return format("{0}/{1}", scriptsSubfolder, fileName);
  }

  public static String buildResourcePath() {
    return Scripts.class.getClassLoader().getResource(Scripts.buildResourcePath("")).getFile();
  }

  static List<StringBuilder> readScriptContent(String scriptPath) {
    ArrayList<StringBuilder> scriptLines = new ArrayList<>();
    try (BufferedReader tr = new BufferedReader(
        new InputStreamReader(
            Scripts.class.getClassLoader()
                .getResource(scriptPath).openStream(), "UTF-8"))) {
      StringBuilder sb = new StringBuilder();
      String currentLine;
      while ((currentLine = tr.readLine()) != null) {
        if (!currentLine.startsWith("--")) {
          if (currentLine.equalsIgnoreCase("GO")) {
            scriptLines.add(sb);
            sb = new StringBuilder();
          } else {
            sb.append(currentLine).append(System.lineSeparator());
          }
        }
      }
    } catch (NullPointerException | IOException e) {
      e.printStackTrace();
    }
    return scriptLines;
  }
}

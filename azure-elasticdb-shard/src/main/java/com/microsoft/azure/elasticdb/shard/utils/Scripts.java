package com.microsoft.azure.elasticdb.shard.utils;

// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static java.text.MessageFormat.format;

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
        BufferedReader br = null;
        FileReader fr = null;
        StringBuilder content = new StringBuilder();
        List<StringBuilder> fileContent = new ArrayList<>();

        try {
            fr = new FileReader(Scripts.class.getClassLoader().getResource(scriptPath).getFile());
            br = new BufferedReader(fr);
            String currentLine;
            while ((currentLine = br.readLine()) != null) {
                if (!currentLine.startsWith("--")) {
                    if (currentLine.equalsIgnoreCase("go")) {
                        fileContent.add(content);
                        content = new StringBuilder();
                    } else {
                        content.append(currentLine).append(System.getProperty("line.separator"));
                    }
                }
            }
        } catch (NullPointerException | IOException e) {
            e.printStackTrace();
            return null;
        } finally {
            try {
                if (br != null) {
                    br.close();
                }
                if (fr != null) {
                    fr.close();
                }
            } catch (IOException ex) {
                ex.printStackTrace();
                return null;
            }
        }
        return fileContent;
    }
}

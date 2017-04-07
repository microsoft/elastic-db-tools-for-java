package com.microsoft.azure.elasticdb.shard.utils;

// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class Scripts {

    private static String extension = ".sql";

    public static String getCheckShardMapManagerGlobal() {
        return "CheckShardMapManagerGlobal" + extension;
    }

    public static String getCreateShardMapManagerGlobal() {
        return "CreateShardMapManagerGlobal" + extension;
    }

    public static String getDropShardMapManagerGlobal() {
        return "DropShardMapManagerGlobal" + extension;
    }

    public static String getCheckShardMapManagerLocal() {
        return "CheckShardMapManagerLocal" + extension;
    }

    public static String getCreateShardMapManagerLocal() {
        return "CreateShardMapManagerLocal" + extension;
    }

    public static String getDropShardMapManagerLocal() {
        return "DropShardMapManagerLocal" + extension;
    }

    static List<StringBuilder> readFileContent(String fileName) {
        BufferedReader br = null;
        FileReader fr = null;
        StringBuilder content = new StringBuilder();
        List<StringBuilder> fileContent = new ArrayList<>();

        try {
            fr = new FileReader(Scripts.class.getClassLoader().getResource(fileName).getFile());
            br = new BufferedReader(fr);
            String currentLine;
            while ((currentLine = br.readLine()) != null) {
                if (!currentLine.startsWith("--")) {
                    content.append(currentLine).append(System.getProperty("line.separator"));
                    if (currentLine.equalsIgnoreCase("go")) {
                        fileContent.add(content);
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

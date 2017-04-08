package com.microsoft.azure.elasticdb.shard.utils;

// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;

import static java.text.MessageFormat.*;
import static java.lang.System.out;

public class Scripts {

    private static String scriptsSubfolder = "scripts";

    public static String getCheckShardMapManagerGlobal() {
        return buiildResourcePath("CheckShardMapManagerGlobal.sql");
    }

    public static String getCreateShardMapManagerGlobal() {
        return buiildResourcePath("CreateShardMapManagerGlobal.sql");
    }

    public static String getDropShardMapManagerGlobal() {
        return buiildResourcePath("DropShardMapManagerGlobal.sql");
    }

    public static String getCheckShardMapManagerLocal() {
        return buiildResourcePath("CheckShardMapManagerLocal.sql");
    }

    public static String getCreateShardMapManagerLocal() {
        return buiildResourcePath("CreateShardMapManagerLocal.sql");
    }

    public static String getDropShardMapManagerLocal() {
        return buiildResourcePath("DropShardMapManagerLocal.sql");
    }

    public static String buiildResourcePath(String fileName){
        return format("{0}/{1}", scriptsSubfolder, fileName);
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

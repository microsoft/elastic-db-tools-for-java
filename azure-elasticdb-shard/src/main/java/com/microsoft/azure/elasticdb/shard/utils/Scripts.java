package com.microsoft.azure.elasticdb.shard.utils;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

public class Scripts {

    private static String baseFolderPath = "../scripts/";

    public static String getCheckShardMapManagerGlobal() {
        return readFileContent(baseFolderPath + "CheckShardMapManagerGlobal.sql");
    }

    public static String getCreateShardMapManagerGlobal() {
        return readFileContent(baseFolderPath + "CreateShardMapManagerGlobal.sql");
    }

    public static String getDropShardMapManagerGlobal() {
        return readFileContent(baseFolderPath + "DropShardMapManagerGlobal.sql");
    }

    public static String getCheckShardMapManagerLocal() {
        return readFileContent(baseFolderPath + "CheckShardMapManagerLocal.sql");
    }

    public static String getCreateShardMapManagerLocal() {
        return readFileContent(baseFolderPath + "CreateShardMapManagerLocal.sql");
    }

    public static String getDropShardMapManagerLocal() {
        return readFileContent(baseFolderPath + "DropShardMapManagerLocal.sql");
    }

    private static String readFileContent(String fileFullPath) {
        BufferedReader br = null;
        FileReader fr = null;
        String sFileContent = "";

        try {
            fr = new FileReader(fileFullPath);
            br = new BufferedReader(fr);
            String sCurrentLine;
            while ((sCurrentLine = br.readLine()) != null) {
                sFileContent += sCurrentLine + System.getProperty("line.separator");
            }
        } catch (IOException e) {
            e.printStackTrace();
            return "";
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
                return "";
            }
        }
        return sFileContent;
    }
}

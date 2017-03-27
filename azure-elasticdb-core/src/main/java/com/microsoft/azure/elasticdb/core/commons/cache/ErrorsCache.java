package com.microsoft.azure.elasticdb.core.commons.cache;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import java.util.Set;

public class ErrorsCache
{
    private final Properties configProp = new Properties();

    private ErrorsCache()
    {
        InputStream in = this.getClass().getClassLoader().getResourceAsStream("errors.properties");
        try {
            configProp.load(in);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static class Singleton
    {
        private static final ErrorsCache INSTANCE = new ErrorsCache();
    }

    public static ErrorsCache getInstance()
    {
        return Singleton.INSTANCE;
    }

    public String getProperty(String key){
        return configProp.getProperty(key);
    }

    public Set<String> getAllPropertyNames(){
        return configProp.stringPropertyNames();
    }

    public boolean containsKey(String key){
        return configProp.containsKey(key);
    }
}

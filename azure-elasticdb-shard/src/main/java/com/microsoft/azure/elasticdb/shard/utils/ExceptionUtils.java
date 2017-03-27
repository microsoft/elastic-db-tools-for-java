package com.microsoft.azure.elasticdb.shard.utils;

import org.apache.commons.lang3.StringUtils;

public class ExceptionUtils {

    /// <summary>
    /// Checks if given string argument is null or empty and if it is, throws an <see cref="ArgumentException"/>.
    /// </summary>
    /// <param name="s">Input string.</param>
    /// <param name="argName">Name of argument whose value is provided in <paramref name="s"/>.</param>
    public static void DisallowNullOrEmptyStringArgument(String s, String argName)
    {
        if (StringUtils.isEmpty(s))
        {
            throw new IllegalArgumentException(argName);
        }
    }

    //TODO: Migrate other methods
}

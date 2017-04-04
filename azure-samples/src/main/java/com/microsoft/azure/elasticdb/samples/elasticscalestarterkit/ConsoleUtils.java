package com.microsoft.azure.elasticdb.samples.elasticscalestarterkit;

// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

import com.microsoft.azure.elasticdb.core.commons.helpers.ReferenceObjectHelper;
import com.microsoft.azure.elasticdb.shard.utils.StringUtilsLocal;

import java.util.Scanner;
import java.util.function.Function;

public final class ConsoleUtils {
    /**
     * Writes detailed information to the console.
     */
    public static void WriteInfo(String format, Object... args) {
        WriteColor(ConsoleColor.DarkGray, "\t" + format, args);
    }

    /**
     * Writes warning text to the console.
     */
    public static void WriteWarning(String format, Object... args) {
        WriteColor(ConsoleColor.Yellow, format, args);
    }

    /**
     * Writes colored text to the console.
     */
    public static void WriteColor(String color, String format, Object... args) {
        System.out.println(color + StringUtilsLocal.FormatInvariant(format, args) + ConsoleColor.Default);
    }

    /**
     * Reads an integer from the console.
     */
    public static int ReadIntegerInput(String prompt) {
        return ReadIntegerInput(prompt, false);
    }

    /**
     * Reads an integer from the console, or returns null if the user enters nothing and allowNull is true.
     */
    public static Integer ReadIntegerInput(String prompt, boolean allowNull) {
        while (true) {
            System.out.print(prompt);
            String line = new Scanner(System.in).nextLine();

            if (StringUtilsLocal.isNullOrWhiteSpace(line) && allowNull) {
                return null;
            }

            int inputValue = 0;
            ReferenceObjectHelper<Integer> tempRef_inputValue = new ReferenceObjectHelper<Integer>(inputValue);
            //TODO: if (TryParseHelper.tryParseInt(line, tempRef_inputValue)) {
            inputValue = tempRef_inputValue.argValue;
            return inputValue;
            /*} else {
                inputValue = tempRef_inputValue.argValue;
            }*/
        }
    }

    /**
     * Reads an integer from the console.
     */
    public static int ReadIntegerInput(String prompt, int defaultValue, Function<Integer, Boolean> validator) {
        while (true) {
            Integer input = ReadIntegerInput(prompt, true);

            if (input == null) {
                // No input, so return default
                return defaultValue;
            } else {
                // Input was provided, so validate it
                if (validator.apply(input)) {
                    // Validation passed, so return
                    return input;
                }
            }
        }
    }
}

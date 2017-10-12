package com.microsoft.azure.elasticdb.core.commons.transientfaulthandling;

import java.util.Locale;

/*
 * Elastic database tools for Azure SQL Database.
 * 
 * Copyright(c) Microsoft Corporation All rights reserved.
 * 
 * This program is made available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

import com.microsoft.azure.elasticdb.core.commons.helpers.Resources;

/**
 * Implements the common guard methods.
 */
public class Guard {

    /**
     * Checks a string argument to ensure that it isn't null or empty.
     *
     * @param argumentValue
     *            The argument value to check.
     * @param argumentName
     *            The name of the argument.
     * @return The return value should be ignored. It is intended to be used only when validating arguments during instance creation (for example,
     *         when calling the base constructor).
     */
    public static boolean argumentNotNullOrEmptyString(String argumentValue,
            String argumentName) {
        argumentNotNull(argumentValue, argumentName);

        if (argumentValue.length() == 0) {
            throw new IllegalArgumentException(String.format(Locale.getDefault(), Resources.StringCannotBeEmpty, argumentName));
        }

        return true;
    }

    /**
     * Checks an argument to ensure that it isn't null.
     *
     * @param argumentValue
     *            The argument value to check.
     * @param argumentName
     *            The name of the argument.
     * @return The return value should be ignored. It is intended to be used only when validating arguments during instance creation (for example,
     *         when calling the base constructor).
     */
    public static boolean argumentNotNull(Object argumentValue,
            String argumentName) {
        if (argumentValue == null) {
            throw new IllegalArgumentException(argumentName);
        }

        return true;
    }

    /**
     * Checks an argument to ensure that its 32-bit signed value isn't negative.
     *
     * @param argumentValue
     *            The Integer value of the argument.
     * @param argumentName
     *            The name of the argument for diagnostic purposes.
     */
    public static void argumentNotNegativeValue(int argumentValue,
            String argumentName) {
        if (argumentValue < 0) {
            throw new IllegalArgumentException(String.format(Locale.getDefault(), Resources.ArgumentCannotBeNegative, argumentName));
        }
    }

    /**
     * Checks an argument to ensure that its 64-bit signed value isn't negative.
     *
     * @param argumentValue
     *            The Long value of the argument.
     * @param argumentName
     *            The name of the argument for diagnostic purposes.
     */
    public static void argumentNotNegativeValue(long argumentValue,
            String argumentName) {
        if (argumentValue < 0) {
            throw new IllegalArgumentException(String.format(Locale.getDefault(), Resources.ArgumentCannotBeNegative, argumentName));
        }
    }

    /**
     * Checks an argument to ensure that its value doesn't exceed the specified ceiling baseline.
     *
     * @param argumentValue
     *            The Double value of the argument.
     * @param ceilingValue
     *            The Double ceiling value of the argument.
     * @param argumentName
     *            The name of the argument for diagnostic purposes.
     */
    public static void argumentNotGreaterThan(double argumentValue,
            double ceilingValue,
            String argumentName) {
        if (argumentValue > ceilingValue) {
            throw new IllegalArgumentException(
                    String.format(Locale.getDefault(), Resources.ArgumentCannotBeGreaterThanBaseline, argumentName, ceilingValue));
        }
    }
}

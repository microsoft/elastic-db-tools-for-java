package com.microsoft.azure.elasticdb.core.commons.transientfaulthandling;

// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

import com.microsoft.azure.elasticdb.core.commons.helpers.Resources;

import java.util.Locale;

/**
 * Implements the common guard methods.
 */
public class Guard {
    /**
     * Checks a string argument to ensure that it isn't null or empty.
     *
     * @param argumentValue The argument value to check.
     * @param argumentName  The name of the argument.
     * @return The return value should be ignored. It is intended to be used only when validating arguments during instance creation (for example, when calling the base constructor).
     */
    public static boolean ArgumentNotNullOrEmptyString(String argumentValue, String argumentName) {
        ArgumentNotNull(argumentValue, argumentName);

        if (argumentValue.length() == 0) {
            throw new IllegalArgumentException(String.format(Locale.getDefault(), Resources.StringCannotBeEmpty, argumentName));
        }

        return true;
    }

    /**
     * Checks an argument to ensure that it isn't null.
     *
     * @param argumentValue The argument value to check.
     * @param argumentName  The name of the argument.
     * @return The return value should be ignored. It is intended to be used only when validating arguments during instance creation (for example, when calling the base constructor).
     */
    public static boolean ArgumentNotNull(Object argumentValue, String argumentName) {
        if (argumentValue == null) {
            throw new IllegalArgumentException(argumentName);
        }

        return true;
    }

    /**
     * Checks an argument to ensure that its 32-bit signed value isn't negative.
     *
     * @param argumentValue The <see cref="System.Int32"/> value of the argument.
     * @param argumentName  The name of the argument for diagnostic purposes.
     */
    public static void ArgumentNotNegativeValue(int argumentValue, String argumentName) {
        if (argumentValue < 0) {
            throw new IllegalArgumentException(String.format(Locale.getDefault(), Resources.ArgumentCannotBeNegative, argumentName));
        }
    }

    /**
     * Checks an argument to ensure that its 64-bit signed value isn't negative.
     *
     * @param argumentValue The <see cref="System.Int64"/> value of the argument.
     * @param argumentName  The name of the argument for diagnostic purposes.
     */
    public static void ArgumentNotNegativeValue(long argumentValue, String argumentName) {
        if (argumentValue < 0) {
            throw new IllegalArgumentException(String.format(Locale.getDefault(), Resources.ArgumentCannotBeNegative, argumentName));
        }
    }

    /**
     * Checks an argument to ensure that its value doesn't exceed the specified ceiling baseline.
     *
     * @param argumentValue The <see cref="System.Double"/> value of the argument.
     * @param ceilingValue  The <see cref="System.Double"/> ceiling value of the argument.
     * @param argumentName  The name of the argument for diagnostic purposes.
     */
    public static void ArgumentNotGreaterThan(double argumentValue, double ceilingValue, String argumentName) {
        if (argumentValue > ceilingValue) {
            throw new IllegalArgumentException(String.format(Locale.getDefault(), Resources.ArgumentCannotBeGreaterThanBaseline, argumentName, ceilingValue));
        }
    }
}

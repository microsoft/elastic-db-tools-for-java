package com.microsoft.azure.elasticdb.core.commons.helpers;

// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

public class Resources {
    public static String ArgumentCannotBeGreaterThanBaseline = "The specified argument %1$s cannot be greater than its ceiling value of %2$s.";
    public static String ArgumentCannotBeNegative = "The specified argument %1$s cannot be initialized with a negative value.";
    public static String DefaultRetryStrategyMappingNotFound = "Default retry strategy for technology %1$s, named '%2$s', is not defined.";
    public static String DefaultRetryStrategyNotFound = "Default retry strategy for technology %1$s was not not defined, and there is no overall default strategy.";
    public static String ExceptionRetryManagerAlreadySet = "The RetryManager is already set.";
    public static String ExceptionRetryManagerNotSet = "The default RetryManager has not been set. Set it by invoking the RetryManager.SetDefault static method, or if you are using declarative configuration, you can invoke the RetryPolicyFactory.CreateDefault() method to automatically create the retry manager from the configuration file.";
    public static String RetryLimitExceeded = "The action has exceeded its defined retry limit.";
    public static String RetryStrategyNotFound = "The retry strategy with name '%1$s' cannot be found.";
    public static String StringCannotBeEmpty = "The specified string argument %1$s must not be empty.";
    public static String TaskCannotBeNull = "The specified argument '%1$s' cannot return a null task when invoked.";
    public static String TaskMustBeScheduled = "The specified argument '%1$s' must return a scheduled task (also known as 'hot' task) when invoked.";

}

package com.microsoft.azure.elasticdb.shard.utils;

import java.io.File;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.function.Function;

/*
 * Elastic database tools for Azure SQL Database.
 * 
 * Copyright(c) Microsoft Corporation All rights reserved.
 * 
 * This program is made available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

import com.microsoft.azure.elasticdb.core.commons.transientfaulthandling.SqlDatabaseTransientErrorDetectionStrategy;
import com.microsoft.azure.elasticdb.shard.mapmanager.ShardManagementException;
import com.microsoft.azure.elasticdb.shard.store.StoreException;
import com.microsoft.azure.elasticdb.shard.store.Version;

/**
 * Utility properties and methods used for managing scripts and errors.
 */
public final class SqlUtils {

    /**
     * Special version number representing first step in upgrade script.
     */
    private static final int MAJOR_NUMBER_FOR_INITIAL_UPGRADE_STEP = 0;

    /**
     * Special version number representing last step in upgrade script. Keep this number in sync with upgrade t-sql scripts.
     */
    private static final int MAJOR_NUMBER_FOR_FINAL_UPGRADE_STEP = 1000;
    /**
     * Parsed representation of GSM existence check script.
     */
    private static final List<StringBuilder> CHECK_IF_EXISTS_GLOBAL_SCRIPT = SqlUtils.splitScriptCommands(Scripts.getCheckShardMapManagerGlobal());
    /**
     * Parsed representation of GSM creation script.
     */
    private static final List<StringBuilder> CREATE_GLOBAL_SCRIPT = SqlUtils.splitScriptCommands(Scripts.getCreateShardMapManagerGlobal());
    /**
     * Parsed representation of GSM drop script.
     */
    private static final List<StringBuilder> DROP_GLOBAL_SCRIPT = SqlUtils.splitScriptCommands(Scripts.getDropShardMapManagerGlobal());
    /**
     * Parsed representation of GSM upgrade script.
     */
    private static final List<UpgradeSteps> UPGRADE_GLOBAL_SCRIPT = SqlUtils.parseUpgradeScripts(false);
    /**
     * Parsed representation of LSM existence check script.
     */
    private static final List<StringBuilder> CHECK_IF_EXISTS_LOCAL_SCRIPT = SqlUtils.splitScriptCommands(Scripts.getCheckShardMapManagerLocal());
    /**
     * Parsed representation of LSM creation script.
     */
    private static final List<StringBuilder> CREATE_LOCAL_SCRIPT = SqlUtils.splitScriptCommands(Scripts.getCreateShardMapManagerLocal());
    /**
     * Parsed representation of LSM drop script.
     */
    private static final List<StringBuilder> DROP_LOCAL_SCRIPT = SqlUtils.splitScriptCommands(Scripts.getDropShardMapManagerLocal());
    /**
     * Parsed representation of LSM upgrade script.
     */
    private static final List<UpgradeSteps> UPGRADE_LOCAL_SCRIPT = SqlUtils.parseUpgradeScripts(true);
    /**
     * SQL transient fault detection strategy.
     */
    private static SqlDatabaseTransientErrorDetectionStrategy sqlTransientErrorDetector = new SqlDatabaseTransientErrorDetectionStrategy();
    /**
     * Transient failure detector function.
     */
    private static Function<Exception, Boolean> transientErrorDetector = (e) -> {
        ShardManagementException smmException;
        StoreException storeException;
        SQLException sqlException;

        smmException = (ShardManagementException) ((e instanceof ShardManagementException) ? e : null);

        if (smmException != null) {
            storeException = (StoreException) ((smmException.getCause() instanceof StoreException) ? smmException.getCause() : null);
        }
        else {
            storeException = (StoreException) ((e instanceof StoreException) ? e : null);
        }

        if (storeException != null) {
            sqlException = (SQLException) ((storeException.getCause() instanceof SQLException) ? storeException.getCause() : null);
        }
        else {
            sqlException = (SQLException) ((e instanceof SQLException) ? e : null);
        }
        return sqlTransientErrorDetector.isTransient((sqlException != null) ? new RuntimeException(sqlException) : e);
    };

    /**
     * Transient failure detector function.
     */
    public static Function<Exception, Boolean> getTransientErrorDetector() {
        return SqlUtils.transientErrorDetector;
    }

    /**
     * Parsed representation of GSM existence check script.
     */
    public static List<StringBuilder> getCheckIfExistsGlobalScript() {
        return SqlUtils.CHECK_IF_EXISTS_GLOBAL_SCRIPT;
    }

    /**
     * Parsed representation of GSM creation script.
     */
    public static List<StringBuilder> getCreateGlobalScript() {
        return SqlUtils.CREATE_GLOBAL_SCRIPT;
    }

    /**
     * Parsed representation of GSM drop script.
     */
    public static List<StringBuilder> getDropGlobalScript() {
        return SqlUtils.DROP_GLOBAL_SCRIPT;
    }

    /**
     * Parsed representation of GSM upgrade script.
     */
    public static List<UpgradeSteps> getUpgradeGlobalScript() {
        return SqlUtils.UPGRADE_GLOBAL_SCRIPT;
    }

    /**
     * Parsed representation of LSM existence check script.
     */
    public static List<StringBuilder> getCheckIfExistsLocalScript() {
        return SqlUtils.CHECK_IF_EXISTS_LOCAL_SCRIPT;
    }

    /**
     * Parsed representation of LSM creation script.
     */
    public static List<StringBuilder> getCreateLocalScript() {
        return SqlUtils.CREATE_LOCAL_SCRIPT;
    }

    /**
     * Parsed representation of LSM drop script.
     */
    public static List<StringBuilder> getDropLocalScript() {
        return SqlUtils.DROP_LOCAL_SCRIPT;
    }

    /**
     * Parsed representation of LSM upgrade script.
     */
    public static List<UpgradeSteps> getUpgradeLocalScript() {
        return SqlUtils.UPGRADE_LOCAL_SCRIPT;
    }

    /**
     * Executes the code with SqlException handling.
     *
     * @param operation
     *            Operation to execute.
     */
    public static void withSqlExceptionHandling(Runnable operation) {
        try {
            operation.run();
        }
        catch (Exception se) {
            throw new StoreException(Errors._Store_StoreException, se);
        }
    }

    /**
     * Executes the code with SqlException handling. <typeparam name="ResultT">Type of result.</typeparam>
     *
     * @param operation
     *            Operation to execute.
     * @return Result of the operation.
     */
    public static <ResultT> ResultT withSqlExceptionHandling(Callable<ResultT> operation) {
        try {
            return operation.call();
        }
        catch (Exception se) {
            throw new StoreException(Errors._Store_StoreException, se);
        }
    }

    /**
     * Asynchronously executes the code with SqlException handling. <typeparam name="ResultT">Type of result.</typeparam>
     *
     * @param operationAsync
     *            Operation to execute.
     * @return Task encapsulating the result of the operation.
     */
    public static <ResultT> Callable<ResultT> withSqlExceptionHandlingAsync(Callable<Callable<ResultT>> operationAsync) {
        try {
            return operationAsync.call();
        }
        catch (Exception se) {
            throw new StoreException(Errors._Store_StoreException, se);
        }
    }

    public static List<StringBuilder> filterUpgradeCommands(List<UpgradeSteps> commandList,
            Version targetVersion) {
        return filterUpgradeCommands(commandList, targetVersion, null);
    }

    /**
     * Filters collection of upgrade steps based on the specified target version of store.
     *
     * @param commandList
     *            Collection of upgrade steps.
     * @param targetVersion
     *            Target version of store.
     * @param currentVersion
     *            Current version of store.
     * @return Collection of string builder that represent batches of commands to upgrade store to specified target version.
     */
    public static List<StringBuilder> filterUpgradeCommands(List<UpgradeSteps> commandList,
            Version targetVersion,
            Version currentVersion) {
        ArrayList<StringBuilder> list = new ArrayList<>();

        for (UpgradeSteps s : commandList) {
            // For every upgrade step, add it to the output list if its initial version
            // satisfy one of the 3 criteria below:
            // 1. If it is part of initial upgrade step (from version 0.0 to 1.0)
            // which acquires SCH-M lock on ShardMapManagerGlobal
            // 2. If initial version is greater than current store version
            // and less than target version requested
            // 3. If it is part of final upgrade step which releases SCH-M lock on ShardMapManagerGlobal

            if ((s.getInitialMajorVersion() == MAJOR_NUMBER_FOR_INITIAL_UPGRADE_STEP)
                    || ((currentVersion == null || s.getInitialMajorVersion() > currentVersion.getMajor()
                            || (s.getInitialMajorVersion() == currentVersion.getMajor() && s.getInitialMinorVersion() >= currentVersion.getMinor()))
                            && (s.getInitialMajorVersion() < targetVersion.getMajor() || (s.getInitialMajorVersion() == targetVersion.getMajor()
                                    && s.getInitialMinorVersion() < targetVersion.getMinor())))
                    || (s.getInitialMajorVersion() == MAJOR_NUMBER_FOR_FINAL_UPGRADE_STEP)) {
                list.add(s.getCommands());
            }
        }

        return list;
    }

    /**
     * Splits the input script into batches of individual commands, the go token is considered the separation boundary. Also skips comment lines.
     *
     * @param scriptName
     *            Resource path of the script file.
     * @return Collection of string builder that represent batches of commands.
     */
    private static List<StringBuilder> splitScriptCommands(String scriptName) {
        return Scripts.readScriptContent(scriptName);
    }

    private static List<UpgradeSteps> parseUpgradeScripts() {
        return parseUpgradeScripts(false);
    }

    /**
     * Split upgrade scripts into batches of upgrade steps, the go token is considered as separation boundary of batches.
     *
     * @param parseLocal
     *            Whether to parse ShardMapManagerLocal upgrade scripts, default = false
     * @return List of upgrade steps
     */
    private static List<UpgradeSteps> parseUpgradeScripts(boolean parseLocal) {
        ArrayList<UpgradeSteps> upgradeSteps = new ArrayList<>();

        final String prefix = StringUtilsLocal.formatInvariant("UpgradeShardMapManager%sFrom", (parseLocal ? "Local" : "Global"));

        // Filter upgrade scripts based on file name and order by initial Major.Minor version
        File[] scripts = new File(Scripts.buildResourcePath()).listFiles((dir,
                name) -> name.startsWith(prefix) && name.toLowerCase().endsWith(".sql"));

        Arrays.stream(scripts).sorted(Comparator.comparing(s -> s.getName().replace(prefix, "").split("To")[0])).forEachOrdered(s -> {
            String name = s.getName();
            String[] versions = name.replace(prefix, "").split("To")[0].split("\\.");
            int initialMajorVersion = Integer.parseInt(versions[0]);
            int initialMinorVersion = Integer.parseInt(versions[1]);
            for (StringBuilder cmd : splitScriptCommands(Scripts.buildResourcePath(name))) {
                upgradeSteps.add(new UpgradeSteps(initialMajorVersion, initialMinorVersion, cmd));
            }
        });

        return upgradeSteps;
    }

    /**
     * structure to hold upgrade command batches along with the starting version to apply the upgrade step.
     */
    public static final class UpgradeSteps {

        /**
         * Major version to apply this upgrade step.
         */
        private int initialMajorVersion;
        /**
         * Minor version to apply this upgrade step.
         */
        private int initialMinorVersion;
        /**
         * Commands in this upgrade step batch. These will be executed only when store is at (this.InitialMajorVersion, this.InitialMinorVersion).
         */
        private StringBuilder commands;

        public UpgradeSteps() {
        }

        /**
         * Construct upgrade steps.
         *
         * @param initialMajorVersion
         *            Expected major version of store to run this upgrade step.
         * @param initialMinorVersion
         *            Expected minor version of store to run this upgrade step.
         * @param commands
         *            Commands to execute as part of this upgrade step.
         */
        public UpgradeSteps(int initialMajorVersion,
                int initialMinorVersion,
                StringBuilder commands) {
            this();
            this.setInitialMajorVersion(initialMajorVersion);
            this.setInitialMinorVersion(initialMinorVersion);
            this.setCommands(commands);
        }

        public int getInitialMajorVersion() {
            return initialMajorVersion;
        }

        private void setInitialMajorVersion(int value) {
            initialMajorVersion = value;
        }

        public int getInitialMinorVersion() {
            return initialMinorVersion;
        }

        private void setInitialMinorVersion(int value) {
            initialMinorVersion = value;
        }

        public StringBuilder getCommands() {
            return commands;
        }

        public void setCommands(StringBuilder value) {
            commands = value;
        }

        /**
         * Clones current Instance.
         *
         * @return clone of current Instance.
         */
        public UpgradeSteps clone() {
            UpgradeSteps varCopy = new UpgradeSteps();
            varCopy.setInitialMajorVersion(this.getInitialMajorVersion());
            varCopy.setInitialMinorVersion(this.getInitialMinorVersion());
            varCopy.commands = this.commands;

            return varCopy;
        }
    }
}

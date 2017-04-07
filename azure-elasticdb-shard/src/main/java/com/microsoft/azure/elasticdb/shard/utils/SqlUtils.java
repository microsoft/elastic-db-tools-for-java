package com.microsoft.azure.elasticdb.shard.utils;

// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

import com.microsoft.azure.elasticdb.core.commons.transientfaulthandling.SqlDatabaseTransientErrorDetectionStrategy;
import com.microsoft.azure.elasticdb.shard.mapmanager.ShardManagementException;
import com.microsoft.azure.elasticdb.shard.store.StoreException;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.function.Function;

/**
 * Utility properties and methods used for managing scripts and errors.
 */
public final class SqlUtils {
    /**
     * Regular expression for go tokens.
     */
    private static final String s_goTokenRegularExpression = "go";

    /**
     * Regular expression for comment lines.
     */
    private static final String s_commentLineRegularExpression = "--";

    /**
     * Special version number representing first step in upgrade script.
     */
    private static final int MajorNumberForInitialUpgradeStep = 0;

    /**
     * Special version number representing last step in upgrade script.
     * Keep this number in sync with upgrade t-sql scripts.
     */
    private static final int MajorNumberForFinalUpgradeStep = 1000;
    /**
     * Parsed representation of GSM existence check script.
     */
    private static final List<StringBuilder> s_checkIfExistsGlobalScript = SqlUtils.SplitScriptCommands(Scripts.getCheckShardMapManagerGlobal());
    /**
     * Parsed representation of GSM creation script.
     */
    private static final List<StringBuilder> s_createGlobalScript = SqlUtils.SplitScriptCommands(Scripts.getCreateShardMapManagerGlobal());
    /**
     * Parsed representation of GSM drop script.
     */
    private static final List<StringBuilder> s_dropGlobalScript = SqlUtils.SplitScriptCommands(Scripts.getDropShardMapManagerGlobal());
    /**
     * Parsed representation of GSM upgrade script.
     */
    private static final List<UpgradeSteps> s_upgradeGlobalScript = SqlUtils.ParseUpgradeScripts(false);
    /**
     * Parsed representation of LSM existence check script.
     */
    private static final List<StringBuilder> s_checkIfExistsLocalScript = SqlUtils.SplitScriptCommands(Scripts.getCheckShardMapManagerLocal());
    /**
     * Parsed representation of LSM creation script.
     */
    private static final List<StringBuilder> s_createLocalScript = SqlUtils.SplitScriptCommands(Scripts.getCreateShardMapManagerLocal());
    /**
     * Parsed representation of LSM drop script.
     */
    private static final List<StringBuilder> s_dropLocalScript = SqlUtils.SplitScriptCommands(Scripts.getDropShardMapManagerLocal());
    /**
     * Parsed representation of LSM upgrade script.
     */
    private static final List<UpgradeSteps> s_upgradeLocalScript = SqlUtils.ParseUpgradeScripts(true);
    /**
     * SQL transient fault detection strategy.
     */
    private static SqlDatabaseTransientErrorDetectionStrategy s_sqlTransientErrorDetector = new SqlDatabaseTransientErrorDetectionStrategy();
    /**
     * Transient failure detector function.
     */
    private static Function<Exception, Boolean> s_transientErrorDetector = (e) -> {
        ShardManagementException smmException = null;
        StoreException storeException = null;
        SQLException sqlException = null;

        smmException = (ShardManagementException) ((e instanceof ShardManagementException) ? e : null);

        if (smmException != null) {
            storeException = (StoreException) ((smmException.getCause() instanceof StoreException) ? smmException.getCause() : null);
        } else {
            storeException = (StoreException) ((e instanceof StoreException) ? e : null);
        }

        if (storeException != null) {
            sqlException = (SQLException) ((storeException.getCause() instanceof SQLException) ? storeException.getCause() : null);
        } else {
            sqlException = (SQLException) ((e instanceof SQLException) ? e : null);
        }
        return s_sqlTransientErrorDetector.isTransient((sqlException != null) ? new RuntimeException(sqlException) : e);
    };

    /**
     * Transient failure detector function.
     */
    public static Function<Exception, Boolean> getTransientErrorDetector() {
        return SqlUtils.s_transientErrorDetector;
    }

    /**
     * Parsed representation of GSM existence check script.
     */
    public static List<StringBuilder> getCheckIfExistsGlobalScript() {
        return SqlUtils.s_checkIfExistsGlobalScript;
    }

    /**
     * Parsed representation of GSM creation script.
     */
    public static List<StringBuilder> getCreateGlobalScript() {
        return SqlUtils.s_createGlobalScript;
    }

    /**
     * Parsed representation of GSM drop script.
     */
    public static List<StringBuilder> getDropGlobalScript() {
        return SqlUtils.s_dropGlobalScript;
    }

    /**
     * Parsed representation of GSM upgrade script.
     */
    public static List<UpgradeSteps> getUpgradeGlobalScript() {
        return SqlUtils.s_upgradeGlobalScript;
    }

    /**
     * Parsed representation of LSM existence check script.
     */
    public static List<StringBuilder> getCheckIfExistsLocalScript() {
        return SqlUtils.s_checkIfExistsLocalScript;
    }

    /**
     * Parsed representation of LSM creation script.
     */
    public static List<StringBuilder> getCreateLocalScript() {
        return SqlUtils.s_createLocalScript;
    }

    /**
     * Parsed representation of LSM drop script.
     */
    public static List<StringBuilder> getDropLocalScript() {
        return SqlUtils.s_dropLocalScript;
    }

    /**
     * Parsed representation of LSM upgrade script.
     */
    public static List<UpgradeSteps> getUpgradeLocalScript() {
        return SqlUtils.s_upgradeLocalScript;
    }

    /**
     * Reads a varbinary column from the given reader.
     *
     * @param reader   Input reader.
     * @param colIndex Index of the column.
     * @return Buffer representing the data value.
     */
    //TODO
    /*public static byte[] ReadSqlBytes(SqlDataReader reader, int colIndex) {
        assert reader != null;

        SqlBytes data = reader.GetSqlBytes(colIndex);

        if (data.IsNull) {
            return null;
        } else {
            byte[] buffer = new byte[data.getLength()];

            data.Read(0, buffer, 0, (int) data.getLength());

            return buffer;
        }
    }*/

    /**
     * Adds parameter to given command.
     *
     * @param cmd           Command to add parameter to.
     * @param parameterName Parameter name.
     * @param dbType        Parameter type.
     * @param direction     Parameter direction.
     * @param size          Size of parameter, useful for variable length types only.
     * @param value         Parameter value.
     * @return Parameter object this created.
     */
    //TODO
    /*public static SqlParameter AddCommandParameter(SqlCommand cmd, String parameterName, SqlDbType dbType, ParameterDirection direction, int size, Object value) {
        SqlParameter p = new SqlParameter(parameterName, dbType);
        p.setDirection(direction);
        p.setValue((value != null) ? value : DBNull.Value);

        if ((dbType == SqlDbType.NVarChar) || (dbType == SqlDbType.VarBinary)) {
            p.Size = size;
        }

        cmd.Parameters.Add(p);

        return p;
    }*/

    /**
     * Executes the code with SqlException handling.
     *
     * @param operation Operation to execute.
     */
    public static void WithSqlExceptionHandling(Runnable operation) {
        try {
            operation.run();
        } catch (Exception se) {
            throw new StoreException(Errors._Store_StoreException, se);
        }
    }

    /**
     * Executes the code asynchronously with SqlException handling.
     *
     * @param operationAsync Operation to execute.
     * @return Task to await sql exception handling completion
     */
    //TODO
    /*public static Callable WithSqlExceptionHandlingAsync(Function<Callable> operationAsync) {
        try {
            await operationAsync.invoke().ConfigureAwait(false);
        } catch (SQLException se) {
            throw new StoreException(Errors._Store_StoreException, se);
        }
    }*/

    /**
     * Executes the code with SqlException handling.
     * <p>
     * <typeparam name="TResult">Type of result.</typeparam>
     *
     * @param operation Operation to execute.
     * @return Result of the operation.
     */
    public static <TResult> TResult WithSqlExceptionHandling(Callable<TResult> operation) {
        try {
            return operation.call();
        } catch (Exception se) {
            throw new StoreException(Errors._Store_StoreException, se);
        }
    }

    /**
     * Asynchronously executes the code with SqlException handling.
     * <p>
     * <typeparam name="TResult">Type of result.</typeparam>
     *
     * @param operationAsync Operation to execute.
     * @return Task encapsulating the result of the operation.
     */
    public static <TResult> Callable<TResult> WithSqlExceptionHandlingAsync(Callable<Callable<TResult>> operationAsync) {
        try {
            return operationAsync.call();
        } catch (Exception se) {
            throw new StoreException(Errors._Store_StoreException, se);
        }
    }

    public static List<StringBuilder> FilterUpgradeCommands(List<UpgradeSteps> commandList, Version targetVersion) {
        return FilterUpgradeCommands(commandList, targetVersion, null);
    }

    /**
     * Filters collection of upgrade steps based on the specified target version of store.
     *
     * @param commandList    Collection of upgrade steps.
     * @param targetVersion  Target version of store.
     * @param currentVersion Current version of store.
     * @return Collection of string builder that represent batches of commands to upgrade store to specified target version.
     */
    public static List<StringBuilder> FilterUpgradeCommands(List<UpgradeSteps> commandList, Version targetVersion, Version currentVersion) {
        ArrayList<StringBuilder> list = new ArrayList<StringBuilder>();

        for (UpgradeSteps s : commandList) {
            // For every upgrade step, add it to the output list if its initial version satisfy one of the 3 criteria below:
            // 1. If it is part of initial upgrade step (from version 0.0 to 1.0) which acquires SCH-M lock on ShardMapManagerGlobal
            // 2. If initial version is greater than current store version and less than target version requested
            // 3. If it is part of final upgrade step which releases SCH-M lock on ShardMapManagerGlobal

            if ((s.getInitialMajorVersion() == MajorNumberForInitialUpgradeStep) || ((currentVersion == null || s.getInitialMajorVersion() > currentVersion.getMajor() || (s.getInitialMajorVersion() == currentVersion.getMajor() && s.getInitialMinorVersion() >= currentVersion.getMinor())) && (s.getInitialMajorVersion() < targetVersion.getMajor() || (s.getInitialMajorVersion() == targetVersion.getMajor() && s.getInitialMinorVersion() < targetVersion.getMinor()))) || (s.getInitialMajorVersion() == MajorNumberForFinalUpgradeStep)) {
                list.add(s.getCommands());
            }
        }

        return list;
    }

    /**
     * Splits the input script into batches of individual commands, the go token is
     * considered the separation boundary. Also skips comment lines.
     *
     * @param script Input script.
     * @return Collection of string builder that represent batches of commands.
     */
    private static List<StringBuilder> SplitScriptCommands(String script) {
        return Scripts.readFileContent(script);
    }

    private static List<UpgradeSteps> ParseUpgradeScripts() {
        return ParseUpgradeScripts(false);
    }

    /**
     * Split upgrade scripts into batches of upgrade steps, the go token is
     * considered as separation boundary of batches.
     *
     * @param parseLocal Whether to parse ShardMapManagerLocal upgrade scripts, default = false
     * @return
     */
    private static List<UpgradeSteps> ParseUpgradeScripts(boolean parseLocal) {
        /*ArrayList<UpgradeSteps> upgradeSteps = new ArrayList<UpgradeSteps>();

        ResourceSet rs = Scripts.ResourceManager.GetResourceSet(System.Globalization.CultureInfo.CurrentCulture, true, true);

        String upgradeFileNameFilter = "^UpgradeShardMapManagerGlobalFrom(\\d*).(\\d*)";

        if (parseLocal) {
            upgradeFileNameFilter = upgradeFileNameFilter.replace("Global", "Local");
        }

        Regex fileNameRegEx = new Regex(upgradeFileNameFilter, RegexOptions.IgnoreCase.getValue() | RegexOptions.CultureInvariant.getValue());

        // Filter upgrade scripts based on file name and order by initial Major.Minor version
        Version((int) (m.Groups[1].Value), (int) (m.Groups[2].Value)) select
        new tempVar = new Version((int) (m.Groups[1].Value), (int) (m.Groups[2].Value)) select new ();
        tempVar.Key = r.Key;
        tempVar.Value = r.Value;
        tempVar.initialMajorVersion = (int) (m.Groups[1].Value);
        tempVar.initialMinorVersion = (int) (m.Groups[2].Value);
//TODO TASK: There is no equivalent to implicit typing in Java:
//TODO TASK: There is no Java equivalent to LINQ queries:
        var upgradeScriptObjects = from r in rs.<DictionaryEntry>Cast() let m = fileNameRegEx.Match(r.Key.toString())
        where m.Success orderby tempVar;

//TODO TASK: There is no equivalent to implicit typing in Java:
        for (var entry : upgradeScriptObjects) {
            for (StringBuilder cmd : SplitScriptCommands(entry.Value.toString())) {
                upgradeSteps.add(new UpgradeSteps(entry.initialMajorVersion, entry.initialMinorVersion, cmd));
            }
        }

        return upgradeSteps;*/
        return null; //TODO
    }

    /**
     * structure to hold upgrade command batches along with the starting version to apply the upgrade step.
     */
    public final static class UpgradeSteps {
        /**
         * Major version to apply this upgrade step.
         */
        private int _initialMajorVersion;
        /**
         * Minor version to apply this upgrade step.
         */
        private int _initialMinorVersion;
        /**
         * Commands in this upgrade step batch. These will be executed only when store is at (this.InitialMajorVersion, this.InitialMinorVersion).
         */
        private StringBuilder Commands;

        public UpgradeSteps() {
        }

        /**
         * Construct upgrade steps.
         *
         * @param initialMajorVersion Expected major version of store to run this upgrade step.
         * @param initialMinorVersion Expected minor version of store to run this upgrade step.
         * @param commands            Commands to execute as part of this upgrade step.
         */
        public UpgradeSteps(int initialMajorVersion, int initialMinorVersion, StringBuilder commands) {
            this();
            this.setInitialMajorVersion(initialMajorVersion);
            this.setInitialMinorVersion(initialMinorVersion);
            this.setCommands(commands);
        }

        public int getInitialMajorVersion() {
            return _initialMajorVersion;
        }

        private void setInitialMajorVersion(int value) {
            _initialMajorVersion = value;
        }

        public int getInitialMinorVersion() {
            return _initialMinorVersion;
        }

        private void setInitialMinorVersion(int value) {
            _initialMinorVersion = value;
        }

        public StringBuilder getCommands() {
            return Commands;
        }

        private void setCommands(StringBuilder value) {
            Commands = value;
        }

        public UpgradeSteps clone() {
            UpgradeSteps varCopy = new UpgradeSteps();
            varCopy.setInitialMajorVersion(this.getInitialMajorVersion());
            varCopy.setInitialMinorVersion(this.getInitialMinorVersion());
            varCopy.Commands = this.Commands;

            return varCopy;
        }
    }
}

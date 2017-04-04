package com.microsoft.azure.elasticdb.shard.utils;

// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

import com.microsoft.azure.elasticdb.core.commons.helpers.ActionGeneric;
import com.microsoft.azure.elasticdb.core.commons.helpers.ActionGeneric1Param;
import com.microsoft.azure.elasticdb.shard.mapmanager.ShardManagementException;
import com.microsoft.azure.elasticdb.shard.store.StoreException;
import javafx.concurrent.Task;
import jdk.nashorn.internal.runtime.regexp.joni.Regex;

import java.util.List;

/**
 * Utility properties and methods used for managing scripts and errors.
 */
public final class SqlUtils {
    /**
     * Regular expression for go tokens.
     */
    private static final Regex s_goTokenRegularExpression = new Regex("^\\s*go\\s*$");

    /**
     * Regular expression for comment lines.
     */
    private static final Regex s_commentLineRegularExpression = new Regex("^\\s*--");

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
    private static final Lazy<List<StringBuilder>> s_checkIfExistsGlobalScript = new Lazy<List<StringBuilder>>(() -> SqlUtils.SplitScriptCommands(ReadOnlyScripts.CheckShardMapManagerGlobal), LazyThreadSafetyMode.PublicationOnly);
    /**
     * Parsed representation of GSM creation script.
     */
    private static final Lazy<List<StringBuilder>> s_createGlobalScript = new Lazy<List<StringBuilder>>(() -> SqlUtils.SplitScriptCommands(Scripts.CreateShardMapManagerGlobal), LazyThreadSafetyMode.PublicationOnly);
    /**
     * Parsed representation of GSM drop script.
     */
    private static final Lazy<List<StringBuilder>> s_dropGlobalScript = new Lazy<List<StringBuilder>>(() -> SqlUtils.SplitScriptCommands(Scripts.DropShardMapManagerGlobal), LazyThreadSafetyMode.PublicationOnly);
    /**
     * Parsed representation of GSM upgrade script.
     */
    private static final Lazy<List<UpgradeSteps>> s_upgradeGlobalScript = new Lazy<List<UpgradeSteps>>(() -> SqlUtils.ParseUpgradeScripts(false), LazyThreadSafetyMode.PublicationOnly);
    /**
     * Parsed representation of LSM existence check script.
     */
    private static final Lazy<List<StringBuilder>> s_checkIfExistsLocalScript = new Lazy<List<StringBuilder>>(() -> SqlUtils.SplitScriptCommands(ReadOnlyScripts.CheckShardMapManagerLocal), LazyThreadSafetyMode.PublicationOnly);
    /**
     * Parsed representation of LSM creation script.
     */
    private static final Lazy<List<StringBuilder>> s_createLocalScript = new Lazy<List<StringBuilder>>(() -> SqlUtils.SplitScriptCommands(Scripts.CreateShardMapManagerLocal), LazyThreadSafetyMode.PublicationOnly);
    /**
     * Parsed representation of LSM drop script.
     */
    private static final Lazy<List<StringBuilder>> s_dropLocalScript = new Lazy<List<StringBuilder>>(() -> SqlUtils.SplitScriptCommands(Scripts.DropShardMapManagerLocal), LazyThreadSafetyMode.PublicationOnly);
    /**
     * Parsed representation of LSM upgrade script.
     */
    private static final Lazy<List<UpgradeSteps>> s_upgradeLocalScript = new Lazy<List<UpgradeSteps>>(() -> SqlUtils.ParseUpgradeScripts(true), LazyThreadSafetyMode.PublicationOnly);
    /**
     * SQL transient fault detection strategy.
     */
    private static SqlDatabaseTransientErrorDetectionStrategy s_sqlTransientErrorDetector = new SqlDatabaseTransientErrorDetectionStrategy();
    /**
     * Transient failure detector function.
     */
    private static ActionGeneric1Param<RuntimeException, Boolean> s_transientErrorDetector = (e) -> {
        ShardManagementException smmException = null;
        StoreException storeException = null;
        SqlException sqlException = null;

        smmException = (ShardManagementException) ((e instanceof ShardManagementException) ? e : null);

        if (smmException != null) {
            storeException = (StoreException) ((smmException.getCause() instanceof StoreException) ? smmException.getCause() : null);
        } else {
            storeException = (StoreException) ((e instanceof StoreException) ? e : null);
        }

        if (storeException != null) {
            sqlException = (SqlException) ((storeException.getCause() instanceof SqlException) ? storeException.getCause() : null);
        } else {
            sqlException = (SqlException) ((e instanceof SqlException) ? e : null);
        }
        return s_sqlTransientErrorDetector.IsTransient((sqlException != null) ? sqlException : e);
    };

    /**
     * Transient failure detector function.
     */
    public static ActionGeneric1Param<RuntimeException, Boolean> getTransientErrorDetector() {
        return SqlUtils.s_transientErrorDetector;
    }

    /**
     * Parsed representation of GSM existence check script.
     */
    public static List<StringBuilder> getCheckIfExistsGlobalScript() {
        return SqlUtils.s_checkIfExistsGlobalScript.Value;
    }

    /**
     * Parsed representation of GSM creation script.
     */
    public static List<StringBuilder> getCreateGlobalScript() {
        return SqlUtils.s_createGlobalScript.Value;
    }

    /**
     * Parsed representation of GSM drop script.
     */
    public static List<StringBuilder> getDropGlobalScript() {
        return SqlUtils.s_dropGlobalScript.Value;
    }

    /**
     * Parsed representation of GSM upgrade script.
     */
    public static List<UpgradeSteps> getUpgradeGlobalScript() {
        return SqlUtils.s_upgradeGlobalScript.Value;
    }

    /**
     * Parsed representation of LSM existence check script.
     */
    public static List<StringBuilder> getCheckIfExistsLocalScript() {
        return SqlUtils.s_checkIfExistsLocalScript.Value;
    }

    /**
     * Parsed representation of LSM creation script.
     */
    public static List<StringBuilder> getCreateLocalScript() {
        return SqlUtils.s_createLocalScript.Value;
    }

    /**
     * Parsed representation of LSM drop script.
     */
    public static List<StringBuilder> getDropLocalScript() {
        return SqlUtils.s_dropLocalScript.Value;
    }

    /**
     * Parsed representation of LSM upgrade script.
     */
    public static List<UpgradeSteps> getUpgradeLocalScript() {
        return SqlUtils.s_upgradeLocalScript.Value;
    }

    /**
     * Reads a varbinary column from the given reader.
     *
     * @param reader   Input reader.
     * @param colIndex Index of the column.
     * @return Buffer representing the data value.
     */
//WARNING: Unsigned integer types have no direct equivalent in Java:
//ORIGINAL LINE: internal static byte[] ReadSqlBytes(SqlDataReader reader, int colIndex)
    public static byte[] ReadSqlBytes(SqlDataReader reader, int colIndex) {
        assert reader != null;

        SqlBytes data = reader.GetSqlBytes(colIndex);

        if (data.IsNull) {
            return null;
        } else {
//WARNING: Unsigned integer types have no direct equivalent in Java:
//ORIGINAL LINE: byte[] buffer = new byte[data.Length];
            byte[] buffer = new byte[data.getLength()];

            data.Read(0, buffer, 0, (int) data.getLength());

            return buffer;
        }
    }

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
    public static SqlParameter AddCommandParameter(SqlCommand cmd, String parameterName, SqlDbType dbType, ParameterDirection direction, int size, Object value) {
        SqlParameter p = new SqlParameter(parameterName, dbType);
        p.setDirection(direction);
        p.setValue((value != null) ? value : DBNull.Value);

        if ((dbType == SqlDbType.NVarChar) || (dbType == SqlDbType.VarBinary)) {
            p.Size = size;
        }

        cmd.Parameters.Add(p);

        return p;
    }

    /**
     * Executes the code with SqlException handling.
     *
     * @param operation Operation to execute.
     */
    public static void WithSqlExceptionHandling(ActionGeneric operation) {
        try {
            operation.invoke();
        } catch (SqlException se) {
            throw new StoreException(Errors._Store_StoreException, se);
        }
    }

    /**
     * Executes the code asynchronously with SqlException handling.
     *
     * @param operationAsync Operation to execute.
     * @return Task to await sql exception handling completion
     */
    public static Task WithSqlExceptionHandlingAsync(ActionGeneric1Param<Task> operationAsync) {
        try {
            await operationAsync.invoke().ConfigureAwait(false);
        } catch (SqlException se) {
            throw new StoreException(Errors._Store_StoreException, se);
        }
    }

    /**
     * Executes the code with SqlException handling.
     * <p>
     * <typeparam name="TResult">Type of result.</typeparam>
     *
     * @param operation Operation to execute.
     * @return Result of the operation.
     */
    public static <TResult> TResult WithSqlExceptionHandling(ActionGeneric<TResult> operation) {
        try {
            return operation.invoke();
        } catch (SqlException se) {
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
    public static <TResult> Task<TResult> WithSqlExceptionHandlingAsync(ActionGeneric<Task<TResult>> operationAsync) {
        try {
            return await operationAsync.invoke().ConfigureAwait(false);
        } catch (SqlException se) {
            throw new StoreException(Errors._Store_StoreException, se);
        }
    }

    /**
     * Filters collection of upgrade steps based on the specified target version of store.
     *
     * @param commandList    Collection of upgrade steps.
     * @param targetVersion  Target version of store.
     * @param currentVersion Current version of store.
     * @return Collection of string builder that represent batches of commands to upgrade store to specified target version.
     */

    public static List<StringBuilder> FilterUpgradeCommands(List<UpgradeSteps> commandList, Version targetVersion) {
        return FilterUpgradeCommands(commandList, targetVersion, null);
    }

    public static List<StringBuilder> FilterUpgradeCommands(List<UpgradeSteps> commandList, Version targetVersion, Version currentVersion) {
        ArrayList<StringBuilder> list = new ArrayList<StringBuilder>();

        for (UpgradeSteps s : commandList) {
            // For every upgrade step, add it to the output list if its initial version satisfy one of the 3 criteria below:
            // 1. If it is part of initial upgrade step (from version 0.0 to 1.0) which acquires SCH-M lock on ShardMapManagerGlobal
            // 2. If initial version is greater than current store version and less than target version requested
            // 3. If it is part of final upgrade step which releases SCH-M lock on ShardMapManagerGlobal

            if ((s.getInitialMajorVersion() == MajorNumberForInitialUpgradeStep) || ((currentVersion == null || s.getInitialMajorVersion() > currentVersion.Major || (s.getInitialMajorVersion() == currentVersion.Major && s.getInitialMinorVersion() >= currentVersion.Minor)) && (s.getInitialMajorVersion() < targetVersion.Major || (s.getInitialMajorVersion() == targetVersion.Major && s.getInitialMinorVersion() < targetVersion.Minor))) || (s.getInitialMajorVersion() == MajorNumberForFinalUpgradeStep)) {
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
        ArrayList<StringBuilder> batches = new ArrayList<StringBuilder>();

        try (StringReader sr = new StringReader(script)) {
            StringBuilder current = new StringBuilder();
            String currentLine;

            while ((currentLine = sr.ReadLine()) != null) {
                // Break at the go token boundary.
                if (SqlUtils.s_goTokenRegularExpression.IsMatch(currentLine)) {
                    batches.add(current);
                    current = new StringBuilder();
                } else if (!SqlUtils.s_commentLineRegularExpression.IsMatch(currentLine)) {
                    // Add the line to the batch if it is not a comment.
                    current.append(currentLine + "\r\n");
                }
            }
        }

        return batches;
    }

    /**
     * Split upgrade scripts into batches of upgrade steps, the go token is
     * considered as separation boundary of batches.
     *
     * @param parseLocal Whether to parse ShardMapManagerLocal upgrade scripts, default = false
     * @return
     */

    private static List<UpgradeSteps> ParseUpgradeScripts() {
        return ParseUpgradeScripts(false);
    }

    private static List<UpgradeSteps> ParseUpgradeScripts(boolean parseLocal) {
        ArrayList<UpgradeSteps> upgradeSteps = new ArrayList<UpgradeSteps>();

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

        return upgradeSteps;
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

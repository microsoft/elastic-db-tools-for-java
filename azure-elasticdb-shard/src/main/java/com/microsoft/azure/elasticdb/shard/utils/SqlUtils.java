package com.microsoft.azure.elasticdb.shard.utils;

// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

import java.util.List;

/**
 * Utility properties and methods used for managing scripts and errors.
 */
public final class SqlUtils {
    /**
     * Special version number representing first step in upgrade script.
     */
    private static final int MajorNumberForInitialUpgradeStep = 0;

    /**
     * Special version number representing last step in upgrade script.
     * Keep this number in sync with upgrade t-sql scripts.
     */
    private static final int MajorNumberForFinalUpgradeStep = 1000;

    public static List<StringBuilder> getCheckIfExistsGlobalScript() {
        return null;
    }

    public static boolean TransientErrorDetector(RuntimeException ex) {
        return false;
    }

    /**
     * structure to hold upgrade command batches along with the starting version to apply the upgrade step.
     */
    public final static class UpgradeSteps {
        /**
         * Major version to apply this upgrade step.
         */
        private int InitialMajorVersion;
        /**
         * Minor version to apply this upgrade step.
         */
        private int InitialMinorVersion;
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
            return InitialMajorVersion;
        }

        private void setInitialMajorVersion(int value) {
            InitialMajorVersion = value;
        }

        public int getInitialMinorVersion() {
            return InitialMinorVersion;
        }

        private void setInitialMinorVersion(int value) {
            InitialMinorVersion = value;
        }

        public StringBuilder getCommands() {
            return Commands;
        }

        private void setCommands(StringBuilder value) {
            Commands = value;
        }

        public UpgradeSteps clone() {
            UpgradeSteps varCopy = new UpgradeSteps();

            varCopy.InitialMajorVersion = this.InitialMajorVersion;
            varCopy.InitialMinorVersion = this.InitialMinorVersion;
            varCopy.Commands = this.Commands;

            return varCopy;
        }
    }
}

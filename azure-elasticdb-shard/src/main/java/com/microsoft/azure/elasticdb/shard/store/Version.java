package com.microsoft.azure.elasticdb.shard.store;

// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

import javax.xml.bind.annotation.XmlElement;

/**
 * Summary:
 * Represents the version number of an assembly, operating system, or the common language runtime.
 * This class cannot be inherited.
 */
public final class Version {
    /**
     * Summary:
     * Gets the value of the major component of the version number for the current Version object.
     * <p>
     * Returns:
     * The major version number.
     */
    @XmlElement(name = "MajorVersion")
    private int Major;
    /**
     * Summary:
     * Gets the value of the minor component of the version number for the current Version object.
     * <p>
     * Returns:
     * The minor version number.
     */
    @XmlElement(name = "MinorVersion")
    private int Minor;
    /**
     * Summary:
     * Gets the value of the build component of the version number for the current Version object.
     * <p>
     * Returns:
     * The build number, or -1 if the build number is undefined.
     */
    private int Build;
    /**
     * Summary:
     * Gets the value of the revision component of the version number for the current Version object.
     * <p>
     * Returns:
     * The revision number, or -1 if the revision number is undefined.
     */
    private int Revision;

    /**
     * Summary:
     * Initializes a new instance of the Version class.
     */
    public Version() {
        this.Major = 0;
        this.Minor = 0;
        this.Build = 0;
        this.Revision = 0;
    }

    /**
     * Summary:
     * Initializes a new instance of the Version class using the specified string.
     * <p>
     * Parameters:
     * version:
     * A string containing the major, minor, build, and revision numbers, where each
     * number is delimited with a period character ('.').
     * <p>
     * TODO: Exceptions:
     * T:ArgumentException:
     * version has fewer than two components or more than four components.
     * <p>
     * T:ArgumentNullException:
     * version is null.
     * <p>
     * T:ArgumentOutOfRangeException:
     * A major, minor, build, or revision component is less than zero.
     * <p>
     * T:FormatException:
     * At least one component of version does not parse to an integer.
     * <p>
     * T:OverflowException:
     * At least one component of version represents a number greater than Int32.MaxValue.
     */
    public Version(String version) {
        String[] parts = version.split(".");
        this.Major = parts.length > 0 && parts[0] != null ? Integer.parseInt(parts[0]) : 0;
        this.Minor = parts.length > 1 && parts[1] != null ? Integer.parseInt(parts[1]) : 0;
        this.Build = parts.length > 2 && parts[2] != null ? Integer.parseInt(parts[2]) : 0;
        this.Revision = parts.length > 3 && parts[3] != null ? Integer.parseInt(parts[3]) : 0;
    }

    /**
     * Summary:
     * Initializes a new instance of the Version class using the specified major and minor values.
     * <p>
     * Parameters:
     * major:
     * The major version number.
     * <p>
     * minor:
     * The minor version number.
     * <p>
     * TODO: Exceptions:
     * T:ArgumentOutOfRangeException:
     * major or minor is less than zero.
     */
    public Version(int major, int minor) {
        this.Major = major;
        this.Minor = minor;
        this.Build = 0;
        this.Revision = 0;
    }

    /**
     * Summary:
     * Initializes a new instance of the Version class using the specified major, minor, and build values.
     * <p>
     * Parameters:
     * major:
     * The major version number.
     * <p>
     * minor:
     * The minor version number.
     * <p>
     * build:
     * The build number.
     * <p>
     * TODO: Exceptions:
     * T:ArgumentOutOfRangeException:
     * major, minor, or build is less than zero.
     */
    public Version(int major, int minor, int build) {
        this.Major = major;
        this.Minor = minor;
        this.Build = build;
        this.Revision = 0;
    }

    /**
     * Summary:
     * Initializes a new instance of the Version class with the specified major, minor, build, and revision numbers.
     * <p>
     * Parameters:
     * major:
     * The major version number.
     * <p>
     * minor:
     * The minor version number.
     * <p>
     * build:
     * The build number.
     * <p>
     * revision:
     * The revision number.
     * <p>
     * TODO: Exceptions:
     * T:ArgumentOutOfRangeException:
     * major, minor, build, or revision is less than zero.
     */
    public Version(int major, int minor, int build, int revision) {
        this.Major = major;
        this.Minor = minor;
        this.Build = build;
        this.Revision = revision;
    }

    public static boolean isFirstGreaterThan(Version first, Version second) {
        if (first.getMajor() == second.getMajor() && first.getMinor() == second.getMinor()
                && first.getBuild() == second.getBuild() && first.getRevision() == second.getRevision()) {
            return false;
        } else if (first.getMajor() > second.getMajor()) {
            return true;
        } else if (first.getMajor() < second.getMajor()) {
            return false;
        } else if (first.getMinor() > second.getMinor()) {
            return true;
        } else if (first.getMinor() < second.getMinor()) {
            return false;
        } else if (first.getBuild() > second.getBuild()) {
            return true;
        } else if (first.getBuild() < second.getBuild()) {
            return false;
        } else if (first.getRevision() > second.getRevision()) {
            return true;
        } else if (first.getRevision() < second.getRevision()) {
            return false;
        }
        return false;
    }

    public int getMajor() {
        return Major;
    }

    public int getMinor() {
        return Minor;
    }

    public int getBuild() {
        return Build;
    }

    public int getRevision() {
        return Revision;
    }

    @Override
    public String toString() {
        return String.format("%d.%d.%d.%d", Major, Minor, Build, Revision);
    }
}

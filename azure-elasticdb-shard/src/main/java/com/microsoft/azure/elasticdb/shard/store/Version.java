package com.microsoft.azure.elasticdb.shard.store;

/* Copyright (c) Microsoft. All rights reserved.
Licensed under the MIT license. See LICENSE file in the project root for full license information.*/

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
   * Returns:
   * The major version number.
   */
  @XmlElement(name = "MajorVersion")
  private int major;
  /**
   * Summary:
   * Gets the value of the minor component of the version number for the current Version object.
   * Returns:
   * The minor version number.
   */
  @XmlElement(name = "MinorVersion")
  private int minor;
  /**
   * Summary:
   * Gets the value of the build component of the version number for the current Version object.
   * Returns:
   * The build number, or -1 if the build number is undefined.
   */
  private int build;
  /**
   * Summary:
   * Gets the value of the revision component of the version number for the current Version object.
   * Returns:
   * The revision number, or -1 if the revision number is undefined.
   */
  private int revision;

  /**
   * Summary:
   * Initializes a new instance of the Version class.
   */
  public Version() {
    this.major = 0;
    this.minor = 0;
    this.build = 0;
    this.revision = 0;
  }

  /**
   * Summary:
   * Initializes a new instance of the Version class using the specified string.
   * Parameters:
   * version:
   * A string containing the major, minor, build, and revision numbers, where each
   * number is delimited with a period character ('.').
   * TODO: Exceptions:
   * T:ArgumentException:
   * version has fewer than two components or more than four components.
   * T:ArgumentNullException:
   * version is null.
   * T:ArgumentOutOfRangeException:
   * A major, minor, build, or revision component is less than zero.
   * T:FormatException:
   * At least one component of version does not parse to an integer.
   * T:OverflowException:
   * At least one component of version represents a number greater than Int32.MaxValue.
   */
  public Version(String version) {
    String[] parts = version.split(".");
    this.major = parts.length > 0 && parts[0] != null ? Integer.parseInt(parts[0]) : 0;
    this.minor = parts.length > 1 && parts[1] != null ? Integer.parseInt(parts[1]) : 0;
    this.build = parts.length > 2 && parts[2] != null ? Integer.parseInt(parts[2]) : 0;
    this.revision = parts.length > 3 && parts[3] != null ? Integer.parseInt(parts[3]) : 0;
  }

  /**
   * Summary:
   * Initializes a new instance of the Version class using the specified major and minor values.
   * Parameters:
   * major:
   * The major version number.
   * minor:
   * The minor version number.
   * TODO: Exceptions:
   * T:ArgumentOutOfRangeException:
   * major or minor is less than zero.
   */
  public Version(int major, int minor) {
    this.major = major;
    this.minor = minor;
    this.build = 0;
    this.revision = 0;
  }

  /**
   * Summary: Initializes a new instance of the Version class using the specified major, minor, and
   * build values.  Parameters: major: The major version number.  minor: The minor version
   * number.  build: The build number.  TODO: Exceptions: T:ArgumentOutOfRangeException:
   * major, minor, or build is less than zero.
   */
  public Version(int major, int minor, int build) {
    this.major = major;
    this.minor = minor;
    this.build = build;
    this.revision = 0;
  }

  /**
   * Summary: Initializes a new instance of the Version class with the specified major, minor,
   * build, and revision numbers.  Parameters: major: The major version number.  minor: The
   * minor version number.  build: The build number.  revision: The revision number.  TODO:
   * Exceptions: T:ArgumentOutOfRangeException: major, minor, build, or revision is less than zero.
   */
  public Version(int major, int minor, int build, int revision) {
    this.major = major;
    this.minor = minor;
    this.build = build;
    this.revision = revision;
  }

  /**
   * Greater Than Operator Overloaded.
   *
   * @param first First or Left Parameter
   * @param second Second or Right Parameter
   * @return true if first is greater than second else false
   */
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
    return major;
  }

  public int getMinor() {
    return minor;
  }

  public int getBuild() {
    return build;
  }

  public int getRevision() {
    return revision;
  }

  @Override
  public String toString() {
    return String.format("%d.%d.%d.%d", major, minor, build, revision);
  }
}

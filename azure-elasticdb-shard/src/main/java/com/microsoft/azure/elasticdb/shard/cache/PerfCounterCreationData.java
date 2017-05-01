package com.microsoft.azure.elasticdb.shard.cache;

/* Copyright (c) Microsoft. All rights reserved.
Licensed under the MIT license. See LICENSE file in the project root for full license information.*/

/**
 * Structure holding performance counter creation information.
 */
public final class PerfCounterCreationData {

  private PerformanceCounterName counterName = PerformanceCounterName.values()[0];
  private PerformanceCounterType counterType = PerformanceCounterType.values()[0];
  private String counterDisplayName;
  private String counterHelpText;

  /**
   * Creates an Instance of PerfCounterCreationData.
   */
  public PerfCounterCreationData() {
  }

  /**
   * Creates an Instance of PerfCounterCreationData.
   *
   * @param name Performance Counter Name
   * @param type Performance Counter Type
   * @param displayName Display Name
   * @param helpText Help Text
   */
  public PerfCounterCreationData(PerformanceCounterName name, PerformanceCounterType type,
      String displayName, String helpText) {
    counterName = name;
    counterType = type;
    counterDisplayName = displayName;
    counterHelpText = helpText;
  }

  public PerformanceCounterName getCounterName() {
    return counterName;
  }

  public PerformanceCounterType getCounterType() {
    return counterType;
  }

  public String getCounterDisplayName() {
    return counterDisplayName;
  }

  public String getCounterHelpText() {
    return counterHelpText;
  }

  /**
   * Clone the current instance of PerfCounterCreationData.
   *
   * @return Cloned PerfCounterCreationData Instance
   */
  public PerfCounterCreationData clone() {
    PerfCounterCreationData varCopy = new PerfCounterCreationData();

    varCopy.counterName = this.counterName;
    varCopy.counterType = this.counterType;
    varCopy.counterDisplayName = this.counterDisplayName;
    varCopy.counterHelpText = this.counterHelpText;

    return varCopy;
  }
}
package com.microsoft.azure.elasticdb.shard.cache;

/* Copyright (c) Microsoft. All rights reserved.
Licensed under the MIT license. See LICENSE file in the project root for full license information.*/

import com.microsoft.azure.elasticdb.core.commons.helpers.ReferenceObjectHelper;
import com.microsoft.azure.elasticdb.shard.utils.PerformanceCounters;
import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Class represenging single instance of a all performance counters in shard management catagory.
 */
public class PerfCounterInstance implements AutoCloseable {

  public static final ArrayList<PerfCounterCreationData> counterList =
      new ArrayList<>(Arrays.asList(new PerfCounterCreationData[]{
          new PerfCounterCreationData(PerformanceCounterName.MappingsCount,
              PerformanceCounterType.NumberOfItems64, PerformanceCounters.MappingsCountDisplayName,
              PerformanceCounters.MappingsCountHelpText),
          new PerfCounterCreationData(PerformanceCounterName.MappingsAddOrUpdatePerSec,
              PerformanceCounterType.RateOfCountsPerSecond64,
              PerformanceCounters.MappingsAddOrUpdatePerSecDisplayName,
              PerformanceCounters.MappingsAddOrUpdatePerSecHelpText),
          new PerfCounterCreationData(PerformanceCounterName.MappingsRemovePerSec,
              PerformanceCounterType.RateOfCountsPerSecond64,
              PerformanceCounters.MappingsRemovePerSecDisplayName,
              PerformanceCounters.MappingsRemovePerSecHelpText),
          new PerfCounterCreationData(PerformanceCounterName.MappingsLookupSucceededPerSec,
              PerformanceCounterType.RateOfCountsPerSecond64,
              PerformanceCounters.MappingsLookupSucceededPerSecDisplayName,
              PerformanceCounters.MappingsLookupSucceededPerSecHelpText),
          new PerfCounterCreationData(PerformanceCounterName.MappingsLookupFailedPerSec,
              PerformanceCounterType.RateOfCountsPerSecond64,
              PerformanceCounters.MappingsLookupFailedPerSecDisplayName,
              PerformanceCounters.MappingsLookupFailedPerSecHelpText),
          new PerfCounterCreationData(PerformanceCounterName.DdrOperationsPerSec,
              PerformanceCounterType.RateOfCountsPerSecond64,
              PerformanceCounters.DdrOperationsPerSecDisplayName,
              PerformanceCounters.DdrOperationsPerSecHelpText)
      }));
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  private static Object lockObject = new Object();
  private HashMap<PerformanceCounterName, PerformanceCounterWrapper> counters;
  private boolean isInitialized;
  private String instanceName;

  /**
   * Initialize perf counter instance based on shard map name.
   */
  public PerfCounterInstance(String shardMapName) {
    isInitialized = false;

    //TODO
    //instanceName = String.join(String.valueOf(Process.GetCurrentProcess().Id), "-", shardMapName);

    try {
      // check if caller has permissions to create performance counters.
      if (!PerfCounterInstance.hasCreatePerformanceCounterPermissions()) {
        // Trace out warning and continue
        log.info("PerfCounter create User does not have permissions to create performance counters,"
            + "no performance data will be collected.");
      } else {
        // check if PerformanceCounterCategory exists

        if (!PerformanceCounterCategory.exists(
            PerformanceCounters.ShardManagementPerformanceCounterCategory)) {
          // We are not creating performance counter category here as per recommendation in
          // documentation, copying note from
          // https://msdn.microsoft.com/en-us/library/sb32hxtc(v=vs.110).aspx
          // It is strongly recommended that new performance counter categories be created during
          // the installation of the application, not during the execution of the application.
          // This allows time for the operating system to refresh its list of registered
          // performance counter categories. If the list has not been refreshed, the attempt to
          // use the category will fail.

          // Trace out warning and continue
          log.info("PerfCounter create Performance counter category {} does not exist, no"
                  + "performance data will be collected.",
              PerformanceCounters.ShardManagementPerformanceCounterCategory);
        } else {
          // Check if specific instance exists
          if (PerformanceCounterCategory.instanceExists(instanceName,
              PerformanceCounters.ShardManagementPerformanceCounterCategory)) {
            // As performance counters are created with Process lifetime and instance name is
            // unique (PID + shard map name), this should never happen. Trace out error and
            // silently continue.
            log.info("PerfCounter create Performance counter instance {} already exists, no"
                + "performance data will be collected.", instanceName);
          } else {
            // now initialize all counters for this instance
            counters = new HashMap<>();

            for (PerfCounterCreationData d : PerfCounterInstance.counterList) {
              counters.put(d.getCounterName(), new PerformanceCounterWrapper(
                  PerformanceCounters.ShardManagementPerformanceCounterCategory, instanceName,
                  d.getCounterDisplayName()));
            }

            // check that atleast one performance counter was created, so that we can remove
            // instance as part of Dispose()
            //TODO: isInitialized = counters.Any(c -> c.Value.isValid = true);
          }
        }
      }
    } catch (RuntimeException e) {
      // Note: If any of the initialization calls throws, log the exception and silently continue.
      // No perf data will be collected in this case.
      // All other non-static code paths access PerformanceCounter and PerformanceCounterCategory
      // objects only if isInitialized is set to true.

      log.info("PerfCounter PerfCounterInstance..ctor Exception caught while creating performance"
              + "counter instance, no performance data will be collected. Exception:{}",
          e.toString());
    }
  }

  /**
   * Static method to recreate Shard Management performance counter catagory with given counter
   * list.
   */
  public static void createPerformanceCategoryAndCounters() {
    // Creation of performance counters need Administrator privilege
    if (hasCreatePerformanceCategoryPermissions()) {
      // Delete performance counter category, if exists.
      //TODO:
      /*if (PerformanceCounterCategory
          .Exists(PerformanceCounters.ShardManagementPerformanceCounterCategory)) {
        PerformanceCounterCategory
            .Delete(PerformanceCounters.ShardManagementPerformanceCounterCategory);
      }

      CounterCreationDataCollection smmCounters = new CounterCreationDataCollection();

      for (PerfCounterCreationData d : PerfCounterInstance.counterList) {
        smmCounters.Add(new CounterCreationData(d.getCounterDisplayName(), d.getCounterHelpText(),
            d.getCounterType()));
      }

      PerformanceCounterCategory
          .Create(PerformanceCounters.ShardManagementPerformanceCounterCategory,
              PerformanceCounters.ShardManagementPerformanceCounterCategoryHelp,
              PerformanceCounterCategoryType.MultiInstance, smmCounters);*/
    } else {
      // Trace out warning and continue
      log.warn("User does not have permissions to create performance counter category");
    }
  }

  /**
   * Check if caller has permissions to create performance counter catagory.
   *
   * @return If caller can create performance counter catagory
   */
  public static boolean hasCreatePerformanceCategoryPermissions() {
    // PerformanceCounterCategory creation requires user to be part of Administrators group.
    //TODO:
    /*WindowsPrincipal wp = new WindowsPrincipal(WindowsIdentity.GetCurrent());
    return wp.IsInRole(WindowsBuiltInRole.Administrator);*/
    return false;
  }

  /**
   * Check if caller has permissions to create performance counter instance
   *
   * @return If caller can create performance counter instance.
   */
  public static boolean hasCreatePerformanceCounterPermissions() {
    // PerformanceCounter creation requires user to be part of Administrators
    // or 'Performance Monitor Users' local group.
    //TODO:
    /*WindowsPrincipal wp = new WindowsPrincipal(WindowsIdentity.GetCurrent());
    return wp.IsInRole(WindowsBuiltInRole.Administrator) || wp
        .IsInRole(PerformanceCounters.PerformanceMonitorUsersGroupName);*/
    return false;
  }

  /**
   * Try to increment specified performance counter by 1 for current instance.
   *
   * @param counterName Counter to increment.
   */
  public final void incrementCounter(PerformanceCounterName counterName) {
    if (isInitialized) {
      PerformanceCounterWrapper pc = null;
      ReferenceObjectHelper<PerformanceCounterWrapper> refPc =
          new ReferenceObjectHelper<>(pc);
      //TODO:
      /*if (counters.TryGetValue(counterName, refPc)) {
        pc = refPc.argValue;
        pc.Increment();
      } else {
        pc = refPc.argValue;
      }*/
    }
  }

  /**
   * Try to update performance counter with speficied value.
   *
   * @param counterName Counter to update.
   * @param value New value.
   */
  public final void setCounter(PerformanceCounterName counterName, long value) {
    if (isInitialized) {
      PerformanceCounterWrapper pc = null;
      ReferenceObjectHelper<PerformanceCounterWrapper> refPc =
          new ReferenceObjectHelper<>(pc);
      //TODO:
      /*if (counters.TryGetValue(counterName, refPc)) {
        pc = refPc.argValue;
        pc.SetRawValue(value);
      } else {
        pc = refPc.argValue;
      }*/
    }
  }

  /**
   * Dispose performance counter instance.
   */
  public final void close() throws java.io.IOException {
    if (isInitialized) {
      synchronized (lockObject) {
        // If performance counter instance exists, remove it here.
        if (isInitialized) {
          // We can assume here that performance counter catagory, instance and first counter in
          // the cointerList exist as isInitialized is set to true.
          //TODO:
          /*try (PerformanceCounter pcRemove = new PerformanceCounter()) {
            pcRemove.CategoryName = PerformanceCounters.ShardManagementPerformanceCounterCategory;
            pcRemove.CounterName = counterList.get(0).CounterDisplayName;
            pcRemove.InstanceName = instanceName;
            pcRemove.InstanceLifetime = PerformanceCounterInstanceLifetime.Process;
            pcRemove.ReadOnly = false;
            // Removing instance using a single counter removes all counters for that instance.
            pcRemove.RemoveInstance();
          }*/
        }
        isInitialized = false;
      }
    }
  }

  private static class PerformanceCounterCategory {

    public static boolean exists(String shardManagementPerformanceCounterCategory) {
      return false;
    }

    public static boolean instanceExists(String instanceName,
        String shardManagementPerformanceCounterCategory) {
      return false;
    }
  }
}
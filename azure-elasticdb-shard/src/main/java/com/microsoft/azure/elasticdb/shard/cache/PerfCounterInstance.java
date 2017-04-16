package com.microsoft.azure.elasticdb.shard.cache;

// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

import com.microsoft.azure.elasticdb.core.commons.helpers.ReferenceObjectHelper;
import com.microsoft.azure.elasticdb.shard.utils.PerformanceCounters;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;

/**
 * Class represenging single instance of a all performance counters in shard management catagory
 */
public class PerfCounterInstance implements AutoCloseable {

    public static final ArrayList<PerfCounterCreationData> counterList = new ArrayList<PerfCounterCreationData>(Arrays.asList(new PerfCounterCreationData[]{
            new PerfCounterCreationData(PerformanceCounterName.MappingsCount, PerformanceCounterType.NumberOfItems64, PerformanceCounters.MappingsCountDisplayName, PerformanceCounters.MappingsCountHelpText),
            new PerfCounterCreationData(PerformanceCounterName.MappingsAddOrUpdatePerSec, PerformanceCounterType.RateOfCountsPerSecond64, PerformanceCounters.MappingsAddOrUpdatePerSecDisplayName, PerformanceCounters.MappingsAddOrUpdatePerSecHelpText),
            new PerfCounterCreationData(PerformanceCounterName.MappingsRemovePerSec, PerformanceCounterType.RateOfCountsPerSecond64, PerformanceCounters.MappingsRemovePerSecDisplayName, PerformanceCounters.MappingsRemovePerSecHelpText),
            new PerfCounterCreationData(PerformanceCounterName.MappingsLookupSucceededPerSec, PerformanceCounterType.RateOfCountsPerSecond64, PerformanceCounters.MappingsLookupSucceededPerSecDisplayName, PerformanceCounters.MappingsLookupSucceededPerSecHelpText),
            new PerfCounterCreationData(PerformanceCounterName.MappingsLookupFailedPerSec, PerformanceCounterType.RateOfCountsPerSecond64, PerformanceCounters.MappingsLookupFailedPerSecDisplayName, PerformanceCounters.MappingsLookupFailedPerSecHelpText),
            new PerfCounterCreationData(PerformanceCounterName.DdrOperationsPerSec, PerformanceCounterType.RateOfCountsPerSecond64, PerformanceCounters.DdrOperationsPerSecDisplayName, PerformanceCounters.DdrOperationsPerSecHelpText)
    }));
    private final static Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
    private static Object _lockObject = new Object();
    private HashMap<PerformanceCounterName, PerformanceCounterWrapper> _counters;
    private boolean _initialized;
    private String _instanceName;

    /**
     * Initialize perf counter instance based on shard map name
     *
     * @param shardMapName
     */
    public PerfCounterInstance(String shardMapName) {
        _initialized = false;

        //TODO:
        /*_instanceName = String.join(String.valueOf(Process.GetCurrentProcess().Id), "-", shardMapName);

		try {
			// check if caller has permissions to create performance counters.
			if (!PerfCounterInstance.HasCreatePerformanceCounterPermissions()) {
				// Trace out warning and continue
				getTracer().TraceWarning(TraceSourceConstants.ComponentNames.PerfCounter, "create", "User does not have permissions to create performance counters, no performance data will be collected.");
			} else {
				// check if PerformanceCounterCategory exists

				if (!PerformanceCounterCategory.Exists(PerformanceCounters.ShardManagementPerformanceCounterCategory)) {
					// We are not creating performance counter category here as per recommendation in documentation, copying note from
					// https://msdn.microsoft.com/en-us/library/sb32hxtc(v=vs.110).aspx
					// It is strongly recommended that new performance counter categories be created
					// during the installation of the application, not during the execution of the application.
					// This allows time for the operating system to refresh its list of registered performance counter categories.
					// If the list has not been refreshed, the attempt to use the category will fail.

					// Trace out warning and continue
					getTracer().TraceWarning(TraceSourceConstants.ComponentNames.PerfCounter, "create", "Performance counter category {0} does not exist, no performance data will be collected.", PerformanceCounters.ShardManagementPerformanceCounterCategory);
				} else {
					// Check if specific instance exists
					if (PerformanceCounterCategory.InstanceExists(_instanceName, PerformanceCounters.ShardManagementPerformanceCounterCategory)) {
						// As performance counters are created with Process lifetime and instance name is unique (PID + shard map name),
						// this should never happen. Trace out error and silently continue.
						getTracer().TraceWarning(TraceSourceConstants.ComponentNames.PerfCounter, "create", "Performance counter instance {0} already exists, no performance data will be collected.", _instanceName);
					} else {
						// now initialize all counters for this instance
						_counters = new HashMap<PerformanceCounterName, PerformanceCounterWrapper>();

						for (PerfCounterCreationData d : PerfCounterInstance.counterList) {
							_counters.put(d.getCounterName(), new PerformanceCounterWrapper(PerformanceCounters.ShardManagementPerformanceCounterCategory, _instanceName, d.getCounterDisplayName()));
						}

						// check that atleast one performance counter was created, so that we can remove instance as part of Dispose()
						_initialized = _counters.Any(c -> c.Value._isValid = true);
					}
				}
			}
		} catch (RuntimeException e) {
			// Note: If any of the initialization calls throws, log the exception and silently continue.
			// No perf data will be collected in this case.
			// All other non-static code paths access PerformanceCounter and PerformanceCounterCategory
			// objects only if _initialized is set to true.

			getTracer().TraceWarning(TraceSourceConstants.ComponentNames.PerfCounter, "PerfCounterInstance..ctor", "Exception caught while creating performance counter instance, no performance data will be collected. Exception:{}", e.toString());
		}*/
    }

    /**
     * Static method to recreate Shard Management performance counter catagory with given counter list.
     */
    public static void CreatePerformanceCategoryAndCounters() {
        // Creation of performance counters need Administrator privilege
        if (HasCreatePerformanceCategoryPermissions()) {
            // Delete performance counter category, if exists.
            //TODO:
            /*if (PerformanceCounterCategory.Exists(PerformanceCounters.ShardManagementPerformanceCounterCategory)) {
                PerformanceCounterCategory.Delete(PerformanceCounters.ShardManagementPerformanceCounterCategory);
            }

            CounterCreationDataCollection smmCounters = new CounterCreationDataCollection();

            for (PerfCounterCreationData d : PerfCounterInstance.counterList) {
                smmCounters.Add(new CounterCreationData(d.getCounterDisplayName(), d.getCounterHelpText(), d.getCounterType()));
            }

            PerformanceCounterCategory.Create(PerformanceCounters.ShardManagementPerformanceCounterCategory, PerformanceCounters.ShardManagementPerformanceCounterCategoryHelp, PerformanceCounterCategoryType.MultiInstance, smmCounters);*/
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
    public static boolean HasCreatePerformanceCategoryPermissions() {
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
    public static boolean HasCreatePerformanceCounterPermissions() {
        // PerformanceCounter creation requires user to be part of Administrators or 'Performance Monitor Users' local group.
        //TODO:
        /*WindowsPrincipal wp = new WindowsPrincipal(WindowsIdentity.GetCurrent());
        return wp.IsInRole(WindowsBuiltInRole.Administrator) || wp.IsInRole(PerformanceCounters.PerformanceMonitorUsersGroupName);*/
        return false;
    }

    /**
     * Try to increment specified performance counter by 1 for current instance.
     *
     * @param counterName Counter to increment.
     */
    public final void IncrementCounter(PerformanceCounterName counterName) {
        if (_initialized) {
            PerformanceCounterWrapper pc = null;
            ReferenceObjectHelper<PerformanceCounterWrapper> tempRef_pc = new ReferenceObjectHelper<PerformanceCounterWrapper>(pc);
            //TODO:
            /*if (_counters.TryGetValue(counterName, tempRef_pc)) {
            pc = tempRef_pc.argValue;
				pc.Increment();
			} else {
			    pc = tempRef_pc.argValue;
            }*/
        }
    }

    /**
     * Try to update performance counter with speficied value.
     *
     * @param counterName Counter to update.
     * @param value       New value.
     */
    public final void SetCounter(PerformanceCounterName counterName, long value) {
        if (_initialized) {
            PerformanceCounterWrapper pc = null;
            ReferenceObjectHelper<PerformanceCounterWrapper> tempRef_pc = new ReferenceObjectHelper<PerformanceCounterWrapper>(pc);
            //TODO:
            /*if (_counters.TryGetValue(counterName, tempRef_pc)) {
                pc = tempRef_pc.argValue;
                pc.SetRawValue(value);
            } else {
                pc = tempRef_pc.argValue;
            }*/
        }
    }

    /**
     * Dispose performance counter instance
     */
    public final void close() throws java.io.IOException {
        if (_initialized) {
            synchronized (_lockObject) {
                // If performance counter instance exists, remove it here.
                if (_initialized) {
                    // We can assume here that performance counter catagory, instance and first counter in the cointerList exist as _initialized is set to true.
                    //TODO:
                    /*try (PerformanceCounter pcRemove = new PerformanceCounter()) {
                        pcRemove.CategoryName = PerformanceCounters.ShardManagementPerformanceCounterCategory;
                        pcRemove.CounterName = counterList.get(0).CounterDisplayName;
                        pcRemove.InstanceName = _instanceName;
                        pcRemove.InstanceLifetime = PerformanceCounterInstanceLifetime.Process;
                        pcRemove.ReadOnly = false;
                        // Removing instance using a single counter removes all counters for that instance.
                        pcRemove.RemoveInstance();
                    }*/
                }
                _initialized = false;
            }
        }
    }
}
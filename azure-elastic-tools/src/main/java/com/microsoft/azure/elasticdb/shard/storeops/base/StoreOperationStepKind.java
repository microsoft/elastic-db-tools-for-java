package com.microsoft.azure.elasticdb.shard.storeops.base;

import javax.xml.bind.annotation.XmlEnumValue;

/**
 * Step kind for for Bulk Operations.
 */
public enum StoreOperationStepKind {
    /**
     * Remove operation.
     */
    @XmlEnumValue("1")
    Remove(1),

    /**
     * Update operation.
     */
    @XmlEnumValue("2")
    Update(2),

    /**
     * Add operation.
     */
    @XmlEnumValue("3")
    Add(3);

    public static final int SIZE = java.lang.Integer.SIZE;
    private static java.util.HashMap<Integer, StoreOperationStepKind> mappings;
    private int intValue;

    StoreOperationStepKind(int value) {
        intValue = value;
        getMappings().put(value, this);
    }

    private static java.util.HashMap<Integer, StoreOperationStepKind> getMappings() {
        if (mappings == null) {
            synchronized (StoreOperationStepKind.class) {
                if (mappings == null) {
                    mappings = new java.util.HashMap<>();
                }
            }
        }
        return mappings;
    }

    public static StoreOperationStepKind forValue(int value) {
        return getMappings().get(value);
    }

    public int getValue() {
        return intValue;
    }
}

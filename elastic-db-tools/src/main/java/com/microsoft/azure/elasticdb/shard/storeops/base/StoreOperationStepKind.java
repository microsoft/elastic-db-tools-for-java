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
    Remove,//(1),

    /**
     * Update operation.
     */
    @XmlEnumValue("2")
    Update,//(2),

    /**
     * Add operation.
     */
    @XmlEnumValue("3")
    Add;//(3);

//    private int intValue;
//
//    StoreOperationStepKind(int value) {
//        intValue = value;
//    }
//
//    public int getValue() {
//        return intValue;
//    }
}

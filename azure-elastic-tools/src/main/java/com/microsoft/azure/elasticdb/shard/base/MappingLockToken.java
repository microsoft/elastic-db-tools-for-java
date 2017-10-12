package com.microsoft.azure.elasticdb.shard.base;

/*
 * Elastic database tools for Azure SQL Database.
 * 
 * Copyright(c) Microsoft Corporation All rights reserved.
 * 
 * This program is made available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

import java.util.UUID;

/**
 * Public type that represents the owner of a lock held on a mapping. This class is immutable.
 */
public final class MappingLockToken {

    /**
     * Token representing the default state where the mapping isn't locked.
     */
    public static final MappingLockToken NoLock = new MappingLockToken(new UUID(0L, 0L));

    /**
     * Token that can be used to force an unlock on any locked mapping.
     */
    public static final MappingLockToken ForceUnlock = new MappingLockToken(UUID.fromString("FFFFFFFF-FFFF-FFFF-FFFF-FFFFFFFFFFFF"));

    private UUID lockOwnerId;

    /**
     * Instantiates an instance of <see cref="MappingLockToken"/> with the given lock owner id.
     *
     * @param lockOwnerId
     *            The lock owner id
     */
    public MappingLockToken(UUID lockOwnerId) {
        this.setLockOwnerId(lockOwnerId);
    }

    /**
     * Creates an instance of <see cref="MappingLockToken"/>.
     *
     * @return An instance of <see cref="MappingLockToken"/>
     */
    public static MappingLockToken create() {
        return new MappingLockToken(UUID.randomUUID());
    }

    /**
     * Equality operator.
     *
     * @param leftMappingLockToken
     *            An instance of <see cref="MappingLockToken"/>
     * @param rightMappingLockToken
     *            An instance of <see cref="MappingLockToken"/>
     * @return True if both belong to the same lock owner
     */
    public static boolean opEquality(MappingLockToken leftMappingLockToken,
            MappingLockToken rightMappingLockToken) {
        return leftMappingLockToken.equals(rightMappingLockToken);
    }

    /**
     * Inequality operator.
     *
     * @param leftMappingLockToken
     *            An instance of <see cref="MappingLockToken"/>
     * @param rightMappingLockToken
     *            An instance of <see cref="MappingLockToken"/>
     * @return True if both belong to the same lock owner
     */
    public static boolean opInequality(MappingLockToken leftMappingLockToken,
            MappingLockToken rightMappingLockToken) {
        return leftMappingLockToken.equals(rightMappingLockToken);
    }

    public UUID getLockOwnerId() {
        return lockOwnerId;
    }

    public void setLockOwnerId(UUID value) {
        lockOwnerId = value;
    }

    /**
     * Determines whether the specified object is equal to the current object.
     *
     * @param obj
     *            The object to compare with the current object.
     * @return True if the specified object is equal to the current object; otherwise, false.
     */
    @Override
    public boolean equals(Object obj) {
        return this.equals((MappingLockToken) ((obj instanceof MappingLockToken) ? obj : null));
    }

    /**
     * Compares two instances of <see cref="MappingLockToken"/> to see if they have the same owner.
     *
     * @return True if they both belong to the same lock owner
     */
    public boolean equals(MappingLockToken other) {
        return other != null && this.getLockOwnerId().equals(other.getLockOwnerId());
    }

    /**
     * Calculates the hash code for this instance.
     *
     * @return Hash code for the object.
     */
    @Override
    public int hashCode() {
        return this.getLockOwnerId().hashCode();
    }
}
package com.microsoft.azure.elasticdb.shard.recovery;

/*
 * Elastic database tools for Azure SQL Database.
 * 
 * Copyright(c) Microsoft Corporation All rights reserved.
 * 
 * This program is made available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

import java.util.UUID;

/**
 * Recovery token generated and used by methods of the <see cref="RecoveryManager"/> to perform conflict detection and resolution for shard maps.
 */
public final class RecoveryToken {

    /**
     * Internal Guid for this token.
     */
    private UUID id;

    /**
     * Parameter less constructor to generate a new unique token for shard map conflict detection and resolution.
     */
    public RecoveryToken() {
        this.setId(UUID.randomUUID());
    }

    private UUID getId() {
        return id;
    }

    private void setId(UUID value) {
        id = value;
    }

    /**
     * Converts the object to its string representation.
     *
     * @return String representation of the object.
     */
    @Override
    public String toString() {
        return this.getId().toString();
    }

    /**
     * Calculates the hash code for this instance.
     *
     * @return Hash code for the object.
     */
    @Override
    public int hashCode() {
        return this.getId().hashCode();
    }

    /**
     * Determines whether the specified Object is equal to the current Object.
     *
     * @param obj
     *            The object to compare with the current object.
     * @return True if the specified object is equal to the current object; otherwise, false.
     */
    @Override
    public boolean equals(Object obj) {
        RecoveryToken other = (RecoveryToken) ((obj instanceof RecoveryToken) ? obj : null);

        return other != null && this.equals(other);

    }

    /**
     * Performs equality comparison with another given RecoveryToken.
     *
     * @param other
     *            RecoveryToken to compare with.
     * @return True if same locations, false otherwise.
     */
    public boolean equals(RecoveryToken other) {
        return other != null && this.getId().equals(other.getId());

    }
}
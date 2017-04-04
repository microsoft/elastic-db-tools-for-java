package com.microsoft.azure.elasticdb.shard.base;

// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

import com.microsoft.azure.elasticdb.shard.utils.ExceptionUtils;

/**
 * Base class for updates to mappings from shardlets to shards.
 * <p>
 * <typeparam name="TStatus">Type of status field.</typeparam>
 */
public abstract class BaseMappingUpdate<TStatus> implements IMappingUpdate<TStatus> {
    /**
     * Records the modified properties for update.
     */
    private MappingUpdatedProperties _updatedProperties;

    /**
     * Holder for update to status property.
     */
    private TStatus _status;

    /**
     * Holder for update to shard property.
     */
    private Shard _shard;

    /**
     * Gets or sets the Status property.
     */
    public final TStatus getStatus() {
        return _status;
    }

    public final void setStatus(TStatus value) {
        _status = value;
        //_updatedProperties |= MappingUpdatedProperties.Status;
    }

    /**
     * Gets or sets the Shard property.
     */
    public final Shard getShard() {
        return _shard.Clone();
    }

    public final void setShard(Shard value) {
        ExceptionUtils.DisallowNullArgument(value, "value");
        _shard = value.Clone();
        //_updatedProperties |= MappingUpdatedProperties.Shard;
    }

    /**
     Status property.
     */
    /*private TStatus getStatus() {
        return this.getStatus();
	}*/

    /**
     Shard property.
     */
    /*private Shard getShard() {
        return this.getShard();
	}*/

    /**
     * Checks if any property is set in the given bitmap.
     *
     * @param properties Properties bitmap.
     * @return True if any of the properties is set, false otherwise.
     */
    public final boolean IsAnyPropertySet(MappingUpdatedProperties properties) {
        return false;// TODO: (_updatedProperties & properties) != 0;
    }

    /**
     * Checks if the mapping is being taken offline.
     *
     * @param originalStatus Original status.
     * @return True of the update will take the mapping offline.
     */
    public final boolean IsMappingBeingTakenOffline(TStatus originalStatus) {
        // TODO:
        //if ((_updatedProperties & MappingUpdatedProperties.Status) != MappingUpdatedProperties.Status) {
        return false;
        /*} else {
            return this.IsBeingTakenOffline(originalStatus, this.getStatus());
		}*/
    }

    /**
     * Detects if the current mapping is being taken offline.
     *
     * @param originalStatus Original status.
     * @param updatedStatus  Updated status.
     * @return Detects in the derived types if the mapping is being taken offline.
     */
    protected abstract boolean IsBeingTakenOffline(TStatus originalStatus, TStatus updatedStatus);
}

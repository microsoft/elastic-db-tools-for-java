package com.microsoft.azure.elasticdb.shard.base;

// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

/**
 * Represents updates to a mapping between a <see cref="Range{TKey}"/> of values and the <see
 * cref="Shard"/> that stores its data. Also see <see cref="RangeMapping{TKey}"/>.
 */
public final class RangeMappingUpdate extends BaseMappingUpdate<MappingStatus> {

  /**
   * Instantiates a new range mapping update object.
   */
  public RangeMappingUpdate() {
    super();
  }

  /**
   * Detects if the current mapping is being taken offline.
   *
   * @param originalStatus Original status.
   * @param updatedStatus Updated status.
   * @return Detects in the derived types if the mapping is being taken offline.
   */
  @Override
  protected boolean IsBeingTakenOffline(MappingStatus originalStatus, MappingStatus updatedStatus) {
    return originalStatus == MappingStatus.Online && updatedStatus == MappingStatus.Offline;
  }
}
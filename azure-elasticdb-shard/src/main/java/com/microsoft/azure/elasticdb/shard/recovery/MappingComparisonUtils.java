package com.microsoft.azure.elasticdb.shard.recovery;

// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

import com.microsoft.azure.elasticdb.core.commons.helpers.ReferenceObjectHelper;
import com.microsoft.azure.elasticdb.shard.base.ShardKey;
import com.microsoft.azure.elasticdb.shard.base.ShardKeyType;
import com.microsoft.azure.elasticdb.shard.base.ShardRange;
import com.microsoft.azure.elasticdb.shard.store.StoreMapping;
import com.microsoft.azure.elasticdb.shard.store.StoreShardMap;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Contains utility methods for performing comparisons among collections of
 * mappings of either list or range shard maps.
 */
public final class MappingComparisonUtils {
  ///#region Static internal methods

  /**
   * Helper function that produces a list of MappingComparisonResults from union of range boundaries
   * in the gsmMappings and lsmMappings.
   *
   * @param ssm StoreShardmap to be referenced in produced MappingComparisonResults
   * @param gsmMappings List of mappings from the GSM.
   * @param lsmMappings List of mappings from the LSM.
   * @return List of mappingcomparisonresults: one for each range arising from the union of
   * boundaries in gsmMappings and lsmMappings.
   */
  public static ArrayList<MappingComparisonResult> CompareRangeMappings(StoreShardMap ssm,
      List<StoreMapping> gsmMappings, List<StoreMapping> lsmMappings) {
    // Detect if these are point mappings and call the ComparePointMappings function below.

    ArrayList<MappingComparisonResult> result = new ArrayList<MappingComparisonResult>();

    // Identify the type of keys.
    ShardKeyType keyType = ssm.getKeyType();

        /*try (Iterator<StoreMapping> gsmMappingIterator = gsmMappings.iterator()) {
            try (Iterator<StoreMapping> lsmMappingIterator = lsmMappings.iterator()) {
                StoreMapping gsmMappingCurrent = null;
                ShardRange gsmRangeCurrent = null;
                ShardKey gsmMinKeyCurrent = null;

                StoreMapping lsmMappingCurrent = null;
                ShardRange lsmRangeCurrent = null;
                ShardKey lsmMinKeyCurrent = null;

                ReferenceObjectHelper<StoreMapping> tempRef_gsmMappingCurrent = new ReferenceObjectHelper<StoreMapping>(gsmMappingCurrent);
                ReferenceObjectHelper<ShardRange> tempRef_gsmRangeCurrent = new ReferenceObjectHelper<ShardRange>(gsmRangeCurrent);
                ReferenceObjectHelper<ShardKey> tempRef_gsmMinKeyCurrent = new ReferenceObjectHelper<ShardKey>(gsmMinKeyCurrent);
                MoveToNextMapping(gsmMappingIterator, keyType, tempRef_gsmMappingCurrent, tempRef_gsmRangeCurrent, tempRef_gsmMinKeyCurrent);
                gsmMinKeyCurrent = tempRef_gsmMinKeyCurrent.argValue;
                gsmRangeCurrent = tempRef_gsmRangeCurrent.argValue;
                gsmMappingCurrent = tempRef_gsmMappingCurrent.argValue;

                ReferenceObjectHelper<StoreMapping> tempRef_lsmMappingCurrent = new ReferenceObjectHelper<StoreMapping>(lsmMappingCurrent);
                ReferenceObjectHelper<ShardRange> tempRef_lsmRangeCurrent = new ReferenceObjectHelper<ShardRange>(lsmRangeCurrent);
                ReferenceObjectHelper<ShardKey> tempRef_lsmMinKeyCurrent = new ReferenceObjectHelper<ShardKey>(lsmMinKeyCurrent);
                MoveToNextMapping(lsmMappingIterator, keyType, tempRef_lsmMappingCurrent, tempRef_lsmRangeCurrent, tempRef_lsmMinKeyCurrent);
                lsmMinKeyCurrent = tempRef_lsmMinKeyCurrent.argValue;
                lsmRangeCurrent = tempRef_lsmRangeCurrent.argValue;
                lsmMappingCurrent = tempRef_lsmMappingCurrent.argValue;

                while (gsmMinKeyCurrent != null) {
                    // If there is something in LSM, consider the following 6 possibilities.
                    if (lsmMinKeyCurrent != null) {
                        if (lsmMinKeyCurrent <= gsmMinKeyCurrent) {
                            // Case 1. LSM starts to the left of or exactly at GSM.

                            if (lsmRangeCurrent.getHigh() <= gsmMinKeyCurrent) {
                                // Case 1.1: LSM is entirely to the left of Left.

                                // Add the LSM only entry.
                                result.add(new MappingComparisonResult(ssm, new ShardRange(lsmMinKeyCurrent, lsmRangeCurrent.getHigh()), MappingLocation.MappingInShardOnly, null, lsmMappingCurrent));

                                // LSM range exhausted for current iteration.
                                ReferenceObjectHelper<StoreMapping> tempRef_lsmMappingCurrent2 = new ReferenceObjectHelper<StoreMapping>(lsmMappingCurrent);
                                ReferenceObjectHelper<ShardRange> tempRef_lsmRangeCurrent2 = new ReferenceObjectHelper<ShardRange>(lsmRangeCurrent);
                                ReferenceObjectHelper<ShardKey> tempRef_lsmMinKeyCurrent2 = new ReferenceObjectHelper<ShardKey>(lsmMinKeyCurrent);
                                MoveToNextMapping(lsmMappingIterator, keyType, tempRef_lsmMappingCurrent2, tempRef_lsmRangeCurrent2, tempRef_lsmMinKeyCurrent2);
                                lsmMinKeyCurrent = tempRef_lsmMinKeyCurrent2.argValue;
                                lsmRangeCurrent = tempRef_lsmRangeCurrent2.argValue;
                                lsmMappingCurrent = tempRef_lsmMappingCurrent2.argValue;
                            } else {
                                if (lsmRangeCurrent.getHigh() <= gsmRangeCurrent.getHigh()) {
                                    // Case 1.2: LSM overlaps with GSM, with extra values to the left and finishing before GSM.
                                    if (lsmMinKeyCurrent != gsmMinKeyCurrent) {
                                        // Add the LSM only entry.
                                        result.add(new MappingComparisonResult(ssm, new ShardRange(lsmMinKeyCurrent, gsmMinKeyCurrent), MappingLocation.MappingInShardOnly, null, lsmMappingCurrent));
                                    }

                                    // Add common entry.
                                    result.add(new MappingComparisonResult(ssm, new ShardRange(gsmMinKeyCurrent, lsmRangeCurrent.getHigh()), MappingLocation.MappingInShardMapAndShard, gsmMappingCurrent, lsmMappingCurrent));

                                    gsmMinKeyCurrent = lsmRangeCurrent.getHigh();

                                    // LSM range exhausted for current iteration.
                                    ReferenceObjectHelper<StoreMapping> tempRef_lsmMappingCurrent3 = new ReferenceObjectHelper<StoreMapping>(lsmMappingCurrent);
                                    ReferenceObjectHelper<ShardRange> tempRef_lsmRangeCurrent3 = new ReferenceObjectHelper<ShardRange>(lsmRangeCurrent);
                                    ReferenceObjectHelper<ShardKey> tempRef_lsmMinKeyCurrent3 = new ReferenceObjectHelper<ShardKey>(lsmMinKeyCurrent);
                                    MoveToNextMapping(lsmMappingIterator, keyType, tempRef_lsmMappingCurrent3, tempRef_lsmRangeCurrent3, tempRef_lsmMinKeyCurrent3);
                                    lsmMinKeyCurrent = tempRef_lsmMinKeyCurrent3.argValue;
                                    lsmRangeCurrent = tempRef_lsmRangeCurrent3.argValue;
                                    lsmMappingCurrent = tempRef_lsmMappingCurrent3.argValue;

                                    // Detect if GSM range exhausted for current iteration.
                                    if (gsmMinKeyCurrent == gsmRangeCurrent.getHigh()) {
                                        ReferenceObjectHelper<StoreMapping> tempRef_gsmMappingCurrent2 = new ReferenceObjectHelper<StoreMapping>(gsmMappingCurrent);
                                        ReferenceObjectHelper<ShardRange> tempRef_gsmRangeCurrent2 = new ReferenceObjectHelper<ShardRange>(gsmRangeCurrent);
                                        ReferenceObjectHelper<ShardKey> tempRef_gsmMinKeyCurrent2 = new ReferenceObjectHelper<ShardKey>(gsmMinKeyCurrent);
                                        MoveToNextMapping(gsmMappingIterator, keyType, tempRef_gsmMappingCurrent2, tempRef_gsmRangeCurrent2, tempRef_gsmMinKeyCurrent2);
                                        gsmMinKeyCurrent = tempRef_gsmMinKeyCurrent2.argValue;
                                        gsmRangeCurrent = tempRef_gsmRangeCurrent2.argValue;
                                        gsmMappingCurrent = tempRef_gsmMappingCurrent2.argValue;
                                    }
                                } else { // lsmRangeCurrent.getHigh() > gsmRangeCurrent.getHigh()
                                    // Case 1.3: LSM encompasses GSM.

                                    // Add the LSM only entry.
                                    if (lsmMinKeyCurrent != gsmMinKeyCurrent) {
                                        result.add(new MappingComparisonResult(ssm, new ShardRange(lsmMinKeyCurrent, gsmMinKeyCurrent), MappingLocation.MappingInShardOnly, null, lsmMappingCurrent));
                                    }

                                    // Add common entry.
                                    result.add(new MappingComparisonResult(ssm, new ShardRange(gsmMinKeyCurrent, gsmRangeCurrent.getHigh()), MappingLocation.MappingInShardMapAndShard, gsmMappingCurrent, lsmMappingCurrent));

                                    lsmMinKeyCurrent = gsmRangeCurrent.getHigh();

                                    // GSM range exhausted for current iteration.
                                    ReferenceObjectHelper<StoreMapping> tempRef_gsmMappingCurrent3 = new ReferenceObjectHelper<StoreMapping>(gsmMappingCurrent);
                                    ReferenceObjectHelper<ShardRange> tempRef_gsmRangeCurrent3 = new ReferenceObjectHelper<ShardRange>(gsmRangeCurrent);
                                    ReferenceObjectHelper<ShardKey> tempRef_gsmMinKeyCurrent3 = new ReferenceObjectHelper<ShardKey>(gsmMinKeyCurrent);
                                    MoveToNextMapping(gsmMappingIterator, keyType, tempRef_gsmMappingCurrent3, tempRef_gsmRangeCurrent3, tempRef_gsmMinKeyCurrent3);
                                    gsmMinKeyCurrent = tempRef_gsmMinKeyCurrent3.argValue;
                                    gsmRangeCurrent = tempRef_gsmRangeCurrent3.argValue;
                                    gsmMappingCurrent = tempRef_gsmMappingCurrent3.argValue;
                                }
                            }
                        } else {
                            // Case 2. LSM starts to the right of GSM.

                            if (lsmRangeCurrent.getHigh() <= gsmRangeCurrent.getHigh()) {
                                // Case 2.1: GSM encompasses LSM.
                                //Debug.Assert(lsmMinKeyCurrent != gsmMinKeyCurrent, "Must have been handled by Case 1.3");

                                // Add the GSM only entry.
                                result.add(new MappingComparisonResult(ssm, new ShardRange(gsmMinKeyCurrent, lsmMinKeyCurrent), MappingLocation.MappingInShardMapOnly, gsmMappingCurrent, null));

                                gsmMinKeyCurrent = lsmRangeCurrent.getLow();

                                // Add common entry.
                                result.add(new MappingComparisonResult(ssm, new ShardRange(gsmMinKeyCurrent, gsmRangeCurrent.getHigh()), MappingLocation.MappingInShardMapAndShard, gsmMappingCurrent, lsmMappingCurrent));

                                gsmMinKeyCurrent = lsmRangeCurrent.getHigh();

                                // LSM range exhausted for current iteration.
                                ReferenceObjectHelper<StoreMapping> tempRef_lsmMappingCurrent4 = new ReferenceObjectHelper<StoreMapping>(lsmMappingCurrent);
                                ReferenceObjectHelper<ShardRange> tempRef_lsmRangeCurrent4 = new ReferenceObjectHelper<ShardRange>(lsmRangeCurrent);
                                ReferenceObjectHelper<ShardKey> tempRef_lsmMinKeyCurrent4 = new ReferenceObjectHelper<ShardKey>(lsmMinKeyCurrent);
                                MoveToNextMapping(lsmMappingIterator, keyType, tempRef_lsmMappingCurrent4, tempRef_lsmRangeCurrent4, tempRef_lsmMinKeyCurrent4);
                                lsmMinKeyCurrent = tempRef_lsmMinKeyCurrent4.argValue;
                                lsmRangeCurrent = tempRef_lsmRangeCurrent4.argValue;
                                lsmMappingCurrent = tempRef_lsmMappingCurrent4.argValue;

                                // Detect if GSM range exhausted for current iteration.
                                if (gsmMinKeyCurrent == gsmRangeCurrent.getHigh()) {
                                    ReferenceObjectHelper<StoreMapping> tempRef_gsmMappingCurrent4 = new ReferenceObjectHelper<StoreMapping>(gsmMappingCurrent);
                                    ReferenceObjectHelper<ShardRange> tempRef_gsmRangeCurrent4 = new ReferenceObjectHelper<ShardRange>(gsmRangeCurrent);
                                    ReferenceObjectHelper<ShardKey> tempRef_gsmMinKeyCurrent4 = new ReferenceObjectHelper<ShardKey>(gsmMinKeyCurrent);
                                    MoveToNextMapping(gsmMappingIterator, keyType, tempRef_gsmMappingCurrent4, tempRef_gsmRangeCurrent4, tempRef_gsmMinKeyCurrent4);
                                    gsmMinKeyCurrent = tempRef_gsmMinKeyCurrent4.argValue;
                                    gsmRangeCurrent = tempRef_gsmRangeCurrent4.argValue;
                                    gsmMappingCurrent = tempRef_gsmMappingCurrent4.argValue;
                                }
                            } else {
                                if (lsmRangeCurrent.getLow() < gsmRangeCurrent.getHigh()) {
                                    // Case 2.2: LSM overlaps with GSM, with extra values to the right and finishing after GSM.
                                    //Debug.Assert(lsmMinKeyCurrent != gsmMinKeyCurrent, "Must have been handled by Case 1.3");

                                    // Add the GSM only entry.
                                    result.add(new MappingComparisonResult(ssm, new ShardRange(gsmMinKeyCurrent, lsmMinKeyCurrent), MappingLocation.MappingInShardMapOnly, gsmMappingCurrent, null));

                                    // Add common entry.
                                    result.add(new MappingComparisonResult(ssm, new ShardRange(lsmMinKeyCurrent, gsmRangeCurrent.getHigh()), MappingLocation.MappingInShardMapAndShard, gsmMappingCurrent, lsmMappingCurrent));

                                    lsmMinKeyCurrent = gsmRangeCurrent.getHigh();

                                    // GSM range exhausted for current iteration.
                                    ReferenceObjectHelper<StoreMapping> tempRef_gsmMappingCurrent5 = new ReferenceObjectHelper<StoreMapping>(gsmMappingCurrent);
                                    ReferenceObjectHelper<ShardRange> tempRef_gsmRangeCurrent5 = new ReferenceObjectHelper<ShardRange>(gsmRangeCurrent);
                                    ReferenceObjectHelper<ShardKey> tempRef_gsmMinKeyCurrent5 = new ReferenceObjectHelper<ShardKey>(gsmMinKeyCurrent);
                                    MoveToNextMapping(gsmMappingIterator, keyType, tempRef_gsmMappingCurrent5, tempRef_gsmRangeCurrent5, tempRef_gsmMinKeyCurrent5);
                                    gsmMinKeyCurrent = tempRef_gsmMinKeyCurrent5.argValue;
                                    gsmRangeCurrent = tempRef_gsmRangeCurrent5.argValue;
                                    gsmMappingCurrent = tempRef_gsmMappingCurrent5.argValue;
                                } else { // lsmRangeCurrent.getLow() >= gsmRangeCurrent.getHigh()
                                    // Case 2.3: LSM is entirely to the right of GSM.

                                    // Add the GSM only entry.
                                    result.add(new MappingComparisonResult(ssm, new ShardRange(gsmMinKeyCurrent, gsmRangeCurrent.getHigh()), MappingLocation.MappingInShardMapOnly, gsmMappingCurrent, null));

                                    // GSM range exhausted for current iteration.
                                    ReferenceObjectHelper<StoreMapping> tempRef_gsmMappingCurrent6 = new ReferenceObjectHelper<StoreMapping>(gsmMappingCurrent);
                                    ReferenceObjectHelper<ShardRange> tempRef_gsmRangeCurrent6 = new ReferenceObjectHelper<ShardRange>(gsmRangeCurrent);
                                    ReferenceObjectHelper<ShardKey> tempRef_gsmMinKeyCurrent6 = new ReferenceObjectHelper<ShardKey>(gsmMinKeyCurrent);
                                    MoveToNextMapping(gsmMappingIterator, keyType, tempRef_gsmMappingCurrent6, tempRef_gsmRangeCurrent6, tempRef_gsmMinKeyCurrent6);
                                    gsmMinKeyCurrent = tempRef_gsmMinKeyCurrent6.argValue;
                                    gsmRangeCurrent = tempRef_gsmRangeCurrent6.argValue;
                                    gsmMappingCurrent = tempRef_gsmMappingCurrent6.argValue;
                                }
                            }
                        }
                    } else {
                        // Nothing in LSM, we just keep going over the GSM entries.

                        // Add the GSM only entry.
                        result.add(new MappingComparisonResult(ssm, new ShardRange(gsmMinKeyCurrent, gsmRangeCurrent.getHigh()), MappingLocation.MappingInShardMapOnly, gsmMappingCurrent, null));

                        // GSM range exhausted for current iteration.
                        ReferenceObjectHelper<StoreMapping> tempRef_gsmMappingCurrent7 = new ReferenceObjectHelper<StoreMapping>(gsmMappingCurrent);
                        ReferenceObjectHelper<ShardRange> tempRef_gsmRangeCurrent7 = new ReferenceObjectHelper<ShardRange>(gsmRangeCurrent);
                        ReferenceObjectHelper<ShardKey> tempRef_gsmMinKeyCurrent7 = new ReferenceObjectHelper<ShardKey>(gsmMinKeyCurrent);
                        MoveToNextMapping(gsmMappingIterator, keyType, tempRef_gsmMappingCurrent7, tempRef_gsmRangeCurrent7, tempRef_gsmMinKeyCurrent7);
                        gsmMinKeyCurrent = tempRef_gsmMinKeyCurrent7.argValue;
                        gsmRangeCurrent = tempRef_gsmRangeCurrent7.argValue;
                        gsmMappingCurrent = tempRef_gsmMappingCurrent7.argValue;
                    }
                }
                // Go over the partial remainder of LSM entry if any.
                if (lsmRangeCurrent != null && lsmMinKeyCurrent > lsmRangeCurrent.getLow()) {
                    // Add the LSM only entry.
                    result.add(new MappingComparisonResult(ssm, new ShardRange(lsmMinKeyCurrent, lsmRangeCurrent.getHigh()), MappingLocation.MappingInShardOnly, null, lsmMappingCurrent));

                    // LSM range exhausted for current iteration.
                    ReferenceObjectHelper<StoreMapping> tempRef_lsmMappingCurrent5 = new ReferenceObjectHelper<StoreMapping>(lsmMappingCurrent);
                    ReferenceObjectHelper<ShardRange> tempRef_lsmRangeCurrent5 = new ReferenceObjectHelper<ShardRange>(lsmRangeCurrent);
                    ReferenceObjectHelper<ShardKey> tempRef_lsmMinKeyCurrent5 = new ReferenceObjectHelper<ShardKey>(lsmMinKeyCurrent);
                    MoveToNextMapping(lsmMappingIterator, keyType, tempRef_lsmMappingCurrent5, tempRef_lsmRangeCurrent5, tempRef_lsmMinKeyCurrent5);
                    lsmMinKeyCurrent = tempRef_lsmMinKeyCurrent5.argValue;
                    lsmRangeCurrent = tempRef_lsmRangeCurrent5.argValue;
                    lsmMappingCurrent = tempRef_lsmMappingCurrent5.argValue;
                }

                // Go over remaining Right entries if any which have no matches on Left.
                while (lsmMappingCurrent != null) {
                    // Add the LSM only entry.
                    result.add(new MappingComparisonResult(ssm, lsmRangeCurrent, MappingLocation.MappingInShardOnly, null, lsmMappingCurrent));

                    // LSM range exhausted for current iteration.
                    ReferenceObjectHelper<StoreMapping> tempRef_lsmMappingCurrent6 = new ReferenceObjectHelper<StoreMapping>(lsmMappingCurrent);
                    ReferenceObjectHelper<ShardRange> tempRef_lsmRangeCurrent6 = new ReferenceObjectHelper<ShardRange>(lsmRangeCurrent);
                    ReferenceObjectHelper<ShardKey> tempRef_lsmMinKeyCurrent6 = new ReferenceObjectHelper<ShardKey>(lsmMinKeyCurrent);
                    MoveToNextMapping(lsmMappingIterator, keyType, tempRef_lsmMappingCurrent6, tempRef_lsmRangeCurrent6, tempRef_lsmMinKeyCurrent6);
                    lsmMinKeyCurrent = tempRef_lsmMinKeyCurrent6.argValue;
                    lsmRangeCurrent = tempRef_lsmRangeCurrent6.argValue;
                    lsmMappingCurrent = tempRef_lsmMappingCurrent6.argValue;
                }
            }
        }*/
    //TODO
    return result;
  }

  /**
   * Helper function that produces a list of MappingComparisonResults from union of points in the
   * gsmMappings and lsmMappings.
   *
   * @param ssm StoreShardmap to be referenced in produced MappingComparisonResults
   * @param gsmMappings List of mappings from the GSM.
   * @param lsmMappings List of mappings from the LSM.
   * @return List of mappingcomparisonresults: one for each range arising from the union of
   * boundaries in gsmMappings and lsmMappings.
   */
  public static ArrayList<MappingComparisonResult> ComparePointMappings(StoreShardMap ssm,
      List<StoreMapping> gsmMappings, List<StoreMapping> lsmMappings) {
        /*ShardKeyType keyType = ssm.getKeyType();
        // Get a Linq-able set of points from the input mappings.
        //
        Map<ShardKey, StoreMapping> gsmPoints = gsmMappings.ToDictionary(gsmMapping -> ShardKey.fromRawValue(keyType, gsmMapping.getMinValue()));
        Map<ShardKey, StoreMapping> lsmPoints = lsmMappings.ToDictionary(lsmMapping -> ShardKey.fromRawValue(keyType, lsmMapping.getMinValue()));

        // Construct the output list. This is the concatenation of 3 mappings:
        //  1.) Intersection (the key exists in both the shardmap and the shard.)
        //  2.) Shard only (the key exists only in the shard.)
        //  3.) Shardmap only (the key exists only in the shardmap.)
        //
        ArrayList<MappingComparisonResult> results = (new ArrayList<MappingComparisonResult>()).Concat(lsmPoints.keySet().Intersect(gsmPoints.keySet()).Select(commonPoint -> new MappingComparisonResult(ssm, new ShardRange(commonPoint, commonPoint.getNextKey()), MappingLocation.MappingInShardMapAndShard, gsmPoints.get(commonPoint), lsmPoints.get(commonPoint)))).Concat(lsmPoints.keySet().Except(gsmPoints.keySet()).Select(lsmOnlyPoint -> new MappingComparisonResult(ssm, new ShardRange(lsmOnlyPoint, lsmOnlyPoint.getNextKey()), MappingLocation.MappingInShardOnly, null, lsmPoints.get(lsmOnlyPoint)))).Concat(gsmPoints.keySet().Except(lsmPoints.keySet()).Select(gsmOnlyPoint -> new MappingComparisonResult(ssm, new ShardRange(gsmOnlyPoint, gsmOnlyPoint.getNextKey()), MappingLocation.MappingInShardMapOnly, gsmPoints.get(gsmOnlyPoint), null))).ToList();
        // Intersection.
        // Lsm only.
        // Gsm only.

        return results;*/
    return null; //TODO
  }

  ///#endregion

  ///#region Private Helper Functions

  /**
   * Helper function to advance mapping iterators.
   *
   * @param iterator The iterator to advance.
   * @param keyType The data type of the map key.
   * @param nextMapping Output value that will contain next mapping.
   * @param nextRange Output value that will contain next range.
   * @param nextMinKey Output value that will contain next min key.
   */
  private static void MoveToNextMapping(Iterator<StoreMapping> iterator, ShardKeyType keyType,
      ReferenceObjectHelper<StoreMapping> nextMapping, ReferenceObjectHelper<ShardRange> nextRange,
      ReferenceObjectHelper<ShardKey> nextMinKey) {
//TODO TASK: .NET iterators are only converted within the context of 'while' and 'for' loops:
        /*nextMapping.argValue = iterator.MoveNext() ? iterator.Current : null;
        nextRange.argValue = nextMapping.argValue != null ? new ShardRange(ShardKey.fromRawValue(keyType, nextMapping.argValue.getMinValue()), ShardKey.fromRawValue(keyType, nextMapping.argValue.getMaxValue())) : null;
        nextMinKey.argValue = nextRange.argValue != null ? nextRange.argValue.getLow() : null;*/
  }

  ///#endregion
}

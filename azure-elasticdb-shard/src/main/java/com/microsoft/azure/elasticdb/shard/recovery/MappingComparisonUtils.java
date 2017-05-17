package com.microsoft.azure.elasticdb.shard.recovery;

/* Copyright (c) Microsoft. All rights reserved.
Licensed under the MIT license. See LICENSE file in the project root for full license information.*/

import com.microsoft.azure.elasticdb.core.commons.helpers.ReferenceObjectHelper;
import com.microsoft.azure.elasticdb.shard.base.ShardKey;
import com.microsoft.azure.elasticdb.shard.base.ShardKeyType;
import com.microsoft.azure.elasticdb.shard.base.ShardRange;
import com.microsoft.azure.elasticdb.shard.store.StoreMapping;
import com.microsoft.azure.elasticdb.shard.store.StoreShardMap;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * contains utility methods for performing comparisons among collections of
 * mappings of either list or range shard maps.
 */
public final class MappingComparisonUtils {

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
  public static ArrayList<MappingComparisonResult> compareRangeMappings(StoreShardMap ssm,
      List<StoreMapping> gsmMappings, List<StoreMapping> lsmMappings) {
    // Detect if these are point mappings and call the ComparePointMappings function below.

    ArrayList<MappingComparisonResult> result = new ArrayList<>();

    // Identify the type of keys.
    ShardKeyType keyType = ssm.getKeyType();

    StoreMapping gsmMappingCurrent = null;
    ShardRange gsmRangeCurrent = null;
    ShardKey gsmMinKeyCurrent = null;
    Iterator<StoreMapping> gsmMappingIterator = gsmMappings.iterator();

    ReferenceObjectHelper<StoreMapping> refGsmMappingCurrent =
        new ReferenceObjectHelper<>(gsmMappingCurrent);
    ReferenceObjectHelper<ShardRange> refGsmRangeCurrent =
        new ReferenceObjectHelper<>(gsmRangeCurrent);
    ReferenceObjectHelper<ShardKey> refGsmMinKeyCurrent =
        new ReferenceObjectHelper<>(gsmMinKeyCurrent);
    moveToNextMapping(gsmMappingIterator, keyType, refGsmMappingCurrent,
        refGsmRangeCurrent, refGsmMinKeyCurrent);
    gsmMinKeyCurrent = refGsmMinKeyCurrent.argValue;
    gsmRangeCurrent = refGsmRangeCurrent.argValue;
    gsmMappingCurrent = refGsmMappingCurrent.argValue;

    StoreMapping lsmMappingCurrent = null;
    ShardRange lsmRangeCurrent = null;
    ShardKey lsmMinKeyCurrent = null;
    Iterator<StoreMapping> lsmMappingIterator = lsmMappings.iterator();

    ReferenceObjectHelper<StoreMapping> refLsmMappingCurrent =
        new ReferenceObjectHelper<>(lsmMappingCurrent);
    ReferenceObjectHelper<ShardRange> refLsmRangeCurrent =
        new ReferenceObjectHelper<>(lsmRangeCurrent);
    ReferenceObjectHelper<ShardKey> refLsmMinKeyCurrent =
        new ReferenceObjectHelper<>(lsmMinKeyCurrent);
    moveToNextMapping(lsmMappingIterator, keyType, refLsmMappingCurrent,
        refLsmRangeCurrent, refLsmMinKeyCurrent);
    lsmMinKeyCurrent = refLsmMinKeyCurrent.argValue;
    lsmRangeCurrent = refLsmRangeCurrent.argValue;
    lsmMappingCurrent = refLsmMappingCurrent.argValue;

    while (gsmMinKeyCurrent != null) {
      // If there is something in LSM, consider the following 6 possibilities.
      if (lsmMinKeyCurrent != null) {
        if (ShardKey.opLessThanOrEqual(lsmMinKeyCurrent, gsmMinKeyCurrent)) {
          // Case 1. LSM starts to the left of or exactly at GSM.

          if (ShardKey.opLessThanOrEqual(lsmRangeCurrent.getHigh(), gsmMinKeyCurrent)) {
            // Case 1.1: LSM is entirely to the left of Left.

            // Add the LSM only entry.
            result.add(new MappingComparisonResult(ssm,
                new ShardRange(lsmMinKeyCurrent, lsmRangeCurrent.getHigh()),
                MappingLocation.MappingInShardOnly, null, lsmMappingCurrent));

            // LSM range exhausted for current iteration.
            ReferenceObjectHelper<StoreMapping> refLsmMappingCurrent2 =
                new ReferenceObjectHelper<>(lsmMappingCurrent);
            ReferenceObjectHelper<ShardRange> refLsmRangeCurrent2 =
                new ReferenceObjectHelper<>(lsmRangeCurrent);
            ReferenceObjectHelper<ShardKey> refLsmMinKeyCurrent2 =
                new ReferenceObjectHelper<>(lsmMinKeyCurrent);
            moveToNextMapping(lsmMappingIterator, keyType, refLsmMappingCurrent2,
                refLsmRangeCurrent2, refLsmMinKeyCurrent2);
            lsmMinKeyCurrent = refLsmMinKeyCurrent2.argValue;
            lsmRangeCurrent = refLsmRangeCurrent2.argValue;
            lsmMappingCurrent = refLsmMappingCurrent2.argValue;
          } else {
            if (ShardKey.opLessThanOrEqual(lsmRangeCurrent.getHigh(), gsmRangeCurrent.getHigh())) {
              // Case 1.2: LSM overlaps with GSM,
              // with extra values to the left and finishing before GSM.
              if (!(lsmMinKeyCurrent.equals(gsmMinKeyCurrent))) {
                // Add the LSM only entry.
                result.add(new MappingComparisonResult(ssm,
                    new ShardRange(lsmMinKeyCurrent, gsmMinKeyCurrent),
                    MappingLocation.MappingInShardOnly, null, lsmMappingCurrent));
              }

              // Add common entry.
              result.add(new MappingComparisonResult(ssm,
                  new ShardRange(gsmMinKeyCurrent, lsmRangeCurrent.getHigh()),
                  MappingLocation.MappingInShardMapAndShard, gsmMappingCurrent,
                  lsmMappingCurrent));

              gsmMinKeyCurrent = lsmRangeCurrent.getHigh();

              // LSM range exhausted for current iteration.
              ReferenceObjectHelper<StoreMapping> refLsmMappingCurrent3 =
                  new ReferenceObjectHelper<>(lsmMappingCurrent);
              ReferenceObjectHelper<ShardRange> refLsmRangeCurrent3 =
                  new ReferenceObjectHelper<>(lsmRangeCurrent);
              ReferenceObjectHelper<ShardKey> refLsmMinKeyCurrent3 =
                  new ReferenceObjectHelper<>(lsmMinKeyCurrent);
              moveToNextMapping(lsmMappingIterator, keyType, refLsmMappingCurrent3,
                  refLsmRangeCurrent3, refLsmMinKeyCurrent3);
              lsmMinKeyCurrent = refLsmMinKeyCurrent3.argValue;
              lsmRangeCurrent = refLsmRangeCurrent3.argValue;
              lsmMappingCurrent = refLsmMappingCurrent3.argValue;

              // Detect if GSM range exhausted for current iteration.
              if (gsmMinKeyCurrent.equals(gsmRangeCurrent.getHigh())) {
                ReferenceObjectHelper<StoreMapping> refGsmMappingCurrent2 =
                    new ReferenceObjectHelper<>(gsmMappingCurrent);
                ReferenceObjectHelper<ShardRange> refGsmRangeCurrent2 =
                    new ReferenceObjectHelper<>(gsmRangeCurrent);
                ReferenceObjectHelper<ShardKey> refGsmMinKeyCurrent2 =
                    new ReferenceObjectHelper<>(gsmMinKeyCurrent);
                moveToNextMapping(gsmMappingIterator, keyType, refGsmMappingCurrent2,
                    refGsmRangeCurrent2, refGsmMinKeyCurrent2);
                gsmMinKeyCurrent = refGsmMinKeyCurrent2.argValue;
                gsmRangeCurrent = refGsmRangeCurrent2.argValue;
                gsmMappingCurrent = refGsmMappingCurrent2.argValue;
              }
            } else { // lsmRangeCurrent.getHigh() > gsmRangeCurrent.getHigh()
              // Case 1.3: LSM encompasses GSM.

              // Add the LSM only entry.
              if (!(lsmMinKeyCurrent.equals(gsmMinKeyCurrent))) {
                result.add(new MappingComparisonResult(ssm,
                    new ShardRange(lsmMinKeyCurrent, gsmMinKeyCurrent),
                    MappingLocation.MappingInShardOnly, null, lsmMappingCurrent));
              }

              // Add common entry.
              result.add(new MappingComparisonResult(ssm,
                  new ShardRange(gsmMinKeyCurrent, gsmRangeCurrent.getHigh()),
                  MappingLocation.MappingInShardMapAndShard, gsmMappingCurrent,
                  lsmMappingCurrent));

              lsmMinKeyCurrent = gsmRangeCurrent.getHigh();

              // GSM range exhausted for current iteration.
              ReferenceObjectHelper<StoreMapping> refGsmMappingCurrent3 =
                  new ReferenceObjectHelper<>(gsmMappingCurrent);
              ReferenceObjectHelper<ShardRange> refGsmRangeCurrent3 =
                  new ReferenceObjectHelper<>(gsmRangeCurrent);
              ReferenceObjectHelper<ShardKey> refGsmMinKeyCurrent3 =
                  new ReferenceObjectHelper<>(gsmMinKeyCurrent);
              moveToNextMapping(gsmMappingIterator, keyType, refGsmMappingCurrent3,
                  refGsmRangeCurrent3, refGsmMinKeyCurrent3);
              gsmMinKeyCurrent = refGsmMinKeyCurrent3.argValue;
              gsmRangeCurrent = refGsmRangeCurrent3.argValue;
              gsmMappingCurrent = refGsmMappingCurrent3.argValue;
            }
          }
        } else {
          // Case 2. LSM starts to the right of GSM.

          if (ShardKey.opLessThanOrEqual(lsmRangeCurrent.getHigh(), gsmRangeCurrent.getHigh())) {
            // Case 2.1: GSM encompasses LSM.
            //Debug.Assert(lsmMinKeyCurrent != gsmMinKeyCurrent,
            // "Must have been handled by Case 1.3");

            // Add the GSM only entry.
            result.add(new MappingComparisonResult(ssm,
                new ShardRange(gsmMinKeyCurrent, lsmMinKeyCurrent),
                MappingLocation.MappingInShardMapOnly, gsmMappingCurrent, null));

            gsmMinKeyCurrent = lsmRangeCurrent.getLow();

            // Add common entry.
            result.add(new MappingComparisonResult(ssm,
                new ShardRange(gsmMinKeyCurrent, gsmRangeCurrent.getHigh()),
                MappingLocation.MappingInShardMapAndShard, gsmMappingCurrent,
                lsmMappingCurrent));

            gsmMinKeyCurrent = lsmRangeCurrent.getHigh();

            // LSM range exhausted for current iteration.
            ReferenceObjectHelper<StoreMapping> refLsmMappingCurrent4 =
                new ReferenceObjectHelper<>(lsmMappingCurrent);
            ReferenceObjectHelper<ShardRange> refLsmRangeCurrent4 =
                new ReferenceObjectHelper<>(lsmRangeCurrent);
            ReferenceObjectHelper<ShardKey> refLsmMinKeyCurrent4 =
                new ReferenceObjectHelper<>(lsmMinKeyCurrent);
            moveToNextMapping(lsmMappingIterator, keyType, refLsmMappingCurrent4,
                refLsmRangeCurrent4, refLsmMinKeyCurrent4);
            lsmMinKeyCurrent = refLsmMinKeyCurrent4.argValue;
            lsmRangeCurrent = refLsmRangeCurrent4.argValue;
            lsmMappingCurrent = refLsmMappingCurrent4.argValue;

            // Detect if GSM range exhausted for current iteration.
            if (gsmMinKeyCurrent.equals(gsmRangeCurrent.getHigh())) {
              ReferenceObjectHelper<StoreMapping> refGsmMappingCurrent4 =
                  new ReferenceObjectHelper<>(gsmMappingCurrent);
              ReferenceObjectHelper<ShardRange> refGsmRangeCurrent4 =
                  new ReferenceObjectHelper<>(gsmRangeCurrent);
              ReferenceObjectHelper<ShardKey> refGsmMinKeyCurrent4 =
                  new ReferenceObjectHelper<>(gsmMinKeyCurrent);
              moveToNextMapping(gsmMappingIterator, keyType, refGsmMappingCurrent4,
                  refGsmRangeCurrent4, refGsmMinKeyCurrent4);
              gsmMinKeyCurrent = refGsmMinKeyCurrent4.argValue;
              gsmRangeCurrent = refGsmRangeCurrent4.argValue;
              gsmMappingCurrent = refGsmMappingCurrent4.argValue;
            }
          } else {
            if (ShardKey.opLessThan(lsmRangeCurrent.getLow(), gsmRangeCurrent.getHigh())) {
              // Case 2.2: LSM overlaps with GSM,
              // with extra values to the right and finishing after GSM.
              //Debug.Assert(lsmMinKeyCurrent != gsmMinKeyCurrent,
              // "Must have been handled by Case 1.3");

              // Add the GSM only entry.
              result.add(new MappingComparisonResult(ssm,
                  new ShardRange(gsmMinKeyCurrent, lsmMinKeyCurrent),
                  MappingLocation.MappingInShardMapOnly, gsmMappingCurrent, null));

              // Add common entry.
              result.add(new MappingComparisonResult(ssm,
                  new ShardRange(lsmMinKeyCurrent, gsmRangeCurrent.getHigh()),
                  MappingLocation.MappingInShardMapAndShard, gsmMappingCurrent,
                  lsmMappingCurrent));

              lsmMinKeyCurrent = gsmRangeCurrent.getHigh();

              // GSM range exhausted for current iteration.
              ReferenceObjectHelper<StoreMapping> refGsmMappingCurrent5 =
                  new ReferenceObjectHelper<>(gsmMappingCurrent);
              ReferenceObjectHelper<ShardRange> refGsmRangeCurrent5 =
                  new ReferenceObjectHelper<>(gsmRangeCurrent);
              ReferenceObjectHelper<ShardKey> refGsmMinKeyCurrent5 =
                  new ReferenceObjectHelper<>(gsmMinKeyCurrent);
              moveToNextMapping(gsmMappingIterator, keyType, refGsmMappingCurrent5,
                  refGsmRangeCurrent5, refGsmMinKeyCurrent5);
              gsmMinKeyCurrent = refGsmMinKeyCurrent5.argValue;
              gsmRangeCurrent = refGsmRangeCurrent5.argValue;
              gsmMappingCurrent = refGsmMappingCurrent5.argValue;
            } else { // lsmRangeCurrent.getLow() >= gsmRangeCurrent.getHigh()
              // Case 2.3: LSM is entirely to the right of GSM.

              // Add the GSM only entry.
              result.add(new MappingComparisonResult(ssm,
                  new ShardRange(gsmMinKeyCurrent, gsmRangeCurrent.getHigh()),
                  MappingLocation.MappingInShardMapOnly, gsmMappingCurrent, null));

              // GSM range exhausted for current iteration.
              ReferenceObjectHelper<StoreMapping> refGsmMappingCurrent6 =
                  new ReferenceObjectHelper<>(gsmMappingCurrent);
              ReferenceObjectHelper<ShardRange> refGsmRangeCurrent6 =
                  new ReferenceObjectHelper<>(gsmRangeCurrent);
              ReferenceObjectHelper<ShardKey> refGsmMinKeyCurrent6 =
                  new ReferenceObjectHelper<>(gsmMinKeyCurrent);
              moveToNextMapping(gsmMappingIterator, keyType, refGsmMappingCurrent6,
                  refGsmRangeCurrent6, refGsmMinKeyCurrent6);
              gsmMinKeyCurrent = refGsmMinKeyCurrent6.argValue;
              gsmRangeCurrent = refGsmRangeCurrent6.argValue;
              gsmMappingCurrent = refGsmMappingCurrent6.argValue;
            }
          }
        }
      } else {
        // Nothing in LSM, we just keep going over the GSM entries.

        // Add the GSM only entry.
        result.add(new MappingComparisonResult(ssm,
            new ShardRange(gsmMinKeyCurrent, gsmRangeCurrent.getHigh()),
            MappingLocation.MappingInShardMapOnly, gsmMappingCurrent, null));

        // GSM range exhausted for current iteration.
        ReferenceObjectHelper<StoreMapping> refGsmMappingCurrent7 =
            new ReferenceObjectHelper<>(gsmMappingCurrent);
        ReferenceObjectHelper<ShardRange> refGsmRangeCurrent7 =
            new ReferenceObjectHelper<>(gsmRangeCurrent);
        ReferenceObjectHelper<ShardKey> refGsmMinKeyCurrent7 =
            new ReferenceObjectHelper<>(gsmMinKeyCurrent);
        moveToNextMapping(gsmMappingIterator, keyType, refGsmMappingCurrent7,
            refGsmRangeCurrent7, refGsmMinKeyCurrent7);
        gsmMinKeyCurrent = refGsmMinKeyCurrent7.argValue;
        gsmRangeCurrent = refGsmRangeCurrent7.argValue;
        gsmMappingCurrent = refGsmMappingCurrent7.argValue;
      }
    }
    // Go over the partial remainder of LSM entry if any.
    if (lsmRangeCurrent != null
        && ShardKey.opGreaterThan(lsmMinKeyCurrent, lsmRangeCurrent.getLow())) {
      // Add the LSM only entry.
      result.add(new MappingComparisonResult(ssm,
          new ShardRange(lsmMinKeyCurrent, lsmRangeCurrent.getHigh()),
          MappingLocation.MappingInShardOnly, null, lsmMappingCurrent));

      // LSM range exhausted for current iteration.
      ReferenceObjectHelper<StoreMapping> refLsmMappingCurrent5 =
          new ReferenceObjectHelper<>(lsmMappingCurrent);
      ReferenceObjectHelper<ShardRange> refLsmRangeCurrent5 =
          new ReferenceObjectHelper<>(lsmRangeCurrent);
      ReferenceObjectHelper<ShardKey> refLsmMinKeyCurrent5 =
          new ReferenceObjectHelper<>(lsmMinKeyCurrent);
      moveToNextMapping(lsmMappingIterator, keyType, refLsmMappingCurrent5,
          refLsmRangeCurrent5, refLsmMinKeyCurrent5);
      lsmMinKeyCurrent = refLsmMinKeyCurrent5.argValue;
      lsmRangeCurrent = refLsmRangeCurrent5.argValue;
      lsmMappingCurrent = refLsmMappingCurrent5.argValue;
    }

    // Go over remaining Right entries if any which have no matches on Left.
    while (lsmMappingCurrent != null) {
      // Add the LSM only entry.
      result.add(
          new MappingComparisonResult(ssm, lsmRangeCurrent, MappingLocation.MappingInShardOnly,
              null, lsmMappingCurrent));

      // LSM range exhausted for current iteration.
      ReferenceObjectHelper<StoreMapping> refLsmMappingCurrent6 =
          new ReferenceObjectHelper<>(lsmMappingCurrent);
      ReferenceObjectHelper<ShardRange> refLsmRangeCurrent6 =
          new ReferenceObjectHelper<>(lsmRangeCurrent);
      ReferenceObjectHelper<ShardKey> refLsmMinKeyCurrent6 =
          new ReferenceObjectHelper<>(lsmMinKeyCurrent);
      moveToNextMapping(lsmMappingIterator, keyType, refLsmMappingCurrent6,
          refLsmRangeCurrent6, refLsmMinKeyCurrent6);
      lsmMinKeyCurrent = refLsmMinKeyCurrent6.argValue;
      lsmRangeCurrent = refLsmRangeCurrent6.argValue;
      lsmMappingCurrent = refLsmMappingCurrent6.argValue;
    }

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
  public static List<MappingComparisonResult> comparePointMappings(StoreShardMap ssm,
      List<StoreMapping> gsmMappings, List<StoreMapping> lsmMappings) {
    ShardKeyType keyType = ssm.getKeyType();
    // Get a Linq-able set of points from the input mappings.
    Map<ShardKey, StoreMapping> gsmPoints = new HashMap<>();
    for (StoreMapping mapping : gsmMappings) {
      gsmPoints.put(ShardKey.fromRawValue(keyType, mapping.getMinValue()), mapping);
    }
    Map<ShardKey, StoreMapping> lsmPoints = new HashMap<>();
    for (StoreMapping mapping : lsmMappings) {
      lsmPoints.put(ShardKey.fromRawValue(keyType, mapping.getMinValue()), mapping);
    }

    // Construct the output list. This is the concatenation of 3 mappings:
    //  1.) Intersection (the key exists in both the shardmap and the shard.)
    //  2.) Shard only (the key exists only in the shard.)
    //  3.) Shardmap only (the key exists only in the shardmap.)
    //
    Set<ShardKey> lsmKeySet = lsmPoints.keySet();
    Set<ShardKey> gsmKeySet = gsmPoints.keySet();
    Set<ShardKey> lsmAndGsmKeySet = intersect(lsmKeySet, gsmKeySet);

    List<MappingComparisonResult> intersection = lsmAndGsmKeySet.stream()
        .map(commonPoint -> new MappingComparisonResult(ssm,
            new ShardRange(commonPoint, commonPoint.getNextKey()),
            MappingLocation.MappingInShardMapAndShard, gsmPoints.get(commonPoint),
            lsmPoints.get(commonPoint)))
        .collect(Collectors.toList());
    List<MappingComparisonResult> shardOnly = lsmKeySet.stream()
        .filter(key -> !gsmKeySet.contains(key))
        .map(lsmOnlyPoint -> new MappingComparisonResult(ssm,
            new ShardRange(lsmOnlyPoint, lsmOnlyPoint.getNextKey()),
            MappingLocation.MappingInShardOnly, null, lsmPoints.get(lsmOnlyPoint)))
        .collect(Collectors.toList());
    List<MappingComparisonResult> shardMapOnly = gsmPoints.keySet().stream()
        .filter(key -> !lsmKeySet.contains(key))
        .map(gsmOnlyPoint -> new MappingComparisonResult(ssm,
            new ShardRange(gsmOnlyPoint, gsmOnlyPoint.getNextKey()),
            MappingLocation.MappingInShardMapOnly, gsmPoints.get(gsmOnlyPoint), null))
        .collect(Collectors.toList());
    // Intersection.
    // Lsm only.
    // Gsm only.
    intersection.addAll(shardOnly);
    intersection.addAll(shardMapOnly);

    return intersection;
  }

  private static Set<ShardKey> intersect(Set<ShardKey> left, Set<ShardKey> right) {
    Objects.requireNonNull(left);
    Objects.requireNonNull(right);

    Set<ShardKey> keySet = new HashSet<>();
    left.forEach(key -> {
      if (right.contains(key)) {
        keySet.add(key);
      }
    });
    return keySet;
  }

  /**
   * Helper function to advance mapping iterators.
   *
   * @param iterator The iterator to advance.
   * @param keyType The data type of the map key.
   * @param nextMapping Output value that will contain next mapping.
   * @param nextRange Output value that will contain next range.
   * @param nextMinKey Output value that will contain next min key.
   */
  private static void moveToNextMapping(Iterator<StoreMapping> iterator, ShardKeyType keyType,
      ReferenceObjectHelper<StoreMapping> nextMapping, ReferenceObjectHelper<ShardRange> nextRange,
      ReferenceObjectHelper<ShardKey> nextMinKey) {
    nextMapping.argValue = iterator.hasNext() ? iterator.next() : null;
    nextRange.argValue = nextMapping.argValue != null ? new ShardRange(
        ShardKey.fromRawValue(keyType, nextMapping.argValue.getMinValue()),
        ShardKey.fromRawValue(keyType, nextMapping.argValue.getMaxValue())) : null;
    nextMinKey.argValue = nextRange.argValue != null ? nextRange.argValue.getLow() : null;
  }
}

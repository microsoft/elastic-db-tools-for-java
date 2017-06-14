package com.microsoft.azure.elasticdb.shard.unittests;

/*Copyright (c) Microsoft. All rights reserved.
Licensed under the MIT license. See LICENSE file in the project root for full license information.*/

import java.util.ArrayList;
import java.util.function.Predicate;

public final class ListHelper {

  /**
   * Check if item exists in the list.
   */
  public static <T> boolean exists(ArrayList<T> list, Predicate<T> p) {
    for (T item : list) {
      if (p.test(item)) {
        return true;
      }
    }

    return false;
  }

  /**
   * Find an item in the list.
   */
  public static <T> T find(ArrayList<T> list, Predicate<T> p) {
    for (T item : list) {
      if (p.test(item)) {
        return item;
      }
    }

    return null;
  }

  /**
   * Find all items that match the criteria in the list.
   */
  public static <T> ArrayList<T> findAll(ArrayList<T> list, Predicate<T> p) {
    ArrayList<T> dest = new ArrayList<T>();

    for (T item : list) {
      if (p.test(item)) {
        dest.add(item);
      }
    }

    return dest;
  }

  /**
   * Find the index of the item in the list.
   */
  public static <T> int findIndex(ArrayList<T> list, Predicate<T> p) {
    for (int i = 0; i < list.size(); i++) {
      if (p.test(list.get(i))) {
        return i;
      }
    }

    return -1;
  }

  /**
   * Find the index of the item in the list.
   */
  public static <T> int findIndex(ArrayList<T> list, int start, Predicate<T> p) {
    for (int i = start; i < list.size(); i++) {
      if (p.test(list.get(i))) {
        return i;
      }
    }

    return -1;
  }

  /**
   * Find the index of the item in the list.
   */
  public static <T> int findIndex(ArrayList<T> list, int start, int count, Predicate<T> p) {
    for (int i = start; i < start + count; i++) {
      if (p.test(list.get(i))) {
        return i;
      }
    }

    return -1;
  }

  /**
   * Find the last item in the list.
   */
  public static <T> T findLast(ArrayList<T> list, Predicate<T> p) {
    for (int i = list.size() - 1; i > -1; i--) {
      if (p.test(list.get(i))) {
        return list.get(i);
      }
    }

    return null;
  }

  /**
   * Find the index of the last item in the list.
   */
  public static <T> int findLastIndex(ArrayList<T> list, Predicate<T> p) {
    for (int i = list.size() - 1; i > -1; i--) {
      if (p.test(list.get(i))) {
        return i;
      }
    }

    return -1;
  }

  /**
   * Find the index of the last item in the list.
   */
  public static <T> int findLastIndex(ArrayList<T> list, int start, Predicate<T> p) {
    for (int i = start; i > -1; i--) {
      if (p.test(list.get(i))) {
        return i;
      }
    }

    return -1;
  }

  /**
   * Find the index of the last item in the list.
   */
  public static <T> int findLastIndex(ArrayList<T> list, int start, int count, Predicate<T> p) {
    for (int i = start; i > start - count; i--) {
      if (p.test(list.get(i))) {
        return i;
      }
    }

    return -1;
  }

  /**
   * Remove all items in the list.
   */
  public static <T> int removeAll(ArrayList<T> list, Predicate<T> p) {
    int removed = 0;
    for (int i = 0; i < list.size(); i++) {
      if (p.test(list.get(i))) {
        list.remove(i);
        i--;
        removed++;
      }
    }

    return removed;
  }

  /**
   * Check if the condition is true for all items in the list.
   */
  public static <T> boolean trueForAll(ArrayList<T> list, Predicate<T> p) {
    for (T item : list) {
      if (!p.test(item)) {
        return false;
      }
    }

    return true;
  }
}
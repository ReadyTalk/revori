/* Copyright (c) 2010-2012, Revori Contributors

   Permission to use, copy, modify, and/or distribute this software
   for any purpose with or without fee is hereby granted, provided
   that the above copyright notice and this permission notice appear
   in all copies. */

package com.readytalk.revori.util;

import java.util.List;
import java.util.ArrayList;
import java.util.Set;
import java.util.HashSet;
import java.util.Collection;
import java.util.Iterator;

import com.readytalk.revori.Column;

public class Util {
  private static final boolean Debug = true;

  public static <T> List<T> append(List<T> list, T ... elements) {
    return appendToList(list, elements);
  }

  public static <T> List<T> appendToList(List<T> list, T[] elements) {
    List<T> result = new ArrayList<T>(list.size() + elements.length);
    result.addAll(list);
    for (T o: elements) result.add(o);
    return result;
  }

  public static <T> List<T> list(T ... elements) {
    return toList(elements);
  }

  public static List<Column<?>> cols(Column<?> ... elements) {
    return toList(elements);
  }

  public static <T> List<T> toList(T[] elements) {
    List<T> list = new ArrayList<T>(elements.length);
    for (T o: elements) list.add(o);
    return list;
  }

  public static <T> Set<T> set(T ... elements) {
    return toSet(elements);
  }

  public static <T> Set<T> toSet(T[] elements) {
    Set<T> set = new HashSet<T>(elements.length);
    for (T o: elements) set.add(o);
    return set;
  }

  public static <T> Set<T> union(Collection<T> ... sets) {
    Set<T> set = new HashSet<T>();
    for (Collection<T> s: sets) {
      set.addAll(s);
    }
    return set;
  }

  public static void expect(boolean v) {
    if (Debug && ! v) {
      throw new RuntimeException();
    }
  }

  public static Object[] copy(Object[] array) {
    Object[] copy = new Object[array.length];
    System.arraycopy(array, 0, copy, 0, array.length);
    return copy;
  }

  public static <T extends Comparable<T>> int compare(Collection<T> a,
                            Collection<T> b)
  {
    int d = a.size() - b.size();
    if (d != 0) {
      return d;
    }

    Iterator<T> ai = a.iterator();
    Iterator<T> bi = b.iterator();
    while (ai.hasNext()) {
      d = ai.next().compareTo(bi.next());
      if (d != 0) {
        return d;
      }
    }

    return 0;
  }

  public static String toString(Object[] array, int offset, int length) {
    StringBuilder sb = new StringBuilder();
    sb.append("[");
    for (int i = offset; i < offset + length; ++i) {
      sb.append(array[i]);
      if (i < offset + length - 1) {
        sb.append(", ");
      }
    }
    sb.append("]");
    return sb.toString();
  }
}

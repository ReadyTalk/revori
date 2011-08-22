package com.readytalk.oss.dbms.util;

import java.util.List;
import java.util.ArrayList;
import java.util.Set;
import java.util.HashSet;
import java.util.Collection;
import java.util.Iterator;

public class Util {
  private static final boolean Debug = true;

  public static <T> List<T> append(List<T> list, T ... elements) {
    return appendToList(list, elements);
  }

  public static <T> List<T> appendToList(List<T> list, T[] elements) {
    List<T> result = new ArrayList(list.size() + elements.length);
    result.addAll(list);
    for (T o: elements) result.add(o);
    return result;
  }

  public static <T> List<T> list(T ... elements) {
    return toList(elements);
  }

  public static <T> List<T> toList(T[] elements) {
    List<T> list = new ArrayList(elements.length);
    for (T o: elements) list.add(o);
    return list;
  }

  public static <T> Set<T> set(T ... elements) {
    return toSet(elements);
  }

  public static <T> Set<T> toSet(T[] elements) {
    Set<T> set = new HashSet(elements.length);
    for (T o: elements) set.add(o);
    return set;
  }

  public static <T> Set<T> union(Collection<T> ... sets) {
    Set<T> set = new HashSet();
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

  public static int compare(Collection<? extends Comparable> a,
                            Collection<? extends Comparable> b)
  {
    int d = a.size() - b.size();
    if (d != 0) {
      return d;
    }

    Iterator<? extends Comparable> ai = a.iterator();
    Iterator<? extends Comparable> bi = b.iterator();
    while (ai.hasNext()) {
      d = ai.next().compareTo(bi.next());
      if (d != 0) {
        return d;
      }
    }

    return 0;
  }
}

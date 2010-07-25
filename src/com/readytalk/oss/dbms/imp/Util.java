package com.readytalk.oss.dbms.imp;

import java.util.List;
import java.util.ArrayList;
import java.util.Set;
import java.util.HashSet;

public class Util {
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
}

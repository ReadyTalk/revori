package com.readytalk.revori;

import java.util.Comparator;

public class Comparators {
  public static final Comparator<Comparable> Ascending
    = new Comparator<Comparable>() {
    public int compare(Comparable a, Comparable b) {
      return a.compareTo(b);
    }
  };

  public static final Comparator<Comparable> Descending
    = new Comparator<Comparable>() {
    public int compare(Comparable a, Comparable b) {
      return b.compareTo(a);
    }
  };
}

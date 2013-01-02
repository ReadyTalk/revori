/* Copyright (c) 2010-2012, Revori Contributors

   Permission to use, copy, modify, and/or distribute this software
   for any purpose with or without fee is hereby granted, provided
   that the above copyright notice and this permission notice appear
   in all copies. */

package com.readytalk.revori;

import java.util.Comparator;

import javax.annotation.concurrent.Immutable;

@Immutable
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

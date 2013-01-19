/* Copyright (c) 2010-2012, Revori Contributors

   Permission to use, copy, modify, and/or distribute this software
   for any purpose with or without fee is hereby granted, provided
   that the above copyright notice and this permission notice appear
   in all copies. */

package com.readytalk.revori.imp;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

class UnionScan implements Scan {
  private final Scan left;
  private final Scan right;
  private final Comparator comparator;

  public UnionScan(Scan left,
                   Scan right,
                   Comparator comparator)
  {
    this.left = left;
    this.right = right;
    this.comparator = comparator;
  }

  public boolean isUseful() {
    return left.isUseful() && right.isUseful();
  }

  public boolean isSpecific() {
    return left.isSpecific() && right.isSpecific();
  }

  public boolean isUnknown() {
    return left.isUnknown() || right.isUnknown();
  }

  public List<Interval> evaluate() {
    Iterator<Interval> leftIterator = left.evaluate().iterator();
    Iterator<Interval> rightIterator = right.evaluate().iterator();
    List<Interval> result = new ArrayList<Interval>();

    int d = 0;
    Interval leftItem = null;
    Interval rightItem = null;
    while (leftIterator.hasNext() && rightIterator.hasNext()) {
      if (d < 0) {
        leftItem = leftIterator.next();
      } else if (d > 0) {
        rightItem = rightIterator.next();
      } else {
        leftItem = leftIterator.next();
        rightItem = rightIterator.next();
      }

      d = Compare.compare(leftItem, rightItem, comparator);
      if (d < -1) {
        result.add(leftItem);
        result.add(rightItem);
      } else if (d > 1) {
        result.add(rightItem);
        result.add(leftItem);
      } else {
        result.add(Interval.union(leftItem, rightItem, comparator));
      }
    }

    while (leftIterator.hasNext()) {
      result.add(leftIterator.next());
    }

    while (rightIterator.hasNext()) {
      result.add(rightIterator.next());
    }

    return result;
  }
}

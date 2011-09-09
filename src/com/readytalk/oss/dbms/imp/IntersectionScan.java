package com.readytalk.oss.dbms.imp;

import java.util.List;
import java.util.ArrayList;
import java.util.Iterator;

class IntersectionScan implements Scan {
  public final Scan left;
  public final Scan right;

  public IntersectionScan(Scan left,
                          Scan right)
  {
    this.left = left;
    this.right = right;
  }

  public boolean isUseful() {
    return left.isUseful() || right.isUseful();
  }

  public boolean isSpecific() {
    return left.isSpecific() || right.isSpecific();
  }

  public boolean isUnknown() {
    return left.isUnknown() && right.isUnknown();
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

      d = Compare.compare(leftItem, rightItem);
      if (d >= -1 && d <= 1) {
        result.add(Interval.intersection(leftItem, rightItem));
      }
    }

    return result;
  }
}

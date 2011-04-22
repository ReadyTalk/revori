package com.readytalk.oss.dbms.imp;

import java.util.List;
import java.util.ArrayList;
import java.util.Iterator;

class UnionScan implements Scan {
  public final Scan left;
  public final Scan right;

  public UnionScan(Scan left,
                   Scan right)
  {
    this.left = left;
    this.right = right;
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
    List<Interval> result = new ArrayList();

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
      if (d < -1) {
        result.add(leftItem);
        result.add(rightItem);
      } else if (d > 1) {
        result.add(rightItem);
        result.add(leftItem);
      } else {
        result.add(Interval.union(leftItem, rightItem));
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

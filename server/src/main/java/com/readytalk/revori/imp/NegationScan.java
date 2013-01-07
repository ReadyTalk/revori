/* Copyright (c) 2010-2012, Revori Contributors

   Permission to use, copy, modify, and/or distribute this software
   for any purpose with or without fee is hereby granted, provided
   that the above copyright notice and this permission notice appear
   in all copies. */

package com.readytalk.revori.imp;

import java.util.ArrayList;
import java.util.List;

import com.readytalk.revori.imp.Interval.BoundType;

class NegationScan implements Scan {
  public final Scan operand;

  public NegationScan(Scan operand) {
    this.operand = operand;
  }

  public boolean isUseful() {
    return isSpecific();
  }

  public boolean isSpecific() {
    return ! (operand.isUnknown() || operand.isSpecific());
  }

  public boolean isUnknown() {
    return operand.isUnknown();
  }

  public List<Interval> evaluate() {
    List<Interval> result = new ArrayList();
    Interval previous = null;

    for (Interval i: operand.evaluate()) {
      if (previous == null) {
        if (i.low != Compare.Undefined) {
          result.add
            (new Interval
             (Compare.Undefined, BoundType.Inclusive,
              i.low, i.lowBoundType.opposite));
          previous = i;
        }
      } else {
        result.add
          (new Interval
           (previous.high, previous.highBoundType.opposite,
            i.low, i.lowBoundType.opposite));
        previous = i;
      }
    }

    if (previous == null) {
      result.add(Interval.Unbounded);
    } else if (previous.high != Compare.Undefined) {
      result.add
        (new Interval
         (previous.high, previous.highBoundType.opposite,
          Compare.Undefined, BoundType.Inclusive));
    }

    return result;
  }
}

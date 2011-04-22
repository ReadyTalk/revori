package com.readytalk.oss.dbms.imp;

import com.readytalk.oss.dbms.imp.Interval.BoundType;

import java.util.List;
import java.util.ArrayList;

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
        }
      } else {
        result.add
          (new Interval
           (previous.high, previous.highBoundType.opposite,
            i.low, i.lowBoundType.opposite));
      }
        
      previous = i;
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

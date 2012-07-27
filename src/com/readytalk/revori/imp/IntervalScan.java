package com.readytalk.revori.imp;

import static com.readytalk.revori.util.Util.list;

import com.readytalk.revori.imp.Interval.BoundType;

import java.util.List;

class IntervalScan implements Scan {
  public static final IntervalScan Unbounded
    = new IntervalScan(ConstantAdapter.Undefined, ConstantAdapter.Undefined);

  public static final IntervalScan Empty
    = new IntervalScan(ConstantAdapter.Dummy, ConstantAdapter.Dummy);

  public final ExpressionAdapter low;
  public final BoundType lowBoundType;
  public final ExpressionAdapter high;
  public final BoundType highBoundType;

  public IntervalScan(ExpressionAdapter low,
                      BoundType lowBoundType,
                      ExpressionAdapter high,
                      BoundType highBoundType)
  {
    this.low = low;
    this.lowBoundType = lowBoundType;
    this.high = high;
    this.highBoundType = highBoundType;
  }

  public IntervalScan(ExpressionAdapter low,
                      ExpressionAdapter high)
  {
    this(low, BoundType.Inclusive, high, BoundType.Inclusive);
  }

  public boolean isUseful() {
    return low.evaluate(false) != Compare.Undefined 
      || high.evaluate(false) != Compare.Undefined;
  }

  public boolean isUnknown() {
    return false;
  }

  public boolean isSpecific() {
    return low.evaluate(false) != Compare.Undefined
      && high.evaluate(false) != Compare.Undefined;
  }

  public List<Interval> evaluate() {
    return list
      (new Interval
       ((Comparable) low.evaluate(false), lowBoundType,
        (Comparable) high.evaluate(false), highBoundType));
  }
}

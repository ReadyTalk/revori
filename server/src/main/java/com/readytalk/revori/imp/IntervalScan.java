/* Copyright (c) 2010-2012, Revori Contributors

   Permission to use, copy, modify, and/or distribute this software
   for any purpose with or without fee is hereby granted, provided
   that the above copyright notice and this permission notice appear
   in all copies. */

package com.readytalk.revori.imp;

import java.util.List;

import com.google.common.collect.Lists;
import com.readytalk.revori.imp.Interval.BoundType;

class IntervalScan implements Scan {
  public static final IntervalScan Unbounded
    = new IntervalScan(ConstantAdapter.Undefined, ConstantAdapter.Undefined);

  public static final IntervalScan Empty
    = new IntervalScan(ConstantAdapter.Dummy, ConstantAdapter.Dummy);

  private final ExpressionAdapter low;
  private final BoundType lowBoundType;
  private final ExpressionAdapter high;
  private final BoundType highBoundType;

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
		return Lists.newArrayList(new Interval((Comparable) low.evaluate(false), lowBoundType, (Comparable) high
				.evaluate(false), highBoundType));
	}
}

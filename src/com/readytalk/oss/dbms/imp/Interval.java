package com.readytalk.oss.dbms.imp;

class Interval {
  public enum BoundType {
    Inclusive, Exclusive;

    static {
      Inclusive.opposite = Exclusive;
      Exclusive.opposite = Inclusive;
    }

    public BoundType opposite;
  }

  public static final Interval Unbounded = new Interval
    (Compare.Undefined, Compare.Undefined);

  public final Comparable low;
  public final BoundType lowBoundType;
  public final Comparable high;
  public final BoundType highBoundType;

  public Interval(Comparable low,
                  BoundType lowBoundType,
                  Comparable high,
                  BoundType highBoundType)
  {
    this.low = low;
    this.lowBoundType = lowBoundType;
    this.high = high;
    this.highBoundType = highBoundType;
  }

  public Interval(Comparable low,
                  Comparable high)
  {
    this(low, BoundType.Inclusive, high, BoundType.Inclusive);
  }

  public static Interval intersection(Interval left,
                                      Interval right)
  {
    Comparable low;
    BoundType lowBoundType;
    int lowDifference = Compare.compare(left.low, false, right.low, false);
    if (lowDifference > 0) {
      low = left.low;
      lowBoundType = left.lowBoundType;
    } else if (lowDifference < 0) {
      low = right.low;
      lowBoundType = right.lowBoundType;
    } else {
      low = left.low;
      lowBoundType = (left.lowBoundType == BoundType.Exclusive
                      || right.lowBoundType == BoundType.Exclusive
                      ? BoundType.Exclusive : BoundType.Inclusive);
    }

    Comparable high;
    BoundType highBoundType;
    int highDifference = Compare.compare(left.high, true, right.high, true);
    if (highDifference > 0) {
      high = right.high;
      highBoundType = right.highBoundType;
    } else if (highDifference < 0) {
      high = left.high;
      highBoundType = left.highBoundType;
    } else {
      high = left.high;
      highBoundType = (left.highBoundType == BoundType.Exclusive
                      || right.highBoundType == BoundType.Exclusive
                      ? BoundType.Exclusive : BoundType.Inclusive);
    }

    return new Interval(low, lowBoundType, high, highBoundType);
  }

  public static Interval union(Interval left,
                               Interval right)
  {
    Comparable low;
    BoundType lowBoundType;
    int lowDifference = Compare.compare(left.low, false, right.low, false);
    if (lowDifference > 0) {
      low = right.low;
      lowBoundType = right.lowBoundType;
    } else if (lowDifference < 0) {
      low = left.low;
      lowBoundType = left.lowBoundType;
    } else {
      low = left.low;
      lowBoundType = (left.lowBoundType == BoundType.Inclusive
                      || right.lowBoundType == BoundType.Inclusive
                      ? BoundType.Inclusive : BoundType.Exclusive);
    }

    Comparable high;
    BoundType highBoundType;
    int highDifference = Compare.compare(left.high, true, right.high, true);
    if (highDifference > 0) {
      high = left.high;
      highBoundType = left.highBoundType;
    } else if (highDifference < 0) {
      high = right.high;
      highBoundType = right.highBoundType;
    } else {
      high = left.high;
      highBoundType = (left.lowBoundType == BoundType.Inclusive
                       || right.lowBoundType == BoundType.Inclusive
                       ? BoundType.Inclusive : BoundType.Exclusive);
    }

    return new Interval(low, lowBoundType, high, highBoundType);
  }
}

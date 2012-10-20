/* Copyright (c) 2010-2012, Revori Contributors

   Permission to use, copy, modify, and/or distribute this software
   for any purpose with or without fee is hereby granted, provided
   that the above copyright notice and this permission notice appear
   in all copies. */

package com.readytalk.revori.imp;

import com.readytalk.revori.Table;
import com.readytalk.revori.Index;
import com.readytalk.revori.Column;

import com.readytalk.revori.imp.Interval.BoundType;

import java.util.Comparator;

public class Compare {
  public static final Comparable Undefined = new Comparable() {
      public int compareTo(Object o) {
        throw new UnsupportedOperationException();
      }
      
      public String toString() {
        return "undefined";
      }
    };

  public static final Comparable Dummy = new Comparable() {
      public int compareTo(Object o) {
        throw new UnsupportedOperationException();
      }
      
      public String toString() {
        return "dummy";
      }
    };

  public static Comparator TableComparator = new Comparator<Table>() {
    public int compare(Table a, Table b) {
      if (a == b) {
        return 0;
      }

      if (a.equals(Constants.IndexTable)) {
        if (b.equals(Constants.IndexTable)) {
          return 0;
        } else {
          return -1;
        }
      } else if (b.equals(Constants.IndexTable)) {
        return 1;
      } else {
        return a.compareTo(b);
      }
    }
  };

  public static Comparator IndexComparator = new Comparator<Index>() {
    public int compare(Index a, Index b) {
      return a.compareTo(b);
    }
  };

  public static Comparator ColumnComparator = new Comparator<Column>() {
    public int compare(Column a, Column b) {
      return a.compareTo(b);
    }
  };

  public static Object validate(Object value, Class type) {
    if (value == null) {
      return null;
    } else if (type.isInstance(value)) {
      return value;
    } else {
      throw new ClassCastException
        (value.getClass() + " cannot be cast to " + type);
    }
  }

  public static int compare(Object left,
                            Object right,
                            Comparator comparator)
  {
    if (left == right) {
      return 0;
    } else if (left == Dummy) {
      return -1;
    } else if (right == Dummy) {
      return 1;
    } else {
      return comparator.compare(left, right);
    }
  }

  public static boolean equal(Object left,
                              Object right,
                              Comparator comparator)
  {
    return comparator.compare(left, right) == 0;
  }

  public static boolean equal(Object left,
                              Object right)
  {
    return left == right || (left != null && left.equals(right));
  }

  public static int compare(Interval left,
                            Interval right,
                            Comparator comparator)
  {
    int leftHighRightHigh = compare
      (left.high, true, right.high, true, comparator);

    if (leftHighRightHigh > 0) {
      int leftLowRightHigh = compare
        (left.low, false, right.high, true, comparator);

      if (leftLowRightHigh < 0
          || left.lowBoundType == BoundType.Inclusive
          || right.lowBoundType == BoundType.Inclusive)
      {
        return 1;
      } else {
        return 2;
      }
    } else if (leftHighRightHigh < 0) {
      int rightLowLeftHigh = compare
        (right.low, false, left.high, true, comparator);

      if (rightLowLeftHigh < 0
          || right.lowBoundType == BoundType.Inclusive
          || left.lowBoundType == BoundType.Inclusive)
      {
        return -1;
      } else {
        return -2;
      }
    } else {
      if (left.highBoundType == BoundType.Inclusive) {
        if (right.highBoundType == BoundType.Inclusive) {
          return 0;
        } else {
          return 1;
        }
      } else if (right.highBoundType == BoundType.Inclusive) {
        return -1;
      } else {
        return 0;
      }
    }
  }

  public static int compare(Object left,
                            boolean leftHigh,
                            Object right,
                            boolean rightHigh,
                            Comparator comparator)
  {
    if (left == Undefined) {
      if (right == Undefined) {
        if (leftHigh) {
          if (rightHigh) {
            return 0;
          } else {
            return 1;
          }
        } else if (rightHigh) {
          return -1;
        } else {
          return 0;
        }
      } else if (leftHigh) {
        return 1;
      } else {
        return -1;
      }
    } else if (right == Undefined) {
      if (rightHigh) {
        return -1;
      } else {
        return 1;
      }
    } else {
      return compare(left, right, comparator);
    }
  }

  public static int compare(Object left,
                            Object right,
                            boolean rightHigh,
                            Comparator comparator)
  {
    if (right == Undefined) {
      if (rightHigh) {
        return -1;
      } else {
        return 1;
      }
    } else {
      return compare(left, right, comparator);
    }
  }

  public static int compare(Object left,
                            Object right,
                            BoundType rightBoundType,
                            boolean rightHigh,
                            Comparator comparator)
  {
    int difference = compare(left, right, rightHigh, comparator);
    if (difference == 0) {
      if (rightBoundType == BoundType.Exclusive) {
        if (rightHigh) {
          return 1;
        } else {
          return -1;
        }
      } else {
        return 0;
      }
    } else {
      return difference;
    }
  }
}

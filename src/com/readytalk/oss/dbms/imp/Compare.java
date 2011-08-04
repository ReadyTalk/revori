package com.readytalk.oss.dbms.imp;

import com.readytalk.oss.dbms.imp.Interval.BoundType;

class Compare {
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

  public static int compare(Comparable left,
                            Comparable right)
  {
    if (left == right) {
      return 0;
    } else if (left == Dummy) {
      return -1;
    } else if (right == Dummy) {
      return 1;
    } else {
      return ((Comparable) left).compareTo(right);
    }
  }

  public static boolean equal(Object left,
                              Object right)
  {
    return left == right || (left != null && left.equals(right));
  }

  public static int compare(Interval left,
                            Interval right)
  {
    int leftHighRightHigh = compare(left.high, true, right.high, true);
    if (leftHighRightHigh > 0) {
      int leftLowRightHigh = compare(left.low, false, right.high, true);
      if (leftLowRightHigh < 0
          || left.lowBoundType == BoundType.Inclusive
          || right.lowBoundType == BoundType.Inclusive)
      {
        return 1;
      } else {
        return 2;
      }
    } else if (leftHighRightHigh < 0) {
      int rightLowLeftHigh = compare(right.low, false, left.high, true);
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

  public static int compare(Comparable left,
                            boolean leftHigh,
                            Comparable right,
                            boolean rightHigh)
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
      return compare(left, right);
    }
  }

  public static int compare(Comparable left,
                            Comparable right,
                            boolean rightHigh)
  {
    if (right == Undefined) {
      if (rightHigh) {
        return -1;
      } else {
        return 1;
      }
    } else {
      return compare(left, right);
    }
  }

  public static int compare(Comparable left,
                            Comparable right,
                            BoundType rightBoundType,
                            boolean rightHigh)
  {
    int difference = compare(left, right, rightHigh);
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

package com.readytalk.oss.dbms.imp;

import com.readytalk.oss.dbms.BinaryOperation;
import com.readytalk.oss.dbms.ExpressionVisitor;
import com.readytalk.oss.dbms.OperationClass;
import com.readytalk.oss.dbms.imp.Interval.BoundType;

class ComparisonAdapter implements ExpressionAdapter {
  public final BinaryOperation.Type type;
  public final ExpressionAdapter left;
  public final ExpressionAdapter right;
    
  public ComparisonAdapter(BinaryOperation.Type type,
                           ExpressionAdapter left,
                           ExpressionAdapter right)
  {
    this.type = type;
    this.left = left;
    this.right = right;

    if (type.operationClass() != OperationClass.Comparison) {
      throw new IllegalArgumentException();
    }
  }

  public void visit(ExpressionAdapterVisitor visitor) {
    left.visit(visitor);
    right.visit(visitor);
  }

  public Object evaluate(boolean convertDummyToNull) {
    Object leftValue = left.evaluate(convertDummyToNull);
    Object rightValue = right.evaluate(convertDummyToNull);

    // System.out.println("evaluate " + type + " left " + leftValue + " right " + rightValue);

    if (leftValue == null || rightValue == null) {
      return false;
    } else if (leftValue == Compare.Undefined
               || rightValue == Compare.Undefined)
    {
      return Compare.Undefined;
    } else {
      switch (type) {
      case Equal:
        return leftValue.equals(rightValue);

      case NotEqual:
        return ! leftValue.equals(rightValue);

      case GreaterThan:
        return ((Comparable) leftValue).compareTo(rightValue) > 0;

      case GreaterThanOrEqual:
        return ((Comparable) leftValue).compareTo(rightValue) >= 0;

      case LessThan:
        return ((Comparable) leftValue).compareTo(rightValue) < 0;

      case LessThanOrEqual:
        return ((Comparable) leftValue).compareTo(rightValue) <= 0;

      default: throw new RuntimeException
          ("unexpected comparison type: " + type);
      }
    }
  }

  public Scan makeScan(ColumnReferenceAdapter reference) {
    if (left == reference) {
      if (right.evaluate(false) == Compare.Undefined) {
        return UnknownScan.Instance;
      } else {
        switch (type) {
        case Equal:
          return new IntervalScan(right, right);

        case NotEqual:
          return IntervalScan.Unbounded;

        case GreaterThan:
          return new IntervalScan
            (right, BoundType.Exclusive,
             ConstantAdapter.Undefined, BoundType.Inclusive);

        case GreaterThanOrEqual:
          return new IntervalScan(right, ConstantAdapter.Undefined);

        case LessThan:
          return new IntervalScan
            (ConstantAdapter.Undefined, BoundType.Inclusive,
             right, BoundType.Exclusive);

        case LessThanOrEqual:
          return new IntervalScan(ConstantAdapter.Undefined, right);

        default: throw new RuntimeException
            ("unexpected comparison type: " + type);
        }
      }
    } else if (right == reference) {
      if (left.evaluate(false) == Compare.Undefined) {
        return UnknownScan.Instance;
      } else {
        switch (type) {
        case Equal:
          return new IntervalScan(left, left);

        case NotEqual:
          return IntervalScan.Unbounded;

        case GreaterThan:
          return new IntervalScan
            (ConstantAdapter.Undefined, BoundType.Inclusive,
             left, BoundType.Exclusive);

        case GreaterThanOrEqual:
          return new IntervalScan(ConstantAdapter.Undefined, left);

        case LessThan:
          return new IntervalScan
            (left, BoundType.Exclusive,
             ConstantAdapter.Undefined, BoundType.Inclusive);

        case LessThanOrEqual:
          return new IntervalScan(left, ConstantAdapter.Undefined);

        default: throw new RuntimeException
            ("unexpected comparison type: " + type);
        }
      }
    } else {
      return IntervalScan.Unbounded;
    }
  }

  public Class type() {
    return Boolean.class;
  }
}

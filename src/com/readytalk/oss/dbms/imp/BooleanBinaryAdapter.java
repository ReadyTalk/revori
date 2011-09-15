package com.readytalk.oss.dbms.imp;

import com.readytalk.oss.dbms.BinaryOperation;
import com.readytalk.oss.dbms.ExpressionVisitor;
import com.readytalk.oss.dbms.OperationClass;

class BooleanBinaryAdapter implements ExpressionAdapter {
  public final BinaryOperation.Type type;
  public final ExpressionAdapter left;
  public final ExpressionAdapter right;
    
  public BooleanBinaryAdapter(BinaryOperation.Type type,
                              ExpressionAdapter left,
                              ExpressionAdapter right)
  {
    this.type = type;
    this.left = left;
    this.right = right;

    if (type.operationClass() != OperationClass.Boolean) {
      throw new IllegalArgumentException();
    }

    if (left.type() != Boolean.class) {
      throw new ClassCastException
        (left.type() + " cannot be cast to " + Boolean.class);
    } else if (right.type() != Boolean.class) {
      throw new ClassCastException
        (right.type() + " cannot be cast to " + Boolean.class);
    }
  }

  public void visit(ExpressionAdapterVisitor visitor) {
    left.visit(visitor);
    right.visit(visitor);
  }

  public Object evaluate(boolean convertDummyToNull) {
    Object leftValue = left.evaluate(convertDummyToNull);
    Object rightValue = right.evaluate(convertDummyToNull);

    if (leftValue == null || rightValue == null) {
      return false;
    } else if (leftValue == Compare.Undefined
               || rightValue == Compare.Undefined)
    {
      return Compare.Undefined;
    } else {
      switch (type) {
      case And:
        return leftValue == Boolean.TRUE && rightValue == Boolean.TRUE;

      case Or:
        return leftValue == Boolean.TRUE || rightValue == Boolean.TRUE;

      default: throw new RuntimeException
          ("unexpected comparison type: " + type);
      }
    }
  }

  public Scan makeScan(ColumnReferenceAdapter reference) {
    Scan leftScan = left.makeScan(reference);
    Scan rightScan = right.makeScan(reference);

    switch (type) {
    case And:
      return new IntersectionScan
        (leftScan, rightScan, reference.column.comparator);

    case Or:
      return new UnionScan
        (leftScan, rightScan, reference.column.comparator);

    default: throw new RuntimeException
        ("unexpected comparison type: " + type);
    }
  }

  public Class type() {
    return Boolean.class;
  }
}

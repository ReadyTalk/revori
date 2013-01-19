/* Copyright (c) 2010-2012, Revori Contributors

   Permission to use, copy, modify, and/or distribute this software
   for any purpose with or without fee is hereby granted, provided
   that the above copyright notice and this permission notice appear
   in all copies. */

package com.readytalk.revori.imp;

import com.readytalk.revori.BinaryOperation;
import com.readytalk.revori.OperationClass;

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

  public Class<Boolean> type() {
    return Boolean.class;
  }
}

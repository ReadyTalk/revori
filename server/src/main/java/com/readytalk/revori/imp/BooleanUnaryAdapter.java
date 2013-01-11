/* Copyright (c) 2010-2012, Revori Contributors

   Permission to use, copy, modify, and/or distribute this software
   for any purpose with or without fee is hereby granted, provided
   that the above copyright notice and this permission notice appear
   in all copies. */

package com.readytalk.revori.imp;

import com.readytalk.revori.OperationClass;
import com.readytalk.revori.UnaryOperation;

class BooleanUnaryAdapter implements ExpressionAdapter {
  public final UnaryOperation.Type type;
  public final ExpressionAdapter operand;
    
  public BooleanUnaryAdapter(UnaryOperation.Type type,
                             ExpressionAdapter operand)
  {
    this.type = type;
    this.operand = operand;

    if (type.operationClass() != OperationClass.Boolean) {
      throw new IllegalArgumentException();
    }
  }

  public void visit(ExpressionAdapterVisitor visitor) {
    operand.visit(visitor);
  }

  public Object evaluate(boolean convertDummyToNull) {
    Object value = operand.evaluate(convertDummyToNull);

    if (value == null) {
      return type == UnaryOperation.Type.IsNull;
    } else if (value == Compare.Undefined) {
      return Compare.Undefined;
    } else {
      switch (type) {
      case Not:
        return value != Boolean.TRUE;
      case IsNull:
        return false; // the true case is handled above

      default: throw new RuntimeException
          ("unexpected UnaryOperation type: " + type);
      }
    }
  }

  public Scan makeScan(ColumnReferenceAdapter reference) {
    switch (type) {
    case IsNull:
      return IntervalScan.Unbounded;

    case Not:
      return new NegationScan(operand.makeScan(reference));

    default: throw new RuntimeException
        ("unexpected comparison type: " + type);
    }
  }

  public Class type() {
    return Boolean.class;
  }
}

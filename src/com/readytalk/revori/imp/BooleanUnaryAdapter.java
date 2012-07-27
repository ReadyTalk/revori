package com.readytalk.revori.imp;

import com.readytalk.revori.UnaryOperation;
import com.readytalk.revori.ExpressionVisitor;
import com.readytalk.revori.OperationClass;

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
      return false;
    } else if (value == Compare.Undefined) {
      return Compare.Undefined;
    } else {
      switch (type) {
      case Not:
        return value != Boolean.TRUE;

      default: throw new RuntimeException
          ("unexpected comparison type: " + type);
      }
    }
  }

  public Scan makeScan(ColumnReferenceAdapter reference) {
    Scan scan = operand.makeScan(reference);

    switch (type) {
    case Not:
      return new NegationScan(scan);

    default: throw new RuntimeException
        ("unexpected comparison type: " + type);
    }
  }

  public Class type() {
    return Boolean.class;
  }
}

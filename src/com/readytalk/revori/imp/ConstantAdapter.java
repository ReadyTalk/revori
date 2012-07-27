/* Copyright (c) 2010-2012, Revori Contributors

   Permission to use, copy, modify, and/or distribute this software
   for any purpose with or without fee is hereby granted, provided
   that the above copyright notice and this permission notice appear
   in all copies. */

package com.readytalk.revori.imp;

class ConstantAdapter implements ExpressionAdapter {
  public static final ConstantAdapter True = new ConstantAdapter(true);

  public static final ConstantAdapter Undefined = new ConstantAdapter
    (Compare.Undefined);

  public static final ConstantAdapter Dummy = new ConstantAdapter
    (Compare.Dummy);

  public final Object value;
    
  public ConstantAdapter(Object value) {
    this.value = value;
  }

  public Object evaluate(boolean convertDummyToNull) {
    return value;
  }

  public Scan makeScan(ColumnReferenceAdapter reference) {
    if (Boolean.TRUE.equals(value)) {
      return IntervalScan.Unbounded;
    } else if (Boolean.FALSE.equals(value)) {
      return IntervalScan.Empty;
    } else {
      throw new UnsupportedOperationException();
    }
  }

  public void visit(ExpressionAdapterVisitor visitor) {
    visitor.visit(this);
  }

  public Class type() {
    return value == null ? null : value.getClass();
  }
}

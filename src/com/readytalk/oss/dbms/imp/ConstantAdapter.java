package com.readytalk.oss.dbms.imp;

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

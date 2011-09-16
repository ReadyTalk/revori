package com.readytalk.oss.dbms.imp;

import com.readytalk.oss.dbms.ExpressionVisitor;
import com.readytalk.oss.dbms.Aggregate;

class AggregateAdapter implements ExpressionAdapter {
  public final Aggregate aggregate;
  public Object value = Compare.Undefined;

  public AggregateAdapter(Aggregate aggregate) {
    this.aggregate = aggregate;
  }

  public void add(Object accumulation, Object ... increment) {
    value = aggregate.function.add
      (accumulation == Node.Null ? null : accumulation, increment);

    // System.out.println("add " + accumulation + " " + java.util.Arrays.toString(increment) + ": " + value);
  }

  public void subtract(Object accumulation, Object ... increment) {
    value = aggregate.function.subtract
      (accumulation == Node.Null ? null : accumulation, increment);

    // System.out.println("subtract " + accumulation + " " + java.util.Arrays.toString(increment) + ": " + value);
  }

  public void visit(ExpressionAdapterVisitor visitor) {
    visitor.visit(this);
  }

  public Object evaluate(boolean convertDummyToNull) {
    return value;
  }

  public Scan makeScan(ColumnReferenceAdapter reference) {
    throw new UnsupportedOperationException();
  }

  public Class type() {
    return aggregate.type;
  }
}

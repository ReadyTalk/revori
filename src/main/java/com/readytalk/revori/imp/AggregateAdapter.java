/* Copyright (c) 2010-2012, Revori Contributors

   Permission to use, copy, modify, and/or distribute this software
   for any purpose with or without fee is hereby granted, provided
   that the above copyright notice and this permission notice appear
   in all copies. */

package com.readytalk.revori.imp;

import javax.annotation.concurrent.NotThreadSafe;

import com.readytalk.revori.ExpressionVisitor;
import com.readytalk.revori.Aggregate;

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

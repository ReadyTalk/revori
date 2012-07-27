/* Copyright (c) 2010-2012, Revori Contributors

   Permission to use, copy, modify, and/or distribute this software
   for any purpose with or without fee is hereby granted, provided
   that the above copyright notice and this permission notice appear
   in all copies. */

package com.readytalk.revori;

import static com.readytalk.revori.imp.Compare.equal;

import java.util.Collections;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Expression representing a constant value.
 */
public class Constant implements Expression {
  private static final AtomicInteger nextOrder = new AtomicInteger();

  /**
   * The constant value specified when this instance was defined.
   */
  public final Object value;

  public final int order;

  /**
   * Defines a constant value as an expression.  Care should be taken
   * to use only immutable values as constants; use of an mutable
   * value may result in unpredictable behavior.
   */
  public Constant(Object value) {
    this.value = value;
    this.order = nextOrder.getAndIncrement();
  }

  /**
   * {@inheritDoc}
   */
  public void visit(ExpressionVisitor visitor) {
    visitor.visit(this);
  }

  /**
   * {@inheritDoc}
   */
  public Class typeConstraint() {
    return null;
  }

  /**
   * {@inheritDoc}
   */
  public Iterable<Expression> children() {
    return Collections.emptyList();
  }

  public int compareTo(Expression e) {
    if (this == e) return 0;

    if (e instanceof Constant) {
      Constant o = (Constant) e;

      if (equal(value, o.value)) {
        return 0;
      } else {
        return order - o.order;
      }
    } else {
      return getClass().getName().compareTo(e.getClass().getName());
    }
  }

  public boolean equals(Object o) {
    return o instanceof Constant && compareTo((Constant) o) == 0;
  }
}

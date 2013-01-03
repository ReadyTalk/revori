/* Copyright (c) 2010-2012, Revori Contributors

   Permission to use, copy, modify, and/or distribute this software
   for any purpose with or without fee is hereby granted, provided
   that the above copyright notice and this permission notice appear
   in all copies. */

package com.readytalk.revori;

import java.util.Collections;
import java.util.concurrent.atomic.AtomicInteger;

import javax.annotation.concurrent.ThreadSafe;


/**
 * Type representing an expression whose value will be supplied
 * separately when the expression is evaluated.
 */
@ThreadSafe
public class Parameter implements Expression {
  private static final AtomicInteger nextOrder = new AtomicInteger();

  public final int order;

  /**
   * Defines a placeholder as an expression for use when defining
   * query and update templates.  The actual value of the expression
   * will be specified when the template is combined with a list of
   * parameter values to define a query or patch.
   */
  public Parameter() {
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

    if (e instanceof Parameter) {
      Parameter o = (Parameter) e;

      return order - o.order;
    } else {
      return getClass().getName().compareTo(e.getClass().getName());
    }
  }

  public boolean equals(Object o) {
    return o instanceof Parameter && compareTo((Parameter) o) == 0;
  }
}

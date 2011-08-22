package com.readytalk.oss.dbms;

import java.util.Collections;
import java.util.concurrent.atomic.AtomicInteger;


/**
 * Type representing an expression whose value will be supplied
 * separately when the expression is evaluated.
 */
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

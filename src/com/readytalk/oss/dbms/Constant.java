package com.readytalk.oss.dbms;

/**
 * Expression representing a constant value.
 */
public class Constant implements Expression {
  /**
   * The constant value specified when this instance was defined.
   */
  public final Object value;

  /**
   * Defines a constant value as an expression.  Care should be taken
   * to use only immutable values as constants; use of an mutable
   * value may result in unpredictable behavior.
   */
  public Constant(Object value) {
    this.value = value;
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
}

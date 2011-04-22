package com.readytalk.oss.dbms;

/**
 * Type representing an expression whose value will be supplied
 * separately when the expression is evaluated.
 */
public class Parameter implements Expression {
  /**
   * Defines a placeholder as an expression for use when defining
   * query and update templates.  The actual value of the expression
   * will be specified when the template is combined with a list of
   * parameter values to define a query or patch.
   */
  public Parameter() { }

  /**
   * {@inheritDoc}
   */
  public void visit(ExpressionVisitor visitor) {
    visitor.visit(this);
  }
}

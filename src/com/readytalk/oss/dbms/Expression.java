package com.readytalk.oss.dbms;

/**
 * Type representing an expression (e.g. constant, column reference,
 * or query predicate) for use in queries and updates.
 */
public interface Expression {
  /**
   * Visit this expression and any subexpressions by calling
   * ExpressionVisitor.visit(Expression) with each.
   */
  public void visit(ExpressionVisitor visitor);
}

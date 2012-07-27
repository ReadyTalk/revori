package com.readytalk.revori;

/**
 * Type representing an expression (e.g. constant, column reference,
 * or query predicate) for use in queries and updates.
 */
public interface Expression extends Comparable<Expression> {
  /**
   * Visit this expression and any subexpressions by calling
   * ExpressionVisitor.visit(Expression) with each.
   */
  public void visit(ExpressionVisitor visitor);

  public Class typeConstraint();

  public Iterable<Expression> children();
}

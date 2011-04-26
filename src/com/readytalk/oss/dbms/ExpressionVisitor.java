package com.readytalk.oss.dbms;

/**
 * Implementations of this interface may be used to visit each node in
 * an expression graph.
 */
public interface ExpressionVisitor {
  /**
   * Visit the specified expression.
   */
  public void visit(Expression e);
}

/* Copyright (c) 2010-2012, Revori Contributors

   Permission to use, copy, modify, and/or distribute this software
   for any purpose with or without fee is hereby granted, provided
   that the above copyright notice and this permission notice appear
   in all copies. */

package com.readytalk.revori;

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

/* Copyright (c) 2010-2012, Revori Contributors

   Permission to use, copy, modify, and/or distribute this software
   for any purpose with or without fee is hereby granted, provided
   that the above copyright notice and this permission notice appear
   in all copies. */

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

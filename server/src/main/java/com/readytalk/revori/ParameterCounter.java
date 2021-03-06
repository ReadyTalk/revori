/* Copyright (c) 2010-2012, Revori Contributors

   Permission to use, copy, modify, and/or distribute this software
   for any purpose with or without fee is hereby granted, provided
   that the above copyright notice and this permission notice appear
   in all copies. */

package com.readytalk.revori;

import java.util.Collection;
import java.util.Set;

import javax.annotation.concurrent.NotThreadSafe;

import com.google.common.collect.Sets;

/**
 * This is an expression visitor to count the number of unique
 * parameter instances in a set of expressions and their
 * subexpressions.
 */
@NotThreadSafe
public class ParameterCounter implements ExpressionVisitor {
  private final Set<Parameter> parameters = Sets.newHashSet();
  private int count;

  /**
   * Visit the specified expression, incrementing the count of
   * parameters seen if the expression is an instance of Parameter and
   * has not been visited already.
   *
   * @throws IllegalArgumentException if the specified expression is a
   * parameter which has already been visited by this ParameterCounter.
   */
  public void visit(Expression e) {
    if (e instanceof Parameter) {
      Parameter pe = (Parameter) e;
      if (parameters.contains(pe)) {
        throw new IllegalArgumentException
          ("duplicate parameter expression");
      } else {
        parameters.add(pe);
        ++ count;
      }
    }
  }

  /**
   * Returns the current count of unique parameters visited.
   */
  public int count() {
    return count;
  }

  /**
   * Returns the number of unique parameter instances in the specified
   * list of expressions and their subexpressions.
   */
  public static int countParameters(Collection<Expression> expressions) {
    ParameterCounter counter = new ParameterCounter();
    for (Expression e: expressions) {
      e.visit(counter);
    }
    return counter.count;
  }
}

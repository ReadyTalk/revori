/* Copyright (c) 2010-2012, Revori Contributors

   Permission to use, copy, modify, and/or distribute this software
   for any purpose with or without fee is hereby granted, provided
   that the above copyright notice and this permission notice appear
   in all copies. */

package com.readytalk.revori;

import static com.readytalk.revori.imp.Compare.equal;
import static com.readytalk.revori.util.Util.compare;

import java.util.Collections;
import java.util.List;
import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicInteger;

public class Aggregate<T> implements Expression {
  private static final AtomicInteger nextOrder = new AtomicInteger();

  public final Class<T> type;
  public final Foldable<T> function;
  public final List<Expression> expressions;
  public final int order;

  public Aggregate(Class<T> type,
                   Foldable<T> function,
                   List<Expression> expressions)
  {
    this.type = type;
    this.function = function;
    this.expressions = Collections.unmodifiableList
      (new ArrayList<Expression>(expressions));

    this.order = nextOrder.getAndIncrement();

    for (Expression e: this.expressions) {
      e.visit(new ExpressionVisitor() {
          public void visit(Expression e) {
            if (e instanceof Aggregate) {
              throw new IllegalArgumentException
                ("aggregates cannot be nested");
            }
          }
        });
    }
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
  public Class<T> typeConstraint() {
    return type;
  }

  /**
   * {@inheritDoc}
   */
  public Iterable<Expression> children() {
    return Collections.emptyList();
  }

  public int compareTo(Expression e) {
    if (this == e) return 0;

    if (e instanceof Aggregate) {
      Aggregate o = (Aggregate) e;

      int d = type.getName().compareTo(o.type.getName());
      if (d != 0) {
        return d;
      }
      
      d = compare(expressions, o.expressions);
      if (d != 0) {
        return d;
      }

      if (equal(function, o.function)) {
        return 0;
      } else {
        return order - o.order;
      }
    } else {
      return getClass().getName().compareTo(e.getClass().getName());
    }
  }

  public boolean equals(Object o) {
    return o instanceof Aggregate && compareTo((Aggregate) o) == 0;
  }
}

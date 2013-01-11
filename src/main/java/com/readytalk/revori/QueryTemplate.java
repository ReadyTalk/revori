/* Copyright (c) 2010-2012, Revori Contributors

   Permission to use, copy, modify, and/or distribute this software
   for any purpose with or without fee is hereby granted, provided
   that the above copyright notice and this permission notice appear
   in all copies. */

package com.readytalk.revori;

import static com.readytalk.revori.util.Util.append;
import static com.readytalk.revori.util.Util.compare;
import static com.readytalk.revori.util.Util.union;

import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Set;
import java.util.SortedSet;

import javax.annotation.concurrent.NotThreadSafe;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSortedSet;

/**
 * Class representing a template for queries which is not bound to any
 * specific parameters, analogous to a prepared statement in JDBC.
 */
@NotThreadSafe
public final class QueryTemplate implements Comparable<QueryTemplate> {
  /**
   * The number of parameter expressions present in the the expression
   * list and test expression, including any parameters referenced
   * directly or indirectly by those top-level expressions.
   */
  public final int parameterCount;

  /**
   * An immutable list of expressions to be evaluated when the query
   * is executed.
   */
  public final List<Expression> expressions;

  /**
   * The source from which any column references in the expression
   * list should be resolved.
   */
  public final Source source;

  /**
   * An expression which, when resolved, determines whether a row
   * should be included in the query result.
   */
  public final Expression test;

  public final SortedSet<Expression> groupingExpressions;

  public final List<OrderExpression> orderByExpressions;

  public final boolean hasAggregates;

  /**
   * Defines a query template with the specified expressions to be
   * evaluated, the source from which any column references in the
   * expression list should be resolved, and the test for selecting
   * from that source.
   */
  public QueryTemplate(List<Expression> expressions,
                       Source source,
                       Expression test)
  {
    this(expressions, source, test, Collections.<Expression>emptySet());
  }

  public QueryTemplate(List<Expression> expressions,
                       Source source,
                       Expression test,
                       Set<Expression> groupingExpressions)
  {
    this(expressions, source, test, groupingExpressions,
         Collections.<OrderExpression>emptyList());
  }

  public QueryTemplate(List<Expression> expressions,
                       Source source,
                       Expression test,
                       Set<Expression> groupingExpressions,
                       List<OrderExpression> orderByExpressions)
  {
    this.expressions = ImmutableList.copyOf(expressions);
    this.groupingExpressions = ImmutableSortedSet.copyOf(groupingExpressions);
    this.orderByExpressions = ImmutableList.copyOf(orderByExpressions);
    
    this.source = source;
    this.test = test;
    this.parameterCount = ParameterCounter.countParameters
      (union(append(this.expressions, test), groupingExpressions));

    final boolean hasAggregates[] = new boolean[1];
    ExpressionVisitor v = new ExpressionVisitor() {
        public void visit(Expression e) {
          if (e instanceof Aggregate) {
            hasAggregates[0] = true;
          }
        }
      };

    for (Expression e: this.expressions) {
      e.visit(v);
    }

    test.visit(v);

    this.hasAggregates = hasAggregates[0];

  }

  public int compareTo(QueryTemplate o) {
    if (this == o) return 0;

    int d = parameterCount - o.parameterCount;
    if (d != 0) {
      return d;
    }

    d = compare(expressions, o.expressions);
    if (d != 0) {
      return d;
    }

    d = source.compareTo(o.source);
    if (d != 0) {
      return d;
    }

    d = test.compareTo(o.test);
    if (d != 0) {
      return d;
    }

    return compare(groupingExpressions, o.groupingExpressions);
  }

  public boolean equals(Object o) {
    return o instanceof QueryTemplate && compareTo((QueryTemplate) o) == 0;
  }

  public static class OrderExpression {
    public final Expression expression;
    public final Comparator comparator;

    public OrderExpression(Expression expression, Comparator comparator) {
      this.expression = expression;
      this.comparator = comparator;
    }
  }
}

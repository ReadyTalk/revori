package com.readytalk.revori;

import static com.readytalk.revori.util.Util.compare;
import static com.readytalk.revori.util.Util.append;
import static com.readytalk.revori.util.Util.union;

import java.util.Comparator;
import java.util.Collections;
import java.util.List;
import java.util.ArrayList;
import java.util.Set;
import java.util.TreeSet;

/**
 * Class representing a template for queries which is not bound to any
 * specific parameters, analogous to a prepared statement in JDBC.
 */
public final class QueryTemplate implements Comparable<QueryTemplate> {
  private static final int AggregateFlag = 1 << 0;
  private static final int ColumnReferenceFlag = 1 << 1;

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

  public final Set<Expression> groupingExpressions;

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
    this.expressions = Collections.unmodifiableList
      (new ArrayList<Expression>(expressions));
    this.source = source;
    this.test = test;
    this.parameterCount = ParameterCounter.countParameters
      (union(append(this.expressions, test), groupingExpressions));

    groupingExpressions = new TreeSet(groupingExpressions);
    int mask = 0;
    final Set<Expression> basics = new TreeSet();
    for (Expression e: this.expressions) {
      mask |= addBasics(basics, e);
    }

    mask |= addBasics(basics, test);

    if (((mask & AggregateFlag) != 0) || (! groupingExpressions.isEmpty())) {
      groupingExpressions.addAll(basics);
    }

    this.hasAggregates = (mask & AggregateFlag) != 0;

    this.groupingExpressions = Collections.unmodifiableSet
      (groupingExpressions);

    this.orderByExpressions = Collections.unmodifiableList
      (orderByExpressions);
  }

  private static int addBasics(Set<Expression> basics,
                               Expression expression)
  {
    int mask = 0;
    for (Expression e: expression.children()) {
      mask |= addBasics(basics, e);
    }

    if (expression instanceof Aggregate) {
      mask |= AggregateFlag;
    }

    if (expression instanceof ColumnReference) {
      mask |= ColumnReferenceFlag;
    }

    if ((mask & AggregateFlag) == 0 && (mask & ColumnReferenceFlag) != 0) {
      basics.add(expression);
    }

    return mask;
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

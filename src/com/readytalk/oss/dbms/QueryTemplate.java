package com.readytalk.oss.dbms;

import static com.readytalk.oss.dbms.util.Util.compare;
import static com.readytalk.oss.dbms.util.Util.append;
import static com.readytalk.oss.dbms.util.Util.union;

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
    this(expressions, source, test,
         (Set<Expression>) (Set) Collections.emptySet());
  }

  public QueryTemplate(List<Expression> expressions,
                       Source source,
                       Expression test,
                       Set<Expression> groupingExpressions)
  {
    this.expressions = Collections.unmodifiableList
      (new ArrayList(expressions));
    this.source = source;
    this.test = test;
    this.parameterCount = ParameterCounter.countParameters
      (union(append(this.expressions, test), groupingExpressions));

    groupingExpressions = new TreeSet(groupingExpressions);
    boolean allBasic = true;
    final Set<Expression> basics = new TreeSet();
    for (Expression e: this.expressions) {
      if (! addBasics(basics, e)) {
        allBasic = false;
      }
    }

    if ((! allBasic) || (! groupingExpressions.isEmpty())) {
      groupingExpressions.addAll(basics);
    }

    this.hasAggregates = ! allBasic;

    this.groupingExpressions = Collections.unmodifiableSet
      (groupingExpressions);
  }

  private static boolean addBasics(Set<Expression> basics,
                                   Expression expression)
  {
    boolean basic = true;
    for (Expression e: expression.children()) {
      if (! addBasics(basics, e)) {
        basic = false;
      }
    }

    if (basic && (! (expression instanceof Aggregate))) {
      basics.add(expression);
      return true;
    } else {
      return false;
    }
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
}

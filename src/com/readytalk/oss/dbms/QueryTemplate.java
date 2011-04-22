package com.readytalk.oss.dbms;

import static com.readytalk.oss.dbms.util.Util.append;

import java.util.Collections;
import java.util.List;
import java.util.ArrayList;

/**
 * Class representing a template for queries which is not bound to any
 * specific parameters, analogous to a prepared statement in JDBC.
 */
public final class QueryTemplate {
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
    this.expressions = Collections.unmodifiableList
      (new ArrayList(expressions));
    this.source = source;
    this.test = test;
    this.parameterCount = ParameterCounter.countParameters
      (append(this.expressions, test));    
  }
}

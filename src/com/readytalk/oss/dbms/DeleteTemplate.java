package com.readytalk.oss.dbms;

import static com.readytalk.oss.dbms.util.Util.list;

/**
 * Class representing a template for deletes which is not bound to any
 * specific parameters, analogous to a prepared statement in JDBC.
 */
public final class DeleteTemplate implements PatchTemplate {
  private final int parameterCount;

  /**
   * The table from which rows are to be removed.
   */
  public final TableReference tableReference;

  /**
   * The expression which, when evaluated, determines whether a given
   * row should be removed.
   */
  public Expression test;

  /**
   * Defines a patch template which represents a delete operation on
   * the specified table to be applied to rows satisfying the
   * specified test.
   */
  public DeleteTemplate(TableReference tableReference,
                        Expression test)
  {
    this.tableReference = tableReference;
    this.test = test;
    this.parameterCount = ParameterCounter.countParameters(list(test));
  }

  /**
   * {@inheritDoc}
   */
  public int parameterCount() {
    return parameterCount;
  }
}

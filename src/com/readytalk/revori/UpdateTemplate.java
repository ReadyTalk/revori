package com.readytalk.revori;

import static com.readytalk.revori.util.Util.append;

import java.util.Collections;
import java.util.List;
import java.util.ArrayList;

/**
 * Class representing a template for updates which is not bound to any
 * specific parameters, analogous to a prepared statement in JDBC.
 */
public final class UpdateTemplate implements PatchTemplate {
  private final int parameterCount;

  /**
   * The table in which rows are to be updated.
   */
  public final TableReference tableReference;

  /**
   * The expression which, when evaluated, determines whether a given
   * row should be updated.
   */
  public final Expression test;

  /**
   * An immutable list of columns for which values are to be updated.
   */
  public final List<Column<?>> columns;

  /**
   * An immutable list of expressions which, when resolved, yield the
   * values to be updated.
   */
  public final List<Expression> values;

  /**
   * Defines a patch template which represents an update operation on
   * the specified table to be applied to rows satisfying the
   * specified test.  The values to be updated are specified as two
   * ordered lists of equal length: a list of columns and a list of
   * expressions representing the values to be placed into those
   * columns.
   */
  public UpdateTemplate(TableReference tableReference,
                        Expression test,
                        List<Column<?>> columns,
                        List<Expression> values)
  {
    this.tableReference = tableReference;
    this.test = test;
    this.columns = Collections.unmodifiableList(new ArrayList<Column<?>>(columns));
    this.values = Collections.unmodifiableList(new ArrayList<Expression>(values));
    this.parameterCount = ParameterCounter.countParameters
      (append(this.values, test));

    if (this.columns.size() != this.values.size()) {
      throw new IllegalArgumentException
        ("column and value lists must be of equal length");
    }
  }

  /**
   * {@inheritDoc}
   */
  public int parameterCount() {
    return parameterCount;
  }
}

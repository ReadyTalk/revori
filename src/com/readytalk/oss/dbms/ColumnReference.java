package com.readytalk.oss.dbms;

import java.util.Collections;

/**
 * Type representing a specific reference to a column.  A query may
 * make multiple references to the same column (e.g. when joining a
 * table with itself), in which case it is useful to represent those
 * references unambiguously as separate objects.
 */
public final class ColumnReference implements Expression {
  /**
   * The table reference specified when this instance was defined.
   */
  public final TableReference tableReference;

  /**
   * The column specified when this instance was defined.
   */
  public final Column column;

  /**
   * Defines a column reference which may be used to unambiguously
   * refer to a column in a query or update.  Such a query or update
   * may refer to a column more than once, in which case one must
   * create multiple ColumnReferences to the same table.
   */
  public ColumnReference(TableReference tableReference,
                         Column column)
  {
    this.tableReference = tableReference;
    this.column = column;
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
  public Class typeConstraint() {
    return column.type;
  }

  /**
   * {@inheritDoc}
   */
  public Iterable<Expression> children() {
    return Collections.emptyList();
  }

  public int compareTo(Expression e) {
    if (this == e) return 0;

    if (e instanceof ColumnReference) {
      ColumnReference o = (ColumnReference) e;

      int d = column.compareTo(o.column);
      if (d != 0) {
        return d;
      }

      return tableReference.compareTo(o.tableReference);
    } else {
      return getClass().getName().compareTo(e.getClass().getName());
    }
  }

  public boolean equals(Object o) {
    return o instanceof ColumnReference && compareTo((ColumnReference) o) == 0;
  }
}

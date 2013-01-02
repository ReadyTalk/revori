/* Copyright (c) 2010-2012, Revori Contributors

   Permission to use, copy, modify, and/or distribute this software
   for any purpose with or without fee is hereby granted, provided
   that the above copyright notice and this permission notice appear
   in all copies. */

package com.readytalk.revori;

import java.util.Collections;
import java.util.List;
import java.util.ArrayList;
import java.util.Set;
import java.util.HashSet;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * Class representing a template for inserts which is not bound to any
 * specific parameters, analogous to a prepared statement in JDBC.
 */
@NotThreadSafe
public final class InsertTemplate implements PatchTemplate {
  private final int parameterCount;

  /**
   * The table into which rows are to be inserted.
   */
  public final Table table;

  /**
   * An immutable list of columns for which values are to be inserted.
   */
  public final List<Column<?>> columns;

  /**
   * An immutable list of expressions which, when resolved, yield the
   * values to be inserted.
   */
  public final List<Expression> values;

  /**
   * The strategy to use if there is already a matching primary key in
   * the table when the values are inserted.
   */
  public final DuplicateKeyResolution duplicateKeyResolution;

  /**
   * Defines a patch template which represents an insert operation on
   * the specified table.  The values to be inserted are specified as
   * two ordered lists of equal length: a list of columns and a list
   * of expressions representing the values to be placed into those
   * columns.
   *
   * If, when this template is applied, there is already row with a
   * matching primary key in the table, the MyDBMS will act according to
   * the specified DuplicateKeyResolution.
   */
  public InsertTemplate(Table table,
                        List<Column<?>> columns,
                        List<Expression> values,
                        DuplicateKeyResolution duplicateKeyResolution)
  {
    this.table = table;
    this.columns = Collections.unmodifiableList(new ArrayList<Column<?>>(columns));
    this.values = Collections.unmodifiableList(new ArrayList<Expression>(values));
    this.duplicateKeyResolution = duplicateKeyResolution;
    this.parameterCount = ParameterCounter.countParameters(this.values);

    if (this.columns.size() != this.values.size()) {
      throw new IllegalArgumentException
        ("column and value lists must be of equal length");
    }
    
    Set<?> set = new HashSet<Object>(table.primaryKey.columns);
    for (Object o: this.columns) {
      set.remove(o);
    }

    if (set.size() != 0) {
      throw new IllegalArgumentException
        ("not enough columns specified to satisfy primary key");
    }
  }

  /**
   * {@inheritDoc}
   */
  public int parameterCount() {
    return parameterCount;
  }
}

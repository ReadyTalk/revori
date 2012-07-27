/* Copyright (c) 2010-2012, Revori Contributors

   Permission to use, copy, modify, and/or distribute this software
   for any purpose with or without fee is hereby granted, provided
   that the above copyright notice and this permission notice appear
   in all copies. */

package com.readytalk.revori;

import static com.readytalk.revori.util.Util.list;

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

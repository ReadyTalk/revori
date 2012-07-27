/* Copyright (c) 2010-2012, Revori Contributors

   Permission to use, copy, modify, and/or distribute this software
   for any purpose with or without fee is hereby granted, provided
   that the above copyright notice and this permission notice appear
   in all copies. */

package com.readytalk.revori;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Type representing a specific reference to a table.  A query may
 * make multiple references to the same table (e.g. when joining a
 * table with itself), in which case it is useful to represent those
 * references unambiguously as separate objects.
 */
public class TableReference implements Source {
  private static final AtomicInteger nextOrder = new AtomicInteger();

  /**
   * The table specified when this instance was defined.
   */
  public final Table table;

  public final int order;

  /**
   * Defines a table reference which may be used to unambiguously
   * refer to a table in a query or update.  Such a query or update
   * may refer to a table more than once, in which case one must
   * create multiple TableReferences to the same table.
   */
  public TableReference(Table table) {
    this.table = table;
    this.order = nextOrder.getAndIncrement();
  }

  /**
   * {@inheritDoc}
   */
  public void visit(SourceVisitor visitor) {
    visitor.visit(this);
  }

  public int compareTo(Source s) {
    if (this == s) return 0;

    if (s instanceof TableReference) {
      TableReference o = (TableReference) s;

      return order - o.order;
    } else {
      return getClass().getName().compareTo(s.getClass().getName());
    }
  }

  public boolean equals(Object o) {
    return o instanceof TableReference && compareTo((TableReference) o) == 0;
  }
}

/* Copyright (c) 2010-2012, Revori Contributors

   Permission to use, copy, modify, and/or distribute this software
   for any purpose with or without fee is hereby granted, provided
   that the above copyright notice and this permission notice appear
   in all copies. */

package com.readytalk.revori;

import java.util.Comparator;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

/**
 * Class representing a column which may be used to identify an item
 * of interest in a query or update.
 */
@ThreadSafe
public final class Column<T> implements Comparable<Column<?>> {
  @GuardedBy("Column.class")
  private static long nextId = 1;

  public synchronized static String makeId() {
    return (nextId++) + "." + Column.class.getName() + ".id";
  }

  /**
   * The type which values stored to this column must be instances of.
   */
  public final Class<T> type;

  /**
   * The ID specified when this column was defined.
   */
  public final String id;

  public final Comparator comparator;

  /**
   * True if the column of this table should be replicated to other
   * servers.
   */
  public final boolean serializable;

  /**
   * Defines a column which is associated with the specified type and
   * ID.  The type specified here will be used for dynamic type
   * checking whenever a value is inserted or updated in this column
   * of a table; only values which are instances of the specified
   * class will be accepted.<p>
   */
  public Column(Class<T> type, String id) {
    this(type, id, Comparators.Ascending);
  }

  public Column(Class<T> type, String id, Comparator comparator) {
    this(type, id, comparator, true);
  }

  public Column(Class<T> type, String id, Comparator comparator,
                boolean serializable)
  {
    this.type = type;
    this.id = id;
    this.comparator = comparator;
    this.serializable = serializable;

    if (type == null) throw new NullPointerException();
    if (id == null) throw new NullPointerException();
    if (comparator == null) throw new NullPointerException();
  }

  /**
   * Defines a column which is associated with the specified type and
   * an automatically generated ID.<p>
   */
  public Column(Class<T> type) {
    this(type, makeId());
  }

  public int hashCode() {
    return id.hashCode();
  }

  public int compareTo(Column<?> o) {
    if (o == this) return 0;

    int d = id.compareTo(o.id);
    if (d != 0) {
      return d;
    }

    return type.getName().compareTo(o.type.getName());
  }

  /**
   * Returns true if and only if the specified object is a Column and its ID
   * and type are equal to those of this instance.
   */
  public boolean equals(Object o) {
    return o instanceof Column && compareTo((Column<?>) o) == 0;
  }
      
  public String toString() {
    return "column[" + id + " " + type.getName() + "]";
  }
}

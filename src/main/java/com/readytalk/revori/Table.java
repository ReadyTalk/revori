/* Copyright (c) 2010-2012, Revori Contributors

   Permission to use, copy, modify, and/or distribute this software
   for any purpose with or without fee is hereby granted, provided
   that the above copyright notice and this permission notice appear
   in all copies. */

package com.readytalk.revori;

import java.util.List;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Class representing a table.  Instances of this class do not hold
 * any data; they're used only to identify a collection of rows of
 * interest in a query or update.
 */
@ThreadSafe
public class Table implements Comparable<Table> {
  private static long nextId = 1;

  public synchronized static String makeId() {
    return (nextId++) + "." + Table.class.getName() + ".id";
  }

  /**
   * The primary key specified when this table was defined.
   */
  public final Index primaryKey;
  
  /**
   * The order, to provide an absolute order on tables independent of id
   */
  public final int order;

  /**
   * The ID specified when this table was defined.
   */
  public final String id;

  /**
   * True if the contents of this table should be replicated to other
   * servers.
   */
  public final boolean serializable;

  /**
   * Defines a table using the specified list of columns as the
   * primary key.<p>
   *
   * Instances of Table are considered equal if and only if their orders, IDs
   * and primary keys are equal.
   */
  public Table(List<Column<?>> primaryKey, String id) {
    this(primaryKey, id, 0);
  }

  /**
   * Defines a table using the specified list of columns as the
   * primary key.<p>
   *
   * Instances of Table are considered equal if and only if their orders, IDs
   * and primary keys are equal.
   */
  public Table(List<Column<?>> primaryKey, String id, int order) {
    this(primaryKey, id, order, true);
  }

  public Table(List<Column<?>> primaryKey, String id, int order,
               boolean serializable)
  {
    this.primaryKey = new Index(this, primaryKey);
    this.order = order;
    this.id = id;
    this.serializable = serializable;

    if (id == null) throw new NullPointerException();
  }

  /**
   * Defines a table using the specified list of columns as the
   * primary key.  The order is initialized to be greater than the order
   * of any of the <code>comesAfter</code> tables.<p>
   *
   * Instances of Table are considered equal if and only if their orders, IDs
   * and primary keys are equal.
   */
  public Table(List<Column<?>> primaryKey, String id, List<Table> comesAfter) {
    this(primaryKey, id, makeOrder(comesAfter));
  }

  /**
   * Defines a table using the specified primary key and an
   * automatically generated ID.<p>
   */
  public Table(List<Column<?>> primaryKey) {
    this(primaryKey, makeId());
  }

  private static int makeOrder(List<Table> comesAfter) {
    int o = 0;
    for(Table t : comesAfter) {
      if(t.order >= o) {
        o = t.order + 1;
      }
    }
    return o;
  }

  public int compareTo(Table o) {
    if (o == this) return 0;

    int d = order - o.order;
    if (d != 0) {
      return d;
    }

    d = id.compareTo(o.id);
    if (d != 0) {
      return d;
    }

    return primaryKey.compareColumns(o.primaryKey);
  }

  public int hashCode() {
    return id.hashCode();
  }

  /**
   * Returns true if and only if the specified object is a Table and
   * its ID and primaryKey are equal to those of this instance.
   */
  public boolean equals(Object o) {
    return o instanceof Table && compareTo((Table) o) == 0;
  }
      
  public String toString() {
    return "table[" + order + " " + id + " " + primaryKey.columns + "]";
  }
}

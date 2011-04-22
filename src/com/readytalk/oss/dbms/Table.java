package com.readytalk.oss.dbms;

import java.util.List;

/**
 * Class representing a table.  Instances of this class do not hold
 * any data; they're used only to identify a collection of rows of
 * interest in a query or update.
 */
public final class Table implements Comparable<Table> {
  /**
   * The primary key specified when this table was defined.
   */
  public final Index primaryKey;

  /**
   * The ID specified when this table was defined.
   */
  public final String id;

  /**
   * Defines a table using the specified list of columns as the
   * primary key.<p>
   *
   * Instances of Table are considered equal if and only if their IDs
   * and primary keys are equal.
   */
  public Table(List<Column> primaryKey, String id) {
    this.primaryKey = new Index(this, primaryKey);
    this.id = id;
  }

  public int compareTo(Table o) {
    int d = id.compareTo(o.id);
    if (d != 0) {
      return d;
    }

    return primaryKey.compareColumns(o.primaryKey);
  }

  /**
   * Returns true if and only if the specified object is a Table and
   * its ID and primaryKey are equal to those of this instance.
   */
  public boolean equals(Object o) {
    return o instanceof Table && compareTo((Table) o) == 0;
  }
      
  public String toString() {
    return "table[" + id + " " + primaryKey.columns + "]";
  }
}

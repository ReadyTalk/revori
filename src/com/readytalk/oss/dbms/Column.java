package com.readytalk.oss.dbms;

/**
 * Class representing a column which may be used to identify an item
 * of interest in a query or update.
 */
public final class Column implements Comparable<Column> {
  /**
   * The type which values stored to this column must be instances of.
   */
  public final Class type;

  /**
   * The ID specified when this column was defined.
   */
  public final String id;

  /**
   * Defines a column which is associated with the specified type.
   * The type specified here will be used for dynamic type checking
   * whenever a value is inserted or updated in this column of a
   * table; only values which are instances of the specified class
   * will be accepted.<p>
   */
  public Column(Class type, String id) {
    this.type = type;
    this.id = id;
  }

  public int compareTo(Column o) {
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
    return o instanceof Column && compareTo((Column) o) == 0;
  }
      
  public String toString() {
    return "column[" + id + " " + type.getName() + "]";
  }
}
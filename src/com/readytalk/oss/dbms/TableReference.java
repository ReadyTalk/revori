package com.readytalk.oss.dbms;

/**
 * Type representing a specific reference to a table.  A query may
 * make multiple references to the same table (e.g. when joining a
 * table with itself), in which case it is useful to represent those
 * references unambiguously as separate objects.
 */
public class TableReference implements Source {
  /**
   * The table specified when this instance was defined.
   */
  public final Table table;

  /**
   * Defines a table reference which may be used to unambiguously
   * refer to a table in a query or update.  Such a query or update
   * may refer to a table more than once, in which case one must
   * create multiple TableReferences to the same table.
   */
  public TableReference(Table table) {
    this.table = table;
  }
}

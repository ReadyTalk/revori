package com.readytalk.oss.dbms;

public interface RowBuilder {
  
  /**
   * Inserts or updates the key-value pair for a column
   * in the row this RowBuilder was constructed for. 
   * @return self
   */
  public RowBuilder column(Column key,
                           Object value);

  /**
   * Inserts or updates multiple columns at once.
   * @return self
   */
  public RowBuilder columns(ColumnList columns,
                            Object ... values);

  /**
   * Deletes the specified column from the row this
   * RowBuilder was constructed for.
   * @return self
   */
  public RowBuilder delete(Column key);

  /**
   * Indicate that no further updates will
   * be performed on this RowBuilder.
   * @return the parent TableBuilder
   */
  public TableBuilder up();
  
}

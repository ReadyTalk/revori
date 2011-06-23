package com.readytalk.oss.dbms;

public interface RowBuilder {
  
  /**
   * Inserts or updates the key-value pair for a column
   * in the row this RowBuilder was constructed for. 
   * @return itself
   */
  public RowBuilder alter(Resolution resolution, 
                          Column key,
                          Object value);

  /**
   * Deletes the specified column from the row this
   * RowBuilder was constructed for.
   */
  public RowBuilder delete(Column key);
  
}
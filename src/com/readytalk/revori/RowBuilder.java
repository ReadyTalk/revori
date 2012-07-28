/* Copyright (c) 2010-2012, Revori Contributors

   Permission to use, copy, modify, and/or distribute this software
   for any purpose with or without fee is hereby granted, provided
   that the above copyright notice and this permission notice appear
   in all copies. */

package com.readytalk.revori;

public interface RowBuilder {
  
  /**
   * Inserts or updates the key-value pair for a column
   * in the row this RowBuilder was constructed for. 
   * @return self
   */
  public <T> RowBuilder column(Column<T> key,
                           T value);

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
  public RowBuilder delete(Column<?> key);

  /**
   * Indicate that no further updates will
   * be performed on this RowBuilder.
   * @return the parent TableBuilder
   */
  public TableBuilder up();
  
}
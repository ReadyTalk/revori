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
  public <T> RowBuilder update(Column<T> key, T value);

  /**
   * Deletes the specified column from the row this
   * RowBuilder was constructed for.
   * @return self
   */
  public RowBuilder delete(Column<?> key);

  /**
   * Prepares a TableBuilder to update the given table.
   * @return said table builder
   */
  public TableBuilder table(Table table);
  
  /**
   * Prepares a RowBuilder to insert or update a row given by the specified 
   * primary key.
   * @return said row builder
   */
  public RowBuilder row(Object ... key);

  /**
   * Identical to commit(ForeignKeyResolvers.Restrict). 
   */
  public Revision commit();

  /**
   * Commits the changes accumulated in this builder, producing a
   * revision which reflects the base revision with which was created
   * plus any modifications applied thereafter.  This call invalidates
   * the builder; any further attempts to apply modifications to it
   * will result in IllegalStateExceptions.
   *
   * Any foreign key violations present at the time of this call will
   * trigger calls to foreignKeyResolver.handleBrokenReference.
   */
  public Revision commit(ForeignKeyResolver foreignKeyResolver);
}

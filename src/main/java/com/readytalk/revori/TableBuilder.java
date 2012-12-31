/* Copyright (c) 2010-2012, Revori Contributors

   Permission to use, copy, modify, and/or distribute this software
   for any purpose with or without fee is hereby granted, provided
   that the above copyright notice and this permission notice appear
   in all copies. */

package com.readytalk.revori;

public interface TableBuilder {
  
  /**
   * Prepares a RowBuilder to insert or update a row given by the specified 
   * primary key.
   * @return said row builder
   */
  public RowBuilder row(Object ... key);

  /**
   * Prepares a TableBuilder to update the given table.
   * @return said table builder
   */
  public TableBuilder table(Table table);
  
  /**
   * Deletes the row with the specified primary key.
   * @return self
   */
  public TableBuilder delete(Object ... key);

  /**
   * Inserts the row with specified primary key,
   * and no other columns.
   * @return self
   */
  public TableBuilder key(Object... key);

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

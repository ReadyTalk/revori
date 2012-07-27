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
   * Deletes the row with the specified primary key.
   * @return self
   */
  public TableBuilder delete(Object ... key);

  /**
   * Indicate that no further updates will
   * be performed on this TableBuilder.
   * @return the parent RevisionBuilder
   */
  public RevisionBuilder up();

  /**
   * Inserts the row with specified primary key,
   * and no other columns.
   * @return self
   */
  public TableBuilder key(Object... key);
}

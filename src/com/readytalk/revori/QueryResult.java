/* Copyright (c) 2010-2012, Revori Contributors

   Permission to use, copy, modify, and/or distribute this software
   for any purpose with or without fee is hereby granted, provided
   that the above copyright notice and this permission notice appear
   in all copies. */

package com.readytalk.revori;

/**
 * Represents an iterative view of a list of rows produced via
 * execution of a query diff, consisting of any rows added or updated
 * (ResultType.Inserted) and any removed or made obsolete by an update
 * (ResultType.Deleted).<p>
 *
 * See <code>MyDBMS.diff(Revision, Revision, QueryTemplate, Object[])
 * MyDBMS.diff(Revision, Revision, QueryTemplate, Object[])</code> for
 * details on the algorithm used to generate the list.
 */
public interface QueryResult {
  /**
   * These are the possible result types which may be returned by a
   * call to QueryResult.nextRow.
   */
  public enum Type {
    /**
     * Indicates that the current row being visited has not changed
     * from the old revision to the new one.  Currently,
     * QueryResult.nextRow will never actually return this value; it
     * is reserved for internal and possibly future use.
     */
    Unchanged,

      /**
       * Indicates that the current row being visited has been
       * inserted or has replaced an existing row from the old
       * revision to the new one.
       */
      Inserted,

      /**
       * Indicates that the current row being visited has been deleted
       * or replaced by a new one from the old revision to the new
       * one.
       */
      Deleted,

      /**
       * Indicates that there are no further rows to visit in the
       * current query result.
       */
      End;
  }

  /**
   * Visits the next row of the query diff, if any.  If the next row
   * consists of added or updated data, ResultType.Inserted is
   * returned.  If the next row consists of removed or obsolete data,
   * ResultType.Deleted is returned.  If there are no further rows in
   * the diff, ResultType.End is returned.
   */
  public Type nextRow();

  /**
   * Visits the next item of data in the current row.
   *
   * @throws NoSuchElementException if there is no current row or the
   * end of the row has been reached.
   */
  public Object nextItem();

  /**
   * Returns true if and only if the query source is a table (not a
   * join), the current row consists of obsolete data, and the next
   * row consists of updated data with the same primary key as the
   * current row.
   */
  public boolean rowUpdated();
}

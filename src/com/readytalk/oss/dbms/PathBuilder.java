package com.readytalk.oss.dbms;

interface PathBuilder {

  /**
   * Deletes any rows matching the specified path.  This is equivalent
   * to delete(path, 0, path.length).
   */
  public void delete(Object ... path);

  /**
   * Deletes any rows matching the specified path.  If pathLength is
   * zero, all rows in all tables are deleted.  If pathLength is
   * greater than or equal to one, path[pathOffset] is assumed to be a
   * Table, and the remainder (if any) are interpreted as primary key
   * values for that table.  A row is considered to match if its first
   * pathLength primary key values are equal to the elements of the
   * path delimited by pathOffset+1 and pathLength-1.
   */
  public void delete(Object[] path,
                     int pathOffset,
                     int pathLength);

  /**
   * Inserts or updates a row containing the specified values.  This
   * is equivalent to insert(duplicateKeyResolution, path, 0,
   * path.length).
   */
  public void insert(DuplicateKeyResolution duplicateKeyResolution,
                     Object ... path);

  /**
   * Inserts or updates a row containing the specified values.  The
   * path is interpreted as follows:<p>
   *
   * <ul><li>path[pathOffset]: the Table into which the row should be
   * inserted</li>
   *
   * <li>path[pathOffset+1..pathOffset+pathLength-3]: primary key values for
   * the specified table</li>
   *
   * <li>path[pathOffset+pathLength-2]: column to insert into or update</li>
   *
   * <li>path[pathOffset+pathLength-1]: column value to insert or
   * update</li></ul><p>
   *
   * If there is already row with a matching primary key in the table,
   * this method will act according to the specified
   * DuplicateKeyResolution.
   */
  public void insert(DuplicateKeyResolution duplicateKeyResolution,
                     Object[] path,
                     int pathOffset,
                     int pathLength);

}
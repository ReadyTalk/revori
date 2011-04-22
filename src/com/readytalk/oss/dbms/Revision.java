package com.readytalk.oss.dbms;

/**
 * Type representing an immutable database revision.
 */
public interface Revision {
  /**
   * Retrieves the value, if any, associated with the specified path
   * in this revision.  This is equivalent to query(path, 0,
   * path.length).
   */
  public Object query(Object ... path);

  /**
   * Retrieves the value, if any, associated with the specified path
   * in this revision.  The path is interpreted as follows:<p>
   *
   * <ul><li>path[pathOffset]: an Index to query</li>
   *
   * <li>path[pathOffset+1..pathOffset+pathLength-2]: values for the
   * specified index</li>
   *
   * <li>path[pathOffset+pathLength-1]: column to query</li></ul><p>
   */
  public Object query(Object[] path,
                      int pathOffset,
                      int pathLength);
}

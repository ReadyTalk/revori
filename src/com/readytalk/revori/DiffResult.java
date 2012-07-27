package com.readytalk.revori;

/**
 * Type representing a database-wide diff between two revisions.<p>
 *
 * The diff is represented as a tree, where the first level is the
 * union of tables appearing in either the base or the fork,
 * subsequent levels are primary key values for the respective tables,
 * and the leaf nodes are the key-value pairs composing the rows for
 * the respective primary keys.  If a table or key appears in at least
 * one of the two revisions being compared, it will appear in this
 * tree.  Thus, a DiffResult is a state machine which visits each node
 * in the tree in a depth-first fashion.
 */
public interface DiffResult {
  /**
   * These are the possible result types which may be returned by a
   * call to DiffResult.next.
   */
  public enum Type {
    /**
     * Indicates that there are no further nodes to visit.
     */
    End,

      /**
       * Indicates that the state machine is decending to a child node
       * in the tree.
       */
      Descend,
      
      /**
       * Indicates that the state machine is ascending to a parent node
       * in the tree.
       */
      Ascend,

      /**
       * Indicates that the state machine is visiting the key (Table
       * or Column) of a node in the tree.
       */
      Key,

      /**
       * Indicates that the state machine is visiting the value of a
       * leaf in the tree.  This value corresponds to the column just
       * visited.
       */
      Value;
  }

  /**
   * Requests the next event in the state machine.
   */
  public Type next();

  /**
   * Returns the key or value (depending on the last Type returned by
   * next()) present in the base revision at this point in the tree,
   * or null if it is not present.
   *
   * @throws IllegalStateException if the last Type returned by next() was
   * not Type.Key or Type.Value
   */
  public Object base();

  /**
   * Returns the key or value (depending on the last Type returned by
   * next()) present in the fork revision at this point in the tree,
   * or null if it is not present.
   *
   * @throws IllegalStateException if the last Type returned by next() was
   * not Type.Key or Type.Value
   */
  public Object fork();

  /**
   * Instructs the state machine to skip visiting the current node's
   * children, if any, when next() is called next.
   */
  public void skip();
}

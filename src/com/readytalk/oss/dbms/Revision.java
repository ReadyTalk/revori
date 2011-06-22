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
    
  /**
   * Defines a diff which represents the changes between the first
   * revision and the second concerning the specified query.<p>
   *
   * The query is defined by visiting the nodes of the expression
   * trees referred to by the specified query template and resolving
   * any parameter expressions found using the values of the specified
   * parameter array.  The expression trees nodes are visited
   * left-to-right in the order they were specified in
   * <code>QueryTemplate.QueryTemplate(List, Source,
   * Expression)</code>.<p>
   *
   * The result is two sets of tuples satisfying the query
   * constraints, including<p>
   *
   * <ul><li>new tuples which either appear in the second revision but
   * not the first or which have changed from the first to the
   * second (QueryResult.added()), and</li>
   *
   * <li>obsolete tuples which appear in the first but not the
   * second (QueryResult.removed()).</li></ul><p>
   *
   * Note that traditional SQL SELECT query semantics may be achieved
   * by specifying an empty revision as the first parameter and the
   * revision to be queried as the second.<p>
   *
   * The items of each row in the result are visited in the same order
   * as the expressions were specified in
   * <code>QueryTemplate.QueryTemplate(List, Source,
   * Expression)</code>.
   */
  public QueryResult diff(Revision fork,
                          QueryTemplate template,
                          Object ... parameters);

  /**
   * Defines a database-wide diff between two revisions.<p>
   *
   * See the DiffResult documentation for how this diff is
   * represented.
   */
  public DiffResult diff(Revision fork);

  /**
   * Creates a new builder for use in incrementally defining a new
   * revision based on this revision.
   */
  public RevisionBuilder builder();

  /**
   * Defines a new revision which merges the changes introduced in the
   * "left" fork relative to base with the changes introduced in
   * "right" fork relative to base.  The result is determined as
   * follows:<p>
   *
   * <ul><li>If a row was inserted into only one of the forks, or if
   * it was inserted into both with the same values, include the
   * insert in the result</li>
   *
   * <li>If a row was inserted into both forks with different values,
   * defer to the conflict resolver to produce a merged row and
   * include an insert of that row in the result</li>
   *
   * <li>If a row was updated in one fork but not the other, include
   * the update in the result</li>
   *
   * <li>If a row was updated in both forks such that no values
   * conflict, create a new row composed of the changed values from
   * both forks and the unchanged values from the base, and include an
   * update with that row in the result</li>
   *
   * <li>If a row was updated in both forks such that one or more
   * values conflict, defer to the conflict resolver to produce a
   * merged row and include an insert of that row in the result</li>
   *
   * <li>If a row was updated in one fork but deleted in another,
   * include the delete in the result</li></ul>
   */
  public Revision merge(Revision left,
                        Revision right,
                        ConflictResolver conflictResolver,
                        ForeignKeyResolver foreignKeyResolver);
}

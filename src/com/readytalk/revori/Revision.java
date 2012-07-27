package com.readytalk.revori;

/**
 * Type representing an immutable database revision.<p>
 *
 * This interface defines an API for using a revision-oriented
 * relational database management system.  The design centers on
 * immutable database revisions from which new revisions may be
 * derived by applying patches composed of inserts, updates and
 * deletes.  This, along with associated classes and interfaces,
 * provides methods to do the following:<p>
 *
 * <ul><li>Define sets of tables and columns which represent the
 * structure of the data to be stored</li>
 *
 * <li>Define a new, empty database revision</li>
 *
 * <li>Create new revisions by performing actions such as adding and
 * removing indexes and applying SQL-style inserts, updates, and
 * deletes
 *
 *   <ul><li>A row may contain values for any column, and there is no
 *   fixed list of columns which each row in a table must have except
 *   for those specified by the primary key.  A query for a column for
 *   which a row has no value will return null.</li></ul></li>
 *
 * <li>Calculate three-way merges from revisions for concurrency
 * control</li>
 *
 * <li>Define queries using SQL-style relational semantics</li>
 *
 * <li>Execute queries by supplying two revisions
 *
 *   <ul><li>The result is two sets of tuples satisfying the query
 *   constraints:
 *
 *     <ul><li>New tuples which either appear in the second revision
 *     but not the first or which have changed from the first to the
 *     second</li>
 *
 *     <li>Obsolete tuples which appear in the first but not the
 *     second</li></ul></li>
 *
 *   <li>Note that traditional query semantics may be achieved by
 *   specifying an empty revision as the first parameter and the
 *   revision to be queried as the second</li></ul></li></ul>
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
   *
   * If skipBrokenReferences is true, the diff will not visit deleted
   * rows for which at least one foreign key constraint is broken,
   * since their deletion can be inferred by the deletion or update of
   * the referent row(s).  Otherwise, all deleted rows will be
   * visited.
   */
  public DiffResult diff(Revision fork, boolean skipBrokenReferences);

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

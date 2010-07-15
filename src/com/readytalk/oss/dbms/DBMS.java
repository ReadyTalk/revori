package com.readytalk.oss.dbms;

import java.util.Set;
import java.util.Map;
import java.util.List;
import java.io.InputStream;
import java.io.OutputStream;

/**
 * This interface defines an API for using a revision-oriented
 * relational database management system.  The design centers on
 * immutable database revisions from which new revisions may be
 * derived by applying patches composed of inserts, updates and
 * deletes.  It provides methods to do the following:
 *
 * <ul><li>Define a new, empty database revision by supplying a list
 * of tables, each of which has a collection of columns and a primary
 * key defined as a subset of those columns
 *
 *   <ul><li>The identity of a row is defined by its primary key, and
 *   this is what we use when comparing rows while calculating diffs
 *   and merges</li></ul></li>
 *
 * <li>Create new revisions by defining and applying SQL-style
 * inserts, updates, and deletes</li>
 *
 * <li>Calculate diffs between revisions and serialize them for
 * replication and persistence
 *
 *   <ul><li>To generate a snapshot of an entire database, calculate a
 *   diff between the empty revision and the revision of
 *   interest</li></ul></li>
 *
 * <li>Calculate three-way merges from revisions for concurrency
 * control</li>
 *
 * <li>Define queries using SQL-style relational semantics</li>
 *
 * <li>Execute queries by supplying two database revisions
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
 *   database to be queried as the second</li></ul></li></ul>
 */
public interface DBMS {
  /**
   * These are the possible column types which may be specified when
   * defining a column.
   */
  public enum ColumnType {
    Integer32,   // int
      Integer64, // long
      String,    // String
      ByteArray, // instance of DBMS.ByteArray (see below)
      Object;    // any object instance of any type
  }

  /**
   * Represents a windowed view, or slice, of a byte array bounded by
   * the specified offset and length.
   */
  public interface ByteArray {
    public byte[] array();
    public int offset();
    public int length();
  }

  /**
   * Opaque type representing a column identifier for use in data
   * definition and later to identify a column of interest in a query
   * or update.
   */
  public interface Column { }

  /**
   * Opaque type representing a table identifier for use in data
   * definition and later to identify a column of interest in a query
   * or update.
   */
  public interface Table { }

  /**
   * Opaque type representing an immutable database revision.
   */
  public interface Database { }

  /**
   * Opaque type representing an expression (e.g. constant, column
   * reference, or query predicate) for use in queries and updates.
   */
  public interface Expression { }

  /**
   * Opaque type representing an expression that will evaluate to a
   * boolean value.
   */
  public interface BooleanExpression extends Expression { }

  /**
   * Opaque type representing a source (e.g. table reference or join)
   * from which to derive a query result.
   */
  public interface Source { }

  /**
   * Opaque type representing a specific reference to a column.  A
   * query may make multiple references to the same column (e.g. when
   * joining a table with itself), in which case it is useful to
   * represent those references as separate objects for
   * disambiguation.
   */
  public interface ColumnReference extends Expression { }

  /**
   * Opaque type representing a specific reference to a table.  A
   * query may make multiple references to the same tablw (e.g. when
   * joining a table with itself), in which case it is useful to
   * represent those references as separate objects for
   * disambiguation.
   */
  public interface TableReference extends Source { }

  /**
   * Opaque type representing the template for a query which is not
   * bound to any specific parameters, analogous to a prepared
   * statement in JDBC.
   */
  public interface QueryTemplate { }

  /**
   * Opaque type representing a query which combines a query template
   * with a set of specific parameters.
   */
  public interface Query { }

  /**
   * Opaque type representing the template for an insert, update, or
   * delete which is not bound to any specific parameters, analogous
   * to a prepared statement in JDBC.
   */
  public interface PatchTemplate { }

  /**
   * Opaque type representing a series of inserts, updates, and/or
   * deletes which may be applied to a database revision to produce a
   * new revision.
   */
  public interface Patch { }

  /**
   * A list of rows produced as part of a query result.
   */
  public interface ResultIterator {
    public boolean nextRow();
    public Object nextItem();
  }

  /**
   * Two lists of rows produced by the execution of a query diff,
   * consisting of any rows added or updated and any removed.  See
   * {@link #diff(Database, Database, Query) diff(Database, Database,
   * Query)} for details.
   */
  public interface QueryResult {
    public ResultIterator added();
    public ResultIterator removed();
  }

  /**
   * An interface for providing random access to the fields of a row
   * via column identifiers.
   */
  public interface Row {
    public Object value(Column column);
  }

  /**
   * An interface for resolving conflicts which accepts three versions
   * of a row -- the base version and two forks -- and returns a row
   * which resolves the conflict in an application-appropriate way.
   */
  public interface ConflictResolver {
    public Row resolveConflict(Table table,
                               Database base,
                               Row baseRow,
                               Database forkA,
                               Row forkARow,
                               Database forkB,
                               Row forkBRow);
  }

  /**
   * Defines a column identifier which is associated with the
   * specified type.  The type specified here will be used for dynamic
   * type checking whenever a value is inserted or updated in this
   * column of a table.
   */
  public Column column(ColumnType type);

  /**
   * Defines a table identifier which is associated with the specified
   * set of columns and a primary key.  The primary key is defined as
   * an ordered list of one or more columns where the order defines
   * the indexing order as in an SQL DBMS.  The combination of columns
   * in the primary key determine the unique identity of each row in
   * the table.
   */
  public Table table(Set<Column> columns,
                     List<Column> primaryKey);

  /**
   * Defines an empty database revision which is associated with the
   * specified set of table identifiers.
   */
  public Database database(Set<Table> tables);

  /**
   * Defines a table reference which may be used to unambigously refer
   * to a table in a query or update.  Such a query or update may
   * refer to a table more than once, in which case one must create
   * multiple TableReferences to the same table.
   */
  public TableReference tableReference(Table table);

  /**
   * Defines a column reference which may be used to unambigously refer
   * to a column in a query or update.  Such a query or update may
   * refer to a column more than once, in which case one must create
   * multiple ColumnReferences to the same table.
   */
  public ColumnReference columnReference(TableReference tableReference,
                                         Column column);

  /**
   * Defines a constant value as an expression for use when defining query and
   * patch templates.
   */
  public Expression constant(Object value);

  /**
   * Defines a placeholder as an expression for use when defining
   * query and update templates.  The actual value of the expression
   * will be specified when the template is combined with a list of
   * parameter values to define a query or patch.
   */
  public Expression parameter();

  /**
   * Defines a boolean expression which, when evaluated, answers the
   * question whether the parameter expressions are equal.
   */
  public BooleanExpression equal(Expression a,
                                 Expression b);

  /**
   * Defines a left outer join which matches each row in the left
   * table to a row in the right table (or null if there is no
   * corresponding row) according to the specified criterium.
   */
  public Source leftJoin(TableReference left,
                         TableReference right,
                         BooleanExpression criterium);

  /**
   * Defines a left outer join which matches each row in the first
   * table to a row in the second (excluding rows which have no match)
   * according to the specified criterium.
   */
  public Source innerJoin(TableReference a,
                          TableReference b,
                          BooleanExpression criterium);

  /**
   * Defines a query template (AKA prepared statement) with the
   * specified expressions to be evaluated, the source from which any
   * column references in the expression list should be resoved, and
   * the criterium for selecting from that source.
   */
  public QueryTemplate queryTemplate(List<Expression> expressions,
                                     Source source,
                                     BooleanExpression criterium);

  /**
   * Defines a query by matching a template with any parameter values
   * refered to by that template.
   */
  public Query query(QueryTemplate template,
                     Object ... parameters);

  /**
   * Executes the specified query and returns a diff which represents
   * the changes between the first database revision and the second
   * concerning that query.  The diff is a QueryResult from which the
   * list of new or updated result rows are available via
   * QueryResult.added() and the list of deleted or obsolete result
   * row are available via QueryResult.removed().
   */
  public QueryResult diff(Database a,
                          Database b,
                          Query query);

  /**
   * Defines a patch template (AKA prepared statement) which
   * represents an insert operation on the specified table.  The
   * values to be inserted are specified as a map of columns to
   * expressions.
   */
  public PatchTemplate insertTemplate(Table table,
                                      Map<Column, Expression> values);

  /**
   * Defines a patch template (AKA prepared statement) which
   * represents an update operation on the specified table to be
   * applied to rows satisfying the specified criterium.  The values
   * to be inserted are specified as a map of columns to expressions.
   */
  public PatchTemplate updateTemplate(Table table,
                                      BooleanExpression criterium,
                                      Map<Column, Expression> values);

  /**
   * Defines a patch template (AKA prepared statement) which
   * represents a delete operation on the specified table to be
   * applied to rows satisfying the specified criterium.
   */
  public PatchTemplate deleteTemplate(Table table,
                                      BooleanExpression constraint);

  /**
   * Defines a patch by matching a template with any parameter values
   * refered to by that template.
   */
  public Patch patch(PatchTemplate template,
                     Object ... parameters);

  /**
   * Defines a patch which combines the effect of several patches
   * applied in sequence.
   */
  public Patch sequence(Patch ... sequence);

  /**
   * Creates a new database revision which is the result of applying
   * the specified patch to the specified revision.
   **/
  public Database apply(Database database,
                        Patch patch);

  /**
   * Calculates a patch which represents the changes from the first
   * database revision specified to the second.
   **/
  public Patch diff(Database a,
                    Database b);

  /**
   * Creates a new database revision which merges the changes
   * introduced in forkA relative to base with the changes introduced
   * in forkB relative to base.  The result is determined as follows:
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
   * the insert in the result</li>
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
  public Database merge(Database base,
                        Database forkA,
                        Database forkB,
                        ConflictResolver conflictResolver);

  /**
   * Serializes the specified patch and writes it to the specified
   * stream.
   */
  public void write(Patch patch,
                    OutputStream out);

  /**
   * Deserializes a patch from the specified stream.
   */
  public Patch readPatch(InputStream in);
}

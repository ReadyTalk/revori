package com.readytalk.oss.dbms;

import java.util.Set;
import java.util.Map;
import java.util.List;

/**
 * This interface defines an API for using a revision-oriented
 * relational database management system.  The design centers on
 * database revisions from which new revisions may be derived by
 * applying patches composed of inserts, updates and deletes.  It
 * provides methods to do the following:<p>
 *
 * <ul><li>Define a set of tables, each of which has a collection of
 * columns and a primary key defined as a subset of those columns
 *
 *   <ul><li>The identity of a row is defined by its primary key, and
 *   this is what we use when comparing rows while calculating diffs
 *   and merges</li></ul></li>
 *
 * <li>Define a new, empty database revision</li>
 *
 * <li>Create new revisions by defining and applying SQL-style
 * inserts, updates, and deletes</li>
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
public interface DBMS {
  /**
   * These are the possible types which may be specified when defining
   * a column.
   */
  public enum ColumnType {
    /**
     * Indicates a column capable of storing instances of
     * java.lang.Integer.
     */
    Integer32,

      /**
       * Indicates a column capable of storing instances of
       * java.lang.Long.
       */
      Integer64,

      /**
       * Indicates a column capable of storing instances of
       * java.lang.String.
       */
      String,

      /**
       * Indicates a column capable of storing instances of
       * com.readytalk.oss.dbms.DBMS.ByteArray.
       */
      ByteArray,

      /**
       * Indicates a column capable of storing instances of
       * java.lang.Object.
       */
      Object;
  }

  /**
   * These are the possible types which may be specified when defining
   * a join.
   */
  public enum JoinType {
    /**
     * Indicates a left outer join which matches each row in the left
     * table to a row in the right table (or null if there is no
     * corresponding row).
     */
    LeftOuter,

      /**
       * Indicates an inner join which matches each row in the first
       * table to a row in the second (excluding rows which have no
       * match) according to the specified test.
       */
      Inner;
  }

  /**
   * These are the possible classes of UnaryOperationTypes and
   * BinaryOperationTypes.
   */
  public enum OperationClass {
    /**
     * Indicates a comparison (e.g. equal, less than, etc.) operation.
     */
    Comparison,

      /**
       * Indicates a boolean (e.g. and, or, etc.) operation.
       */
      Boolean;
  };

  /**
   * These are the possible types which may be specified when defining
   * an operation with two operands.
   */
  public enum UnaryOperationType {
    /**
     * Indicates a boolean "not" operation.
     */
    Not(OperationClass.Boolean);

    private final OperationClass operationClass;

    private UnaryOperationType(OperationClass operationClass) {
      this.operationClass = operationClass;
    }

    /**
     * Returns the operation class of this operation.
     */
    public OperationClass operationClass() {
      return operationClass;
    }
  }

  /**
   * These are the possible types which may be specified when defining
   * an operation with two operands.
   */
  public enum BinaryOperationType {
    /**
     * Indicates a boolean "and" operation.
     */
    And(OperationClass.Boolean),

      /**
       * Indicates a boolean "or" operation.
       */
      Or(OperationClass.Boolean),

      /**
       * Indicates a comparison which evaluates to true if the
       * operands are equal.
       */
      Equal(OperationClass.Comparison),

      /**
       * Indicates a comparison which evaluates to true if the
       * operands are not equal.
       */
      NotEqual(OperationClass.Comparison),

      /**
       * Indicates a comparison which evaluates to true if the left
       * operand compares less than the right.
       */
      LessThan(OperationClass.Comparison),

      /**
       * Indicates a comparison which evaluates to true if the left
       * operand compares less than or equal to the right.
       */
      LessThanOrEqual(OperationClass.Comparison),

      /**
       * Indicates a comparison which evaluates to true if the left
       * operand compares greater than the right.
       */
      GreaterThan(OperationClass.Comparison),

      /**
       * Indicates a comparison which evaluates to true if the left
       * operand compares greater than or equal to the right.
       */
      GreaterThanOrEqual(OperationClass.Comparison);

    private final OperationClass operationClass;

    private BinaryOperationType(OperationClass operationClass) {
      this.operationClass = operationClass;
    }

    /**
     * Returns the operation class of this operation.
     */
    public OperationClass operationClass() {
      return operationClass;
    }
  }

  /**
   * These are the possible result types which may be returned by a
   * call to QueryResult.nextRow.
   */
  public enum ResultType {
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
   * Represents a windowed view, or slice, of a byte array bounded by
   * the specified offset and length.
   */
  public interface ByteArray {
    public byte[] array();
    public int offset();
    public int length();
  }

  /**
   * Opaque type representing a column for use in table definition and
   * later to identify a column of interest in a query or update.
   */
  public interface Column { }

  /**
   * Opaque type representing an index for use in table definition.
   */
  public interface Index { }

  /**
   * Opaque type representing a table for use in data definition and
   * later to identify a column of interest in a query or update.
   */
  public interface Table { }

  /**
   * Opaque type representing a set of tables.
   */
  public interface Database { }

  /**
   * Opaque type representing an immutable database revision.
   */
  public interface Revision { }

  /**
   * Opaque type representing an expression (e.g. constant, column
   * reference, or query predicate) for use in queries and updates.
   */
  public interface Expression { }

  /**
   * Opaque type representing a query source (e.g. table reference or
   * join) from which to derive a query result.
   */
  public interface Source { }

  /**
   * Opaque type representing a specific reference to a column.  A
   * query may make multiple references to the same column (e.g. when
   * joining a table with itself), in which case it is useful to
   * represent those references unambiguously as separate objects.
   */
  public interface ColumnReference extends Expression { }

  /**
   * Opaque type representing a specific reference to a table.  A
   * query may make multiple references to the same table (e.g. when
   * joining a table with itself), in which case it is useful to
   * represent those references unambiguously as separate objects.
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
   * deletes which may be applied to a revision to produce a new
   * revision.
   */
  public interface Patch { }

  /**
   * Represents an iterative view of a list of rows produced via
   * execution of a query diff, consisting of any rows added or
   * updated (ResultType.Inserted) and any removed or made obsolete by
   * an update (ResultType.Deleted).  Once the last row has been
   * visited, ResultType.End.<p>
   *
   * See {@link #diff(Revision, Revision, Query) diff(Revision,
   * Revision, Query)} for details on the algorithm used to generate
   * the list.
   */
  public interface QueryResult {
    public ResultType nextRow();
    public Object nextItem();
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
                               Revision base,
                               Row baseRow,
                               Revision forkA,
                               Row forkARow,
                               Revision forkB,
                               Row forkBRow);
  }

  /**
   * Defines a column which is associated with the specified type.
   * The type specified here will be used for dynamic type checking
   * whenever a value is inserted or updated in this column of a
   * table.
   */
  public Column column(ColumnType type);

  /**
   * Defines an index which is associated with the specified list of
   * columns.  The order of the list determines the indexing order as
   * in an SQL DBMS.  If the index is unique, only one row may be
   * inserted with a given combination of the specified columns.
   */
  public Index index(List<Column> columns, boolean unique);

  /**
   * Defines a table which is associated with the specified set of
   * columns, primary key, and other indexes.  The combination of
   * columns in the primary key determine the unique identity of each
   * row in the table for the purposes of diff and merge calculation.
   */
  public Table table(Set<Column> columns,
                     Index primaryKey,                     
                     Set<Index> indexes);

  /**
   * Defines an empty database revision.
   */
  public Revision revision();

  /**
   * Defines a table reference which may be used to unambiguously
   * refer to a table in a query or update.  Such a query or update
   * may refer to a table more than once, in which case one must
   * create multiple TableReferences to the same table.
   */
  public TableReference tableReference(Table table);

  /**
   * Defines a column reference which may be used to unambiguously
   * refer to a column in a query or update.  Such a query or update
   * may refer to a column more than once, in which case one must
   * create multiple ColumnReferences to the same table.
   */
  public ColumnReference columnReference(TableReference tableReference,
                                         Column column);

  /**
   * Defines a constant value as an expression for use when defining
   * query and patch templates.
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
   * Defines an expression which, when evaluated, applies the
   * specified operation to its operands.
   */
  public Expression operation(BinaryOperationType type,
                              Expression leftOperand,
                              Expression rightOperand);

  /**
   * Defines an expression which, when evaluated, applies the
   * specified operation to its operand.
   */
  public Expression operation(UnaryOperationType type,
                              Expression operand);

  /**
   * Defines a join which matches each row in the left source to a row
   * in the right source according to the specified join type.
   */
  public Source join(JoinType type,
                     Source left,
                     Source right);

  /**
   * Defines a query template (AKA prepared statement) with the
   * specified expressions to be evaluated, the source from which any
   * column references in the expression list should be resolved, and
   * the test for selecting from that source.
   */
  public QueryTemplate queryTemplate(List<Expression> expressions,
                                     Source source,
                                     Expression test);

  /**
   * Defines a query by matching a template with any parameter values
   * referred to by that template.
   */
  public Query query(QueryTemplate template,
                     Object ... parameters);

  /**
   * Executes the specified query and returns a diff which represents
   * the changes between the first revision and the second concerning
   * that query.<p>
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
   * revision to be queried as the second.
   */
  public QueryResult diff(Revision base,
                          Revision fork,
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
   * applied to rows satisfying the specified test.  The values
   * to be inserted are specified as a map of columns to expressions.
   */
  public PatchTemplate updateTemplate(TableReference tableReference,
                                      Expression test,
                                      Map<Column, Expression> values);

  /**
   * Defines a patch template (AKA prepared statement) which
   * represents a delete operation on the specified table to be
   * applied to rows satisfying the specified test.
   */
  public PatchTemplate deleteTemplate(TableReference tableReference,
                                      Expression test);

  /**
   * Defines a patch by matching a template with any parameter values
   * referred to by that template.
   */
  public Patch patch(PatchTemplate template,
                     Object ... parameters);

  /**
   * Creates a new revision which is the result of applying the
   * specified patch to the specified revision.<p>
   *
   * The token parameter determines which parts of the input revision
   * may be considered mutable.  Only elements in the revision which
   * are tagged with the specified token may be modified -- all others
   * must be considered immutable and replaced to build the new
   * revision.<p>
   *
   * The purpose of the token mechanism is to allow the application to
   * apply a series of patches efficiently by specifying the same
   * token for each one.  Otherwise, the DBMS implementation would
   * have to conservatively assume every revision is immutable and
   * that it must create new copies of each structural and tuple
   * element involved in applying each patch, leading to much more
   * frequent object creation and copying for large batches of
   * updates.<p>
   *
   * One consequence of this optimization is that it is that the
   * application controls the mutability of revisions and is thus
   * responsible for preserving each revision which must remain
   * unchanged by "throwing away" any tokens used to produce it so
   * that they cannot be reused.
   **/
  public Revision apply(Object token,
                        Revision revision,
                        Patch patch);

  /**
   * Creates a new revision which merges the changes introduced in the
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
  public Revision merge(Revision base,
                        Revision left,
                        Revision right,
                        ConflictResolver conflictResolver);
}

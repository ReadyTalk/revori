package com.readytalk.oss.dbms;

import java.util.Collection;
import java.util.Set;
import java.util.Map;
import java.util.List;

/**
 * This interface defines an API for using a revision-oriented
 * relational database management system.  The design centers on
 * immutable database revisions from which new revisions may be
 * derived by applying patches composed of inserts, updates and
 * deletes.  It provides methods to do the following:<p>
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
public interface DBMS {
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
   * These are the possible actions to take when an insert or update
   * introduces a row which conflicts with an existing row by matching
   * the same key of a primary key.
   */
  public enum DuplicateKeyResolution {
    /**
     * Instructs the DBMS to silently skip the insert or update,
     * leaving the old row(s) intact.
     */
    Skip,
      
      /**
       * Instructs the DBMS to silently overwrite the old row with the
       * new one.
       */
      Overwrite,

      /**
       * Instructs the DBMS to throw a DuplicateKeyException when a
       * conflict is detected.
       */
      Throw
  }

  /**
   * Exception thrown when an insert or update introduces a row which
   * conflicts with an existing row by matching the same key of a
   * primary key.
   */
  public static class DuplicateKeyException extends RuntimeException { }

  /**
   * Opaque type representing a column which may be used to identify
   * an item of interest in a query or update.
   */
  public interface Column { }

  /**
   * Opaque type representing an index on a table.  Instances of this
   * interface may used to specify a way to organize data for
   * efficient access.
   */
  public interface Index { }

  /**
   * Opaque type representing a table.  Instances of this interface do
   * not hold any data; they're used only to identify a collection of
   * rows of interest in a query or update.
   */
  public interface Table { }

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
   * Opaque type representing the template for an insert, update, or
   * delete which is not bound to any specific parameters, analogous
   * to a prepared statement in JDBC.
   */
  public interface PatchTemplate { }

  /**
   * Opaque type used for incrementally defining a new revision by
   * applying a series of inserts, updates, and/or deletes to a base
   * revision.
   */
  public interface PatchContext { }

  /**
   * Represents an iterative view of a list of rows produced via
   * execution of a query diff, consisting of any rows added or
   * updated (ResultType.Inserted) and any removed or made obsolete by
   * an update (ResultType.Deleted).<p>
   *
   * See {@link #diff(Revision, Revision, QueryTemplate, Object[])
   * diff(Revision, Revision, QueryTemplate, Object[])} for details on
   * the algorithm used to generate the list.
   */
  public interface QueryResult {
    /**
     * Visits the next row of the query diff, if any.  If the next row
     * consists of added or updated data, ResultType.Inserted is
     * returned.  If the next row consists of removed or obsolete
     * data, ResultType.Deleted is returned.  If there are no further
     * rows in the diff, ResultType.End is returned.
     */
    public ResultType nextRow();

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

  public enum DiffResultType {
    End, Descend, Ascend, Key, Value;
  }

  public interface DiffResult {
    public DiffResultType next();

    public Object get();

    public boolean baseHasKey();

    public boolean forkHasKey();

    public void skip();
  }

  /**
   * An interface for resolving conflicts which accepts three versions
   * of a value -- the base version and two forks -- and returns a
   * value which resolves the conflict in an application-appropriate
   * way.
   */
  public interface ConflictResolver {
    public Object resolveConflict(Table table,
                                  Column column,
                                  Object[] primaryKeyValues,
                                  Object baseValue,
                                  Object leftValue,
                                  Object rightValue);
  }

  /**
   * Defines a column which is associated with the specified type.
   * The type specified here will be used for dynamic type checking
   * whenever a value is inserted or updated in this column of a
   * table; only values which are instances of the specified class
   * will be accepted.
   */
  public Column column(Class type);

  /**
   * Defines an index which is associated with the specified list of
   * columns.  The order of the list determines the indexing order as
   * in an SQL DBMS.
   */
  public Index index(Table table,
                     List<Column> columns);

  /**
   * Defines a table using the specified list of columns as the
   * primary key.
   */
  public Table table(List<Column> primaryKey);

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
   * Executes the specified query and returns a diff which represents
   * the changes between the first revision and the second concerning
   * that query.<p>
   *
   * The query is defined by visiting the nodes of the expression
   * trees referred to by the specified query template and resolving
   * any parameter expressions found using the values of the specified
   * parameter array.  The expression trees nodes are visited
   * left-to-right in the order they were specified in
   * {@link #queryTemplate(List, Source, Expression)
   * queryTemplate(List, Source, Expression)}.<p>
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
   * {@link #queryTemplate(List, Source, Expression)
   * queryTemplate(List, Source, Expression)}.
   */
  public QueryResult diff(Revision base,
                          Revision fork,
                          QueryTemplate template,
                          Object ... parameters);

  public DiffResult diff(Revision base,
                         Revision fork);

  /**
   * Defines a patch template (AKA prepared statement) which
   * represents an insert operation on the specified table.  The
   * values to be inserted are specified as two ordered lists of equal
   * length: a list of columns and a list of expressions representing
   * the values to be placed into those columns.
   *
   * If, when this template is applied, there is already row with a
   * matching primary key in the table, the DBMS will act according to
   * the specified DuplicateKeyResolution.
   */
  public PatchTemplate insertTemplate
    (Table table,
     List<Column> columns,
     List<Expression> values,
     DuplicateKeyResolution duplicateKeyResolution);

  /**
   * Defines a patch template (AKA prepared statement) which
   * represents an update operation on the specified table to be
   * applied to rows satisfying the specified test.  The
   * values to be updated are specified as two ordered lists of equal
   * length: a list of columns and a list of expressions representing
   * the values to be placed into those columns.
   */
  public PatchTemplate updateTemplate(TableReference tableReference,
                                      Expression test,
                                      List<Column> columns,
                                      List<Expression> values);

  /**
   * Defines a patch template (AKA prepared statement) which
   * represents a delete operation on the specified table to be
   * applied to rows satisfying the specified test.
   */
  public PatchTemplate deleteTemplate(TableReference tableReference,
                                      Expression test);

  /**
   * Creates a new patch context for use in incrementally defining a
   * new revision based on the specified base revision.
   */
  public PatchContext patchContext(Revision base);

  /**
   * Applies the specified patch to the specified patch context.<p>
   *
   * The patch is defined by visiting the nodes of the expression
   * trees referred to by the specified patch template and resolving
   * any parameter expressions found using the values of the specified
   * parameter array.  The expression tree nodes are visited
   * left-to-right in the order they where specified when the patch
   * template was defined.
   *
   * @return the number of rows affected by the patch
   *
   * @throws IllegalStateException if the specified patch context has
   * already been committed
   *
   * @throws DuplicateKeyException if the specified patch introduces a
   * duplicate primary key
   *
   * @throws ClassCastException if an inserted or updated value cannot
   * be cast to the declared type of its column
   */
  public int apply(PatchContext context,
                   PatchTemplate template,
                   Object ... parameters)
    throws IllegalStateException,
           DuplicateKeyException,
           ClassCastException;

  public void delete(PatchContext context,
                     Object ... path);

  public void insert(PatchContext context,
                     DuplicateKeyResolution duplicateKeyResolution,
                     Object ... path);

  /**
   * Adds the specified index to the specified patch context.
   */
  public void add(PatchContext context,
                  Index index);

  /**
   * Removes the specified index from the specified patch context.
   */
  public void remove(PatchContext context,
                     Index index);

  /**
   * Commits the specified patch context, producing a revision which
   * reflects the base revision with which was created plus any
   * modifications applied thereafter.  This call invalidates the
   * specified context; any further attempts to apply modifications to
   * it will result in IllegalStateExceptions.
   */
  public Revision commit(PatchContext context);

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
  public Revision merge(Revision base,
                        Revision left,
                        Revision right,
                        ConflictResolver conflictResolver);
}

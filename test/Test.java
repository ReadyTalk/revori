import static com.readytalk.oss.dbms.imp.Util.list;
import static com.readytalk.oss.dbms.imp.Util.set;

import com.readytalk.oss.dbms.DBMS;
import com.readytalk.oss.dbms.DBMS.ColumnType;
import com.readytalk.oss.dbms.DBMS.Column;
import com.readytalk.oss.dbms.DBMS.Index;
import com.readytalk.oss.dbms.DBMS.Table;
import com.readytalk.oss.dbms.DBMS.Expression;
import com.readytalk.oss.dbms.DBMS.Revision;
import com.readytalk.oss.dbms.DBMS.PatchContext;
import com.readytalk.oss.dbms.DBMS.PatchTemplate;
import com.readytalk.oss.dbms.DBMS.TableReference;
import com.readytalk.oss.dbms.DBMS.BinaryOperationType;
import com.readytalk.oss.dbms.DBMS.QueryTemplate;
import com.readytalk.oss.dbms.DBMS.QueryResult;
import com.readytalk.oss.dbms.DBMS.ResultType;
import com.readytalk.oss.dbms.imp.MyDBMS;

import java.util.Set;
import java.util.Collections;

public class Test {
  private static final Set<Index> EmptyIndexSet = Collections.emptySet();

  private static void expectEqual(Object value, Object expected) {
    if (value == null) {
      if (expected == null) {
        return;
      }
    } else if (value.equals(expected)) {
      return;
    }
    throw new RuntimeException("expected " + expected + "; got " + value);
  }

  private static void testSimpleInsertDiffs() {
    DBMS dbms = new MyDBMS();

    Column id = dbms.column(ColumnType.Integer32);
    Column name = dbms.column(ColumnType.String);
    Table users = dbms.table
      (set(id, name),
       dbms.index(list(id), true),
       EmptyIndexSet);

    Revision tail = dbms.revision();

    PatchContext context = dbms.patchContext(tail);

    dbms.apply
      (context,
       dbms.insertTemplate
       (users,
        list(id, name),
        list(dbms.constant(42),
             dbms.constant("elroy"))));

    Revision elroy = dbms.commit(context);

    TableReference usersReference = dbms.tableReference(users);
    QueryTemplate queryTemplate = dbms.queryTemplate
      (list((Expression) dbms.columnReference(usersReference, name)),
        usersReference,
        dbms.operation
        (BinaryOperationType.Equal,
         dbms.columnReference(usersReference, id),
         dbms.parameter()));

    QueryResult result = dbms.diff(tail, elroy, queryTemplate, 42);

    expectEqual(result.nextRow(), ResultType.Inserted);
    expectEqual(result.nextItem(), "elroy");
    expectEqual(result.nextRow(), ResultType.End);

    result = dbms.diff(elroy, tail, queryTemplate, 42);

    expectEqual(result.nextRow(), ResultType.Deleted);
    expectEqual(result.nextItem(), "elroy");
    expectEqual(result.nextRow(), ResultType.End);

    result = dbms.diff(tail, elroy, queryTemplate, 43);

    expectEqual(result.nextRow(), ResultType.End);

    result = dbms.diff(tail, tail, queryTemplate, 42);

    expectEqual(result.nextRow(), ResultType.End);

    result = dbms.diff(elroy, elroy, queryTemplate, 42);

    expectEqual(result.nextRow(), ResultType.End);
  }

  private static void testLargerInsertDiffs() {
    DBMS dbms = new MyDBMS();

    Column id = dbms.column(ColumnType.Integer32);
    Column name = dbms.column(ColumnType.String);
    Table users = dbms.table
      (set(id, name),
       dbms.index(list(id), true),
       EmptyIndexSet);

    Revision tail = dbms.revision();

    PatchTemplate insertTemplate = dbms.insertTemplate
      (users,
       list(id, name),
       list(dbms.parameter(),
            dbms.parameter()));

    PatchContext context = dbms.patchContext(tail);

    dbms.apply(context, insertTemplate, 42, "forty two");
    dbms.apply(context, insertTemplate, 43, "forty three");
    dbms.apply(context, insertTemplate, 44, "forty four");
    dbms.apply(context, insertTemplate,  2, "two");
    dbms.apply(context, insertTemplate, 65, "sixty five");
    dbms.apply(context, insertTemplate,  8, "eight");

    Revision first = dbms.commit(context);

    TableReference usersReference = dbms.tableReference(users);
    QueryTemplate queryTemplate = dbms.queryTemplate
      (list((Expression) dbms.columnReference(usersReference, name)),
        usersReference,
        dbms.operation
        (BinaryOperationType.Equal,
         dbms.columnReference(usersReference, id),
         dbms.parameter()));

    QueryResult result = dbms.diff(tail, first, queryTemplate, 42);

    expectEqual(result.nextRow(), ResultType.Inserted);
    expectEqual(result.nextItem(), "forty two");
    expectEqual(result.nextRow(), ResultType.End);

    result = dbms.diff(first, tail, queryTemplate, 42);

    expectEqual(result.nextRow(), ResultType.Deleted);
    expectEqual(result.nextItem(), "forty two");
    expectEqual(result.nextRow(), ResultType.End);

    result = dbms.diff(tail, first, queryTemplate, 43);

    expectEqual(result.nextRow(), ResultType.Inserted);
    expectEqual(result.nextItem(), "forty three");
    expectEqual(result.nextRow(), ResultType.End);

    result = dbms.diff(tail, tail, queryTemplate, 42);

    expectEqual(result.nextRow(), ResultType.End);

    result = dbms.diff(first, first, queryTemplate, 42);

    expectEqual(result.nextRow(), ResultType.End);

    context = dbms.patchContext(first);

    dbms.apply(context, insertTemplate, 1, "one");
    dbms.apply(context, insertTemplate, 3, "three");
    dbms.apply(context, insertTemplate, 5, "five");
    dbms.apply(context, insertTemplate, 6, "six");

    Revision second = dbms.commit(context);

    result = dbms.diff(tail, second, queryTemplate, 43);

    expectEqual(result.nextRow(), ResultType.Inserted);
    expectEqual(result.nextItem(), "forty three");
    expectEqual(result.nextRow(), ResultType.End);

    result = dbms.diff(first, second, queryTemplate, 43);

    expectEqual(result.nextRow(), ResultType.End);
    
    result = dbms.diff(first, second, queryTemplate, 5);

    expectEqual(result.nextRow(), ResultType.Inserted);
    expectEqual(result.nextItem(), "five");
    expectEqual(result.nextRow(), ResultType.End);
    
    result = dbms.diff(tail, first, queryTemplate, 5);

    expectEqual(result.nextRow(), ResultType.End);
    
    result = dbms.diff(second, first, queryTemplate, 5);

    expectEqual(result.nextRow(), ResultType.Deleted);
    expectEqual(result.nextItem(), "five");
    expectEqual(result.nextRow(), ResultType.End);
  }

  private static void testDeleteDiffs() {
    DBMS dbms = new MyDBMS();

    Column id = dbms.column(ColumnType.Integer32);
    Column name = dbms.column(ColumnType.String);
    Table users = dbms.table
      (set(id, name),
       dbms.index(list(id), true),
       EmptyIndexSet);

    Revision tail = dbms.revision();

    PatchTemplate insertTemplate = dbms.insertTemplate
      (users,
       list(id, name),
       list(dbms.parameter(),
            dbms.parameter()));

    PatchContext context = dbms.patchContext(tail);

    dbms.apply(context, insertTemplate, 42, "forty two");
    dbms.apply(context, insertTemplate, 43, "forty three");
    dbms.apply(context, insertTemplate, 44, "forty four");
    dbms.apply(context, insertTemplate,  2, "two");
    dbms.apply(context, insertTemplate, 65, "sixty five");
    dbms.apply(context, insertTemplate,  8, "eight");

    Revision first = dbms.commit(context);

    TableReference usersReference = dbms.tableReference(users);
    QueryTemplate queryTemplate = dbms.queryTemplate
      (list((Expression) dbms.columnReference(usersReference, name)),
        usersReference,
        dbms.operation
        (BinaryOperationType.Equal,
         dbms.columnReference(usersReference, id),
         dbms.parameter()));

    PatchTemplate deleteTemplate = dbms.deleteTemplate
      (usersReference,
       dbms.operation
       (BinaryOperationType.Equal,
        dbms.columnReference(usersReference, id),
        dbms.parameter()));

    context = dbms.patchContext(first);

    dbms.apply(context, deleteTemplate, 43);

    Revision second = dbms.commit(context);

    QueryResult result = dbms.diff(first, second, queryTemplate, 43);

    expectEqual(result.nextRow(), ResultType.Deleted);
    expectEqual(result.nextItem(), "forty three");
    expectEqual(result.nextRow(), ResultType.End);

    result = dbms.diff(second, first, queryTemplate, 43);

    expectEqual(result.nextRow(), ResultType.Inserted);
    expectEqual(result.nextItem(), "forty three");
    expectEqual(result.nextRow(), ResultType.End);

    result = dbms.diff(tail, second, queryTemplate, 43);

    expectEqual(result.nextRow(), ResultType.End);
    
    result = dbms.diff(first, second, queryTemplate, 42);

    expectEqual(result.nextRow(), ResultType.End);
    
    result = dbms.diff(tail, second, queryTemplate, 42);

    expectEqual(result.nextRow(), ResultType.Inserted);
    expectEqual(result.nextItem(), "forty two");
    expectEqual(result.nextRow(), ResultType.End);
    
    result = dbms.diff(second, tail, queryTemplate, 42);

    expectEqual(result.nextRow(), ResultType.Deleted);
    expectEqual(result.nextItem(), "forty two");
    expectEqual(result.nextRow(), ResultType.End);

    context = dbms.patchContext(first);

    dbms.apply(context, deleteTemplate, 43);
    dbms.apply(context, deleteTemplate, 42);
    dbms.apply(context, deleteTemplate, 65);

    second = dbms.commit(context);

    result = dbms.diff(first, second, queryTemplate, 43);

    expectEqual(result.nextRow(), ResultType.Deleted);
    expectEqual(result.nextItem(), "forty three");
    expectEqual(result.nextRow(), ResultType.End);

    result = dbms.diff(second, first, queryTemplate, 43);

    expectEqual(result.nextRow(), ResultType.Inserted);
    expectEqual(result.nextItem(), "forty three");
    expectEqual(result.nextRow(), ResultType.End);

    result = dbms.diff(first, second, queryTemplate, 42);

    expectEqual(result.nextRow(), ResultType.Deleted);
    expectEqual(result.nextItem(), "forty two");
    expectEqual(result.nextRow(), ResultType.End);

    result = dbms.diff(second, first, queryTemplate, 42);

    expectEqual(result.nextRow(), ResultType.Inserted);
    expectEqual(result.nextItem(), "forty two");
    expectEqual(result.nextRow(), ResultType.End);

    result = dbms.diff(first, second, queryTemplate, 65);

    expectEqual(result.nextRow(), ResultType.Deleted);
    expectEqual(result.nextItem(), "sixty five");
    expectEqual(result.nextRow(), ResultType.End);

    result = dbms.diff(second, first, queryTemplate, 65);

    expectEqual(result.nextRow(), ResultType.Inserted);
    expectEqual(result.nextItem(), "sixty five");
    expectEqual(result.nextRow(), ResultType.End);
    
    result = dbms.diff(first, second, queryTemplate, 2);

    expectEqual(result.nextRow(), ResultType.End);
    
    result = dbms.diff(second, first, queryTemplate, 2);

    expectEqual(result.nextRow(), ResultType.End);
  }

  public static void main(String[] args) {
    testSimpleInsertDiffs();

    testLargerInsertDiffs();

    testDeleteDiffs();
  }
}

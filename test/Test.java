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

  private static void testSimpleDiffs() {
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
        list(dbms.constant(Integer.valueOf(42)),
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

    QueryResult result = dbms.diff
      (tail, elroy, queryTemplate, Integer.valueOf(42));

    expectEqual(result.nextRow(), ResultType.Inserted);
    expectEqual(result.nextItem(), "elroy");
    expectEqual(result.nextRow(), ResultType.End);

    result = dbms.diff
      (elroy, tail, queryTemplate, Integer.valueOf(42));

    expectEqual(result.nextRow(), ResultType.Deleted);
    expectEqual(result.nextItem(), "elroy");
    expectEqual(result.nextRow(), ResultType.End);

    result = dbms.diff
      (tail, elroy, queryTemplate, Integer.valueOf(43));

    expectEqual(result.nextRow(), ResultType.End);

    result = dbms.diff
      (tail, tail, queryTemplate, Integer.valueOf(42));

    expectEqual(result.nextRow(), ResultType.End);

    result = dbms.diff
      (elroy, elroy, queryTemplate, Integer.valueOf(42));

    expectEqual(result.nextRow(), ResultType.End);
  }

  public static void main(String[] args) {
    testSimpleDiffs();
  }
}

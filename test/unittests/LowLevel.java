package unittests;

import org.junit.Test;
import static org.junit.Assert.assertEquals;

import static com.readytalk.oss.dbms.imp.Util.list;
import static com.readytalk.oss.dbms.DBMS.DuplicateKeyResolution.Throw;
import static com.readytalk.oss.dbms.DBMS.DuplicateKeyResolution.Overwrite;

import com.readytalk.oss.dbms.DBMS;
import com.readytalk.oss.dbms.DBMS.Column;
import com.readytalk.oss.dbms.DBMS.Table;
import com.readytalk.oss.dbms.DBMS.Revision;
import com.readytalk.oss.dbms.DBMS.PatchContext;
import com.readytalk.oss.dbms.DBMS.DiffResult;
import com.readytalk.oss.dbms.DBMS.DiffResultType;
import com.readytalk.oss.dbms.imp.MyDBMS;

public class LowLevel {
  private static void expectEqual(Object actual, Object expected) {
    assertEquals(expected, actual);
  }

  @Test
  public void testLowLevelDiffs() {
    DBMS dbms = new MyDBMS();

    Column number = dbms.column(Integer.class);
    Column name = dbms.column(String.class);
    Table numbers = dbms.table(list(number));

    Revision tail = dbms.revision();

    PatchContext context = dbms.patchContext(tail);

    dbms.insert(context, Throw, numbers, 1, name, "one");
    dbms.insert(context, Throw, numbers, 2, name, "two");
    dbms.insert(context, Throw, numbers, 3, name, "three");
    dbms.insert(context, Throw, numbers, 4, name, "four");
    dbms.insert(context, Throw, numbers, 5, name, "five");
    dbms.insert(context, Throw, numbers, 6, name, "six");
    dbms.insert(context, Throw, numbers, 7, name, "seven");
    dbms.insert(context, Throw, numbers, 8, name, "eight");
    dbms.insert(context, Throw, numbers, 9, name, "nine");

    Revision first = dbms.commit(context);

    DiffResult result = dbms.diff(tail, first);

    expectEqual(result.next(), DiffResultType.Key);
    expectEqual(result.base(), null);
    expectEqual(result.fork(), numbers);

    expectEqual(result.next(), DiffResultType.Descend);
    expectEqual(result.next(), DiffResultType.Key);
    expectEqual(result.base(), null);
    expectEqual(result.fork(), 1);
    expectEqual(result.next(), DiffResultType.Descend);
    expectEqual(result.next(), DiffResultType.Key);
    expectEqual(result.base(), null);
    expectEqual(result.fork(), name);
    expectEqual(result.next(), DiffResultType.Value);
    expectEqual(result.base(), null);
    expectEqual(result.fork(), "one");

    expectEqual(result.next(), DiffResultType.Ascend);
    expectEqual(result.next(), DiffResultType.Key);
    expectEqual(result.base(), null);
    expectEqual(result.fork(), 2);
    expectEqual(result.next(), DiffResultType.Descend);
    expectEqual(result.next(), DiffResultType.Key);
    expectEqual(result.base(), null);
    expectEqual(result.fork(), name);
    expectEqual(result.next(), DiffResultType.Value);
    expectEqual(result.base(), null);
    expectEqual(result.fork(), "two");

    expectEqual(result.next(), DiffResultType.Ascend);
    expectEqual(result.next(), DiffResultType.Key);
    expectEqual(result.base(), null);
    expectEqual(result.fork(), 3);
    expectEqual(result.next(), DiffResultType.Descend);
    expectEqual(result.next(), DiffResultType.Key);
    expectEqual(result.base(), null);
    expectEqual(result.fork(), name);
    expectEqual(result.next(), DiffResultType.Value);
    expectEqual(result.base(), null);
    expectEqual(result.fork(), "three");

    expectEqual(result.next(), DiffResultType.Ascend);
    expectEqual(result.next(), DiffResultType.Key);
    expectEqual(result.base(), null);
    expectEqual(result.fork(), 4);
    expectEqual(result.next(), DiffResultType.Descend);
    expectEqual(result.next(), DiffResultType.Key);
    expectEqual(result.base(), null);
    expectEqual(result.fork(), name);
    expectEqual(result.next(), DiffResultType.Value);
    expectEqual(result.base(), null);
    expectEqual(result.fork(), "four");

    expectEqual(result.next(), DiffResultType.Ascend);
    expectEqual(result.next(), DiffResultType.Key);
    expectEqual(result.base(), null);
    expectEqual(result.fork(), 5);
    expectEqual(result.next(), DiffResultType.Descend);
    expectEqual(result.next(), DiffResultType.Key);
    expectEqual(result.base(), null);
    expectEqual(result.fork(), name);
    expectEqual(result.next(), DiffResultType.Value);
    expectEqual(result.base(), null);
    expectEqual(result.fork(), "five");

    expectEqual(result.next(), DiffResultType.Ascend);
    expectEqual(result.next(), DiffResultType.Key);
    expectEqual(result.base(), null);
    expectEqual(result.fork(), 6);
    expectEqual(result.next(), DiffResultType.Descend);
    expectEqual(result.next(), DiffResultType.Key);
    expectEqual(result.base(), null);
    expectEqual(result.fork(), name);
    expectEqual(result.next(), DiffResultType.Value);
    expectEqual(result.base(), null);
    expectEqual(result.fork(), "six");

    expectEqual(result.next(), DiffResultType.Ascend);
    expectEqual(result.next(), DiffResultType.Key);
    expectEqual(result.base(), null);
    expectEqual(result.fork(), 7);
    expectEqual(result.next(), DiffResultType.Descend);
    expectEqual(result.next(), DiffResultType.Key);
    expectEqual(result.base(), null);
    expectEqual(result.fork(), name);
    expectEqual(result.next(), DiffResultType.Value);
    expectEqual(result.base(), null);
    expectEqual(result.fork(), "seven");

    expectEqual(result.next(), DiffResultType.Ascend);
    expectEqual(result.next(), DiffResultType.Key);
    expectEqual(result.base(), null);
    expectEqual(result.fork(), 8);
    expectEqual(result.next(), DiffResultType.Descend);
    expectEqual(result.next(), DiffResultType.Key);
    expectEqual(result.base(), null);
    expectEqual(result.fork(), name);
    expectEqual(result.next(), DiffResultType.Value);
    expectEqual(result.base(), null);
    expectEqual(result.fork(), "eight");

    expectEqual(result.next(), DiffResultType.Ascend);
    expectEqual(result.next(), DiffResultType.Key);
    expectEqual(result.base(), null);
    expectEqual(result.fork(), 9);
    expectEqual(result.next(), DiffResultType.Descend);
    expectEqual(result.next(), DiffResultType.Key);
    expectEqual(result.base(), null);
    expectEqual(result.fork(), name);
    expectEqual(result.next(), DiffResultType.Value);
    expectEqual(result.base(), null);
    expectEqual(result.fork(), "nine");

    expectEqual(result.next(), DiffResultType.Ascend);
    expectEqual(result.next(), DiffResultType.Ascend);
    expectEqual(result.next(), DiffResultType.End);

    context = dbms.patchContext(first);

    dbms.insert(context, Throw, numbers, 10, name, "ten");
    dbms.delete(context, numbers, 2);
    dbms.delete(context, numbers, 3);
    dbms.insert(context, Overwrite, numbers, 4, name, "quatro");
    dbms.delete(context, numbers, 5);
    dbms.delete(context, numbers, 35);
    dbms.insert(context, Throw, numbers, 25, name, "twenty-five");

    Revision second = dbms.commit(context);

    result = dbms.diff(first, second);

    expectEqual(result.next(), DiffResultType.Key);
    expectEqual(result.base(), numbers);
    expectEqual(result.fork(), numbers);

    expectEqual(result.next(), DiffResultType.Descend);
    expectEqual(result.next(), DiffResultType.Key);
    expectEqual(result.base(), 2);
    expectEqual(result.fork(), null);
    expectEqual(result.next(), DiffResultType.Descend);
    expectEqual(result.next(), DiffResultType.Key);
    expectEqual(result.base(), name);
    expectEqual(result.fork(), null);
    expectEqual(result.next(), DiffResultType.Value);
    expectEqual(result.base(), "two");
    expectEqual(result.fork(), null);

    expectEqual(result.next(), DiffResultType.Ascend);
    expectEqual(result.next(), DiffResultType.Key);
    expectEqual(result.base(), 3);
    expectEqual(result.fork(), null);
    expectEqual(result.next(), DiffResultType.Descend);
    expectEqual(result.next(), DiffResultType.Key);
    expectEqual(result.base(), name);
    expectEqual(result.fork(), null);
    expectEqual(result.next(), DiffResultType.Value);
    expectEqual(result.base(), "three");
    expectEqual(result.fork(), null);

    expectEqual(result.next(), DiffResultType.Ascend);
    expectEqual(result.next(), DiffResultType.Key);
    expectEqual(result.base(), 4);
    expectEqual(result.fork(), 4);
    expectEqual(result.next(), DiffResultType.Descend);
    expectEqual(result.next(), DiffResultType.Key);
    expectEqual(result.base(), name);
    expectEqual(result.fork(), name);
    expectEqual(result.next(), DiffResultType.Value);
    expectEqual(result.base(), "four");
    expectEqual(result.fork(), "quatro");

    expectEqual(result.next(), DiffResultType.Ascend);
    expectEqual(result.next(), DiffResultType.Key);
    expectEqual(result.base(), 5);
    expectEqual(result.fork(), null);
    expectEqual(result.next(), DiffResultType.Descend);
    expectEqual(result.next(), DiffResultType.Key);
    expectEqual(result.base(), name);
    expectEqual(result.fork(), null);
    expectEqual(result.next(), DiffResultType.Value);
    expectEqual(result.base(), "five");
    expectEqual(result.fork(), null);

    expectEqual(result.next(), DiffResultType.Ascend);
    expectEqual(result.next(), DiffResultType.Key);
    expectEqual(result.base(), null);
    expectEqual(result.fork(), 10);
    expectEqual(result.next(), DiffResultType.Descend);
    expectEqual(result.next(), DiffResultType.Key);
    expectEqual(result.base(), null);
    expectEqual(result.fork(), name);
    expectEqual(result.next(), DiffResultType.Value);
    expectEqual(result.base(), null);
    expectEqual(result.fork(), "ten");

    expectEqual(result.next(), DiffResultType.Ascend);
    expectEqual(result.next(), DiffResultType.Key);
    expectEqual(result.base(), null);
    expectEqual(result.fork(), 25);
    expectEqual(result.next(), DiffResultType.Descend);
    expectEqual(result.next(), DiffResultType.Key);
    expectEqual(result.base(), null);
    expectEqual(result.fork(), name);
    expectEqual(result.next(), DiffResultType.Value);
    expectEqual(result.base(), null);
    expectEqual(result.fork(), "twenty-five");

    expectEqual(result.next(), DiffResultType.Ascend);
    expectEqual(result.next(), DiffResultType.Ascend);
    expectEqual(result.next(), DiffResultType.End);
  }

  private static void apply(DBMS dbms, PatchContext context, DiffResult result)
  {
    final int MaxDepth = 16;
    Object[] path = new Object[MaxDepth];
    int depth = 0;
    while (true) {
      DiffResultType type = result.next();
      switch (type) {
      case End:
        return;

      case Descend:
        ++ depth;
        break;

      case Ascend:
        path[depth--] = null;
        break;

      case Key: {
        Object forkKey = result.fork();
        if (forkKey != null) {
          path[depth] = forkKey;
        } else {
          path[depth] = result.base();          
          dbms.delete(context, path, 0, depth + 1);
          result.skip();
        }
      } break;

      case Value: {
        path[depth + 1] = result.fork();
        dbms.insert(context, Overwrite, path, 0, depth + 2);
      } break;

      default:
        throw new RuntimeException("unexpected result type: " + type);
      }
    }
  }

  @Test
  public void testLowLevelDiffApplication() {
    DBMS dbms = new MyDBMS();

    Column number = dbms.column(Integer.class);
    Column name = dbms.column(String.class);
    Table numbers = dbms.table(list(number));

    Revision tail = dbms.revision();

    PatchContext context = dbms.patchContext(tail);

    dbms.insert(context, Throw, numbers, 1, name, "one");
    dbms.insert(context, Throw, numbers, 2, name, "two");
    dbms.insert(context, Throw, numbers, 3, name, "three");
    dbms.insert(context, Throw, numbers, 4, name, "four");
    dbms.insert(context, Throw, numbers, 5, name, "five");
    dbms.insert(context, Throw, numbers, 6, name, "six");
    dbms.insert(context, Throw, numbers, 7, name, "seven");
    dbms.insert(context, Throw, numbers, 8, name, "eight");
    dbms.insert(context, Throw, numbers, 9, name, "nine");

    Revision first = dbms.commit(context);

    context = dbms.patchContext(tail);

    apply(dbms, context, dbms.diff(tail, first));
    
    Revision firstApplied = dbms.commit(context);
    
    DiffResult result = dbms.diff(first, firstApplied);
    
    expectEqual(result.next(), DiffResultType.End);

    context = dbms.patchContext(first);

    dbms.insert(context, Throw, numbers, 10, name, "ten");
    dbms.delete(context, numbers, 2);
    dbms.delete(context, numbers, 3);
    dbms.insert(context, Overwrite, numbers, 4, name, "quatro");
    dbms.delete(context, numbers, 5);
    dbms.delete(context, numbers, 35);
    dbms.insert(context, Throw, numbers, 25, name, "twenty-five");

    Revision second = dbms.commit(context);

    context = dbms.patchContext(first);

    apply(dbms, context, dbms.diff(first, second));
    
    Revision secondApplied = dbms.commit(context);
    
    result = dbms.diff(second, secondApplied);

    expectEqual(result.next(), DiffResultType.End);

    context = dbms.patchContext(tail);

    apply(dbms, context, dbms.diff(tail, second));

    secondApplied = dbms.commit(context);
    
    result = dbms.diff(second, secondApplied);

    expectEqual(result.next(), DiffResultType.End);

    context = dbms.patchContext(firstApplied);

    apply(dbms, context, dbms.diff(first, second));

    secondApplied = dbms.commit(context);
    
    result = dbms.diff(second, secondApplied);

    expectEqual(result.next(), DiffResultType.End);
  }
}

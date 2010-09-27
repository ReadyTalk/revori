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

    assertEquals(result.next(), DiffResultType.Key);
    assertEquals(result.base(), null);
    assertEquals(result.fork(), numbers);

    assertEquals(result.next(), DiffResultType.Descend);
    assertEquals(result.next(), DiffResultType.Key);
    assertEquals(result.base(), null);
    assertEquals(result.fork(), 1);
    assertEquals(result.next(), DiffResultType.Descend);
    assertEquals(result.next(), DiffResultType.Key);
    assertEquals(result.base(), null);
    assertEquals(result.fork(), name);
    assertEquals(result.next(), DiffResultType.Value);
    assertEquals(result.base(), null);
    assertEquals(result.fork(), "one");

    assertEquals(result.next(), DiffResultType.Ascend);
    assertEquals(result.next(), DiffResultType.Key);
    assertEquals(result.base(), null);
    assertEquals(result.fork(), 2);
    assertEquals(result.next(), DiffResultType.Descend);
    assertEquals(result.next(), DiffResultType.Key);
    assertEquals(result.base(), null);
    assertEquals(result.fork(), name);
    assertEquals(result.next(), DiffResultType.Value);
    assertEquals(result.base(), null);
    assertEquals(result.fork(), "two");

    assertEquals(result.next(), DiffResultType.Ascend);
    assertEquals(result.next(), DiffResultType.Key);
    assertEquals(result.base(), null);
    assertEquals(result.fork(), 3);
    assertEquals(result.next(), DiffResultType.Descend);
    assertEquals(result.next(), DiffResultType.Key);
    assertEquals(result.base(), null);
    assertEquals(result.fork(), name);
    assertEquals(result.next(), DiffResultType.Value);
    assertEquals(result.base(), null);
    assertEquals(result.fork(), "three");

    assertEquals(result.next(), DiffResultType.Ascend);
    assertEquals(result.next(), DiffResultType.Key);
    assertEquals(result.base(), null);
    assertEquals(result.fork(), 4);
    assertEquals(result.next(), DiffResultType.Descend);
    assertEquals(result.next(), DiffResultType.Key);
    assertEquals(result.base(), null);
    assertEquals(result.fork(), name);
    assertEquals(result.next(), DiffResultType.Value);
    assertEquals(result.base(), null);
    assertEquals(result.fork(), "four");

    assertEquals(result.next(), DiffResultType.Ascend);
    assertEquals(result.next(), DiffResultType.Key);
    assertEquals(result.base(), null);
    assertEquals(result.fork(), 5);
    assertEquals(result.next(), DiffResultType.Descend);
    assertEquals(result.next(), DiffResultType.Key);
    assertEquals(result.base(), null);
    assertEquals(result.fork(), name);
    assertEquals(result.next(), DiffResultType.Value);
    assertEquals(result.base(), null);
    assertEquals(result.fork(), "five");

    assertEquals(result.next(), DiffResultType.Ascend);
    assertEquals(result.next(), DiffResultType.Key);
    assertEquals(result.base(), null);
    assertEquals(result.fork(), 6);
    assertEquals(result.next(), DiffResultType.Descend);
    assertEquals(result.next(), DiffResultType.Key);
    assertEquals(result.base(), null);
    assertEquals(result.fork(), name);
    assertEquals(result.next(), DiffResultType.Value);
    assertEquals(result.base(), null);
    assertEquals(result.fork(), "six");

    assertEquals(result.next(), DiffResultType.Ascend);
    assertEquals(result.next(), DiffResultType.Key);
    assertEquals(result.base(), null);
    assertEquals(result.fork(), 7);
    assertEquals(result.next(), DiffResultType.Descend);
    assertEquals(result.next(), DiffResultType.Key);
    assertEquals(result.base(), null);
    assertEquals(result.fork(), name);
    assertEquals(result.next(), DiffResultType.Value);
    assertEquals(result.base(), null);
    assertEquals(result.fork(), "seven");

    assertEquals(result.next(), DiffResultType.Ascend);
    assertEquals(result.next(), DiffResultType.Key);
    assertEquals(result.base(), null);
    assertEquals(result.fork(), 8);
    assertEquals(result.next(), DiffResultType.Descend);
    assertEquals(result.next(), DiffResultType.Key);
    assertEquals(result.base(), null);
    assertEquals(result.fork(), name);
    assertEquals(result.next(), DiffResultType.Value);
    assertEquals(result.base(), null);
    assertEquals(result.fork(), "eight");

    assertEquals(result.next(), DiffResultType.Ascend);
    assertEquals(result.next(), DiffResultType.Key);
    assertEquals(result.base(), null);
    assertEquals(result.fork(), 9);
    assertEquals(result.next(), DiffResultType.Descend);
    assertEquals(result.next(), DiffResultType.Key);
    assertEquals(result.base(), null);
    assertEquals(result.fork(), name);
    assertEquals(result.next(), DiffResultType.Value);
    assertEquals(result.base(), null);
    assertEquals(result.fork(), "nine");

    assertEquals(result.next(), DiffResultType.Ascend);
    assertEquals(result.next(), DiffResultType.Ascend);
    assertEquals(result.next(), DiffResultType.End);

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

    assertEquals(result.next(), DiffResultType.Key);
    assertEquals(result.base(), numbers);
    assertEquals(result.fork(), numbers);

    assertEquals(result.next(), DiffResultType.Descend);
    assertEquals(result.next(), DiffResultType.Key);
    assertEquals(result.base(), 2);
    assertEquals(result.fork(), null);
    assertEquals(result.next(), DiffResultType.Descend);
    assertEquals(result.next(), DiffResultType.Key);
    assertEquals(result.base(), name);
    assertEquals(result.fork(), null);
    assertEquals(result.next(), DiffResultType.Value);
    assertEquals(result.base(), "two");
    assertEquals(result.fork(), null);

    assertEquals(result.next(), DiffResultType.Descend);
    assertEquals(result.next(), DiffResultType.Key);
    assertEquals(result.base(), 3);
    assertEquals(result.fork(), null);
    assertEquals(result.next(), DiffResultType.Descend);
    assertEquals(result.next(), DiffResultType.Key);
    assertEquals(result.base(), name);
    assertEquals(result.fork(), null);
    assertEquals(result.next(), DiffResultType.Value);
    assertEquals(result.base(), "three");
    assertEquals(result.fork(), null);

    assertEquals(result.next(), DiffResultType.Descend);
    assertEquals(result.next(), DiffResultType.Key);
    assertEquals(result.base(), 4);
    assertEquals(result.fork(), 4);
    assertEquals(result.next(), DiffResultType.Descend);
    assertEquals(result.next(), DiffResultType.Key);
    assertEquals(result.base(), name);
    assertEquals(result.fork(), name);
    assertEquals(result.next(), DiffResultType.Value);
    assertEquals(result.base(), "four");
    assertEquals(result.fork(), "quatro");

    assertEquals(result.next(), DiffResultType.Descend);
    assertEquals(result.next(), DiffResultType.Key);
    assertEquals(result.base(), 5);
    assertEquals(result.fork(), null);
    assertEquals(result.next(), DiffResultType.Descend);
    assertEquals(result.next(), DiffResultType.Key);
    assertEquals(result.base(), name);
    assertEquals(result.fork(), null);
    assertEquals(result.next(), DiffResultType.Value);
    assertEquals(result.base(), "five");
    assertEquals(result.fork(), null);

    assertEquals(result.next(), DiffResultType.Ascend);
    assertEquals(result.next(), DiffResultType.Key);
    assertEquals(result.base(), null);
    assertEquals(result.fork(), 10);
    assertEquals(result.next(), DiffResultType.Descend);
    assertEquals(result.next(), DiffResultType.Key);
    assertEquals(result.base(), null);
    assertEquals(result.fork(), name);
    assertEquals(result.next(), DiffResultType.Value);
    assertEquals(result.base(), null);
    assertEquals(result.fork(), "ten");

    assertEquals(result.next(), DiffResultType.Ascend);
    assertEquals(result.next(), DiffResultType.Key);
    assertEquals(result.base(), null);
    assertEquals(result.fork(), 25);
    assertEquals(result.next(), DiffResultType.Descend);
    assertEquals(result.next(), DiffResultType.Key);
    assertEquals(result.base(), null);
    assertEquals(result.fork(), name);
    assertEquals(result.next(), DiffResultType.Value);
    assertEquals(result.base(), null);
    assertEquals(result.fork(), "twenty-five");

    assertEquals(result.next(), DiffResultType.Ascend);
    assertEquals(result.next(), DiffResultType.Ascend);
    assertEquals(result.next(), DiffResultType.End);
  }

  private static void apply(DBMS dbms, PatchContext context, DiffResult result)
  {
    final int MaxDepth = 16;
    Object[] path = new Object[MaxDepth];
    int depth = 0;
    boolean done = false;
    while (! done) {
      DiffResultType type = result.next();
      switch (type) {
      case End:
        done = true;
        break;

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
          dbms.delete(context, path, depth + 1);
          result.skip();
        }
      } break;

      case Value: {
        path[depth + 1] = result.fork();
        dbms.insert
          (context, Overwrite, path, depth + 2);
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
    
    assertEquals(result.next(), DiffResultType.End);

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

    assertEquals(result.next(), DiffResultType.End);

    context = dbms.patchContext(tail);

    apply(dbms, context, dbms.diff(tail, second));

    secondApplied = dbms.commit(context);
    
    result = dbms.diff(second, secondApplied);

    assertEquals(result.next(), DiffResultType.End);

    context = dbms.patchContext(firstApplied);

    apply(dbms, context, dbms.diff(first, second));

    secondApplied = dbms.commit(context);
    
    result = dbms.diff(second, secondApplied);

    assertEquals(result.next(), DiffResultType.End);
  }
}

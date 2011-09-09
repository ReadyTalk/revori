package unittests;

import junit.framework.TestCase;

import org.junit.Test;
import static com.readytalk.oss.dbms.util.Util.cols;
import static com.readytalk.oss.dbms.DuplicateKeyResolution.Throw;
import static com.readytalk.oss.dbms.DuplicateKeyResolution.Overwrite;

import com.readytalk.oss.dbms.Column;
import com.readytalk.oss.dbms.Revisions;
import com.readytalk.oss.dbms.Table;
import com.readytalk.oss.dbms.Revision;
import com.readytalk.oss.dbms.RevisionBuilder;
import com.readytalk.oss.dbms.DiffResult;

public class LowLevel extends TestCase{
  private static void expectEqual(Object actual, Object expected) {
    assertEquals(expected, actual);
  }

  @Test
  public void testLowLevelDiffs() {
    Column<Integer> number = new Column<Integer>(Integer.class);
    Column<String> name = new Column<String>(String.class);
    Table numbers = new Table(cols(number));

    Revision tail = Revisions.Empty;

    RevisionBuilder builder = tail.builder();

    builder.insert(Throw, numbers, 1, name, "one");
    builder.insert(Throw, numbers, 2, name, "two");
    builder.insert(Throw, numbers, 3, name, "three");
    builder.insert(Throw, numbers, 4, name, "four");
    builder.insert(Throw, numbers, 5, name, "five");
    builder.insert(Throw, numbers, 6, name, "six");
    builder.insert(Throw, numbers, 7, name, "seven");
    builder.insert(Throw, numbers, 8, name, "eight");
    builder.insert(Throw, numbers, 9, name, "nine");

    Revision first = builder.commit();

    DiffResult result = tail.diff(first, false);

    expectEqual(result.next(), DiffResult.Type.Key);
    expectEqual(result.base(), null);
    expectEqual(result.fork(), numbers);

    expectEqual(result.next(), DiffResult.Type.Descend);
    expectEqual(result.next(), DiffResult.Type.Key);
    expectEqual(result.base(), null);
    expectEqual(result.fork(), 1);
    expectEqual(result.next(), DiffResult.Type.Descend);
    expectEqual(result.next(), DiffResult.Type.Key);
    expectEqual(result.base(), null);
    expectEqual(result.fork(), name);
    expectEqual(result.next(), DiffResult.Type.Value);
    expectEqual(result.base(), null);
    expectEqual(result.fork(), "one");

    expectEqual(result.next(), DiffResult.Type.Ascend);
    expectEqual(result.next(), DiffResult.Type.Key);
    expectEqual(result.base(), null);
    expectEqual(result.fork(), 2);
    expectEqual(result.next(), DiffResult.Type.Descend);
    expectEqual(result.next(), DiffResult.Type.Key);
    expectEqual(result.base(), null);
    expectEqual(result.fork(), name);
    expectEqual(result.next(), DiffResult.Type.Value);
    expectEqual(result.base(), null);
    expectEqual(result.fork(), "two");

    expectEqual(result.next(), DiffResult.Type.Ascend);
    expectEqual(result.next(), DiffResult.Type.Key);
    expectEqual(result.base(), null);
    expectEqual(result.fork(), 3);
    expectEqual(result.next(), DiffResult.Type.Descend);
    expectEqual(result.next(), DiffResult.Type.Key);
    expectEqual(result.base(), null);
    expectEqual(result.fork(), name);
    expectEqual(result.next(), DiffResult.Type.Value);
    expectEqual(result.base(), null);
    expectEqual(result.fork(), "three");

    expectEqual(result.next(), DiffResult.Type.Ascend);
    expectEqual(result.next(), DiffResult.Type.Key);
    expectEqual(result.base(), null);
    expectEqual(result.fork(), 4);
    expectEqual(result.next(), DiffResult.Type.Descend);
    expectEqual(result.next(), DiffResult.Type.Key);
    expectEqual(result.base(), null);
    expectEqual(result.fork(), name);
    expectEqual(result.next(), DiffResult.Type.Value);
    expectEqual(result.base(), null);
    expectEqual(result.fork(), "four");

    expectEqual(result.next(), DiffResult.Type.Ascend);
    expectEqual(result.next(), DiffResult.Type.Key);
    expectEqual(result.base(), null);
    expectEqual(result.fork(), 5);
    expectEqual(result.next(), DiffResult.Type.Descend);
    expectEqual(result.next(), DiffResult.Type.Key);
    expectEqual(result.base(), null);
    expectEqual(result.fork(), name);
    expectEqual(result.next(), DiffResult.Type.Value);
    expectEqual(result.base(), null);
    expectEqual(result.fork(), "five");

    expectEqual(result.next(), DiffResult.Type.Ascend);
    expectEqual(result.next(), DiffResult.Type.Key);
    expectEqual(result.base(), null);
    expectEqual(result.fork(), 6);
    expectEqual(result.next(), DiffResult.Type.Descend);
    expectEqual(result.next(), DiffResult.Type.Key);
    expectEqual(result.base(), null);
    expectEqual(result.fork(), name);
    expectEqual(result.next(), DiffResult.Type.Value);
    expectEqual(result.base(), null);
    expectEqual(result.fork(), "six");

    expectEqual(result.next(), DiffResult.Type.Ascend);
    expectEqual(result.next(), DiffResult.Type.Key);
    expectEqual(result.base(), null);
    expectEqual(result.fork(), 7);
    expectEqual(result.next(), DiffResult.Type.Descend);
    expectEqual(result.next(), DiffResult.Type.Key);
    expectEqual(result.base(), null);
    expectEqual(result.fork(), name);
    expectEqual(result.next(), DiffResult.Type.Value);
    expectEqual(result.base(), null);
    expectEqual(result.fork(), "seven");

    expectEqual(result.next(), DiffResult.Type.Ascend);
    expectEqual(result.next(), DiffResult.Type.Key);
    expectEqual(result.base(), null);
    expectEqual(result.fork(), 8);
    expectEqual(result.next(), DiffResult.Type.Descend);
    expectEqual(result.next(), DiffResult.Type.Key);
    expectEqual(result.base(), null);
    expectEqual(result.fork(), name);
    expectEqual(result.next(), DiffResult.Type.Value);
    expectEqual(result.base(), null);
    expectEqual(result.fork(), "eight");

    expectEqual(result.next(), DiffResult.Type.Ascend);
    expectEqual(result.next(), DiffResult.Type.Key);
    expectEqual(result.base(), null);
    expectEqual(result.fork(), 9);
    expectEqual(result.next(), DiffResult.Type.Descend);
    expectEqual(result.next(), DiffResult.Type.Key);
    expectEqual(result.base(), null);
    expectEqual(result.fork(), name);
    expectEqual(result.next(), DiffResult.Type.Value);
    expectEqual(result.base(), null);
    expectEqual(result.fork(), "nine");

    expectEqual(result.next(), DiffResult.Type.Ascend);
    expectEqual(result.next(), DiffResult.Type.Ascend);
    expectEqual(result.next(), DiffResult.Type.End);

    builder = first.builder();

    builder.insert(Throw, numbers, 10, name, "ten");
    builder.delete(numbers, 2);
    builder.delete(numbers, 3);
    builder.insert(Overwrite, numbers, 4, name, "quatro");
    builder.delete(numbers, 5);
    builder.delete(numbers, 35);
    builder.insert(Throw, numbers, 25, name, "twenty-five");

    Revision second = builder.commit();

    result = first.diff(second, false);

    expectEqual(result.next(), DiffResult.Type.Key);
    expectEqual(result.base(), numbers);
    expectEqual(result.fork(), numbers);

    expectEqual(result.next(), DiffResult.Type.Descend);
    expectEqual(result.next(), DiffResult.Type.Key);
    expectEqual(result.base(), 2);
    expectEqual(result.fork(), null);
    expectEqual(result.next(), DiffResult.Type.Descend);
    expectEqual(result.next(), DiffResult.Type.Key);
    expectEqual(result.base(), name);
    expectEqual(result.fork(), null);
    expectEqual(result.next(), DiffResult.Type.Value);
    expectEqual(result.base(), "two");
    expectEqual(result.fork(), null);

    expectEqual(result.next(), DiffResult.Type.Ascend);
    expectEqual(result.next(), DiffResult.Type.Key);
    expectEqual(result.base(), 3);
    expectEqual(result.fork(), null);
    expectEqual(result.next(), DiffResult.Type.Descend);
    expectEqual(result.next(), DiffResult.Type.Key);
    expectEqual(result.base(), name);
    expectEqual(result.fork(), null);
    expectEqual(result.next(), DiffResult.Type.Value);
    expectEqual(result.base(), "three");
    expectEqual(result.fork(), null);

    expectEqual(result.next(), DiffResult.Type.Ascend);
    expectEqual(result.next(), DiffResult.Type.Key);
    expectEqual(result.base(), 4);
    expectEqual(result.fork(), 4);
    expectEqual(result.next(), DiffResult.Type.Descend);
    expectEqual(result.next(), DiffResult.Type.Key);
    expectEqual(result.base(), name);
    expectEqual(result.fork(), name);
    expectEqual(result.next(), DiffResult.Type.Value);
    expectEqual(result.base(), "four");
    expectEqual(result.fork(), "quatro");

    expectEqual(result.next(), DiffResult.Type.Ascend);
    expectEqual(result.next(), DiffResult.Type.Key);
    expectEqual(result.base(), 5);
    expectEqual(result.fork(), null);
    expectEqual(result.next(), DiffResult.Type.Descend);
    expectEqual(result.next(), DiffResult.Type.Key);
    expectEqual(result.base(), name);
    expectEqual(result.fork(), null);
    expectEqual(result.next(), DiffResult.Type.Value);
    expectEqual(result.base(), "five");
    expectEqual(result.fork(), null);

    expectEqual(result.next(), DiffResult.Type.Ascend);
    expectEqual(result.next(), DiffResult.Type.Key);
    expectEqual(result.base(), null);
    expectEqual(result.fork(), 10);
    expectEqual(result.next(), DiffResult.Type.Descend);
    expectEqual(result.next(), DiffResult.Type.Key);
    expectEqual(result.base(), null);
    expectEqual(result.fork(), name);
    expectEqual(result.next(), DiffResult.Type.Value);
    expectEqual(result.base(), null);
    expectEqual(result.fork(), "ten");

    expectEqual(result.next(), DiffResult.Type.Ascend);
    expectEqual(result.next(), DiffResult.Type.Key);
    expectEqual(result.base(), null);
    expectEqual(result.fork(), 25);
    expectEqual(result.next(), DiffResult.Type.Descend);
    expectEqual(result.next(), DiffResult.Type.Key);
    expectEqual(result.base(), null);
    expectEqual(result.fork(), name);
    expectEqual(result.next(), DiffResult.Type.Value);
    expectEqual(result.base(), null);
    expectEqual(result.fork(), "twenty-five");

    expectEqual(result.next(), DiffResult.Type.Ascend);
    expectEqual(result.next(), DiffResult.Type.Ascend);
    expectEqual(result.next(), DiffResult.Type.End);

    { builder = tail.builder();

      builder.insert(Throw, numbers, 1);
      builder.insert(Throw, numbers, 2);

      Revision fork = builder.commit();

      expectEqual(fork.query(numbers.primaryKey, 1, number), 1);
      expectEqual(fork.query(numbers.primaryKey, 2, number), 2);

      result = tail.diff(fork, false);

      expectEqual(result.next(), DiffResult.Type.Key);
      expectEqual(result.base(), null);
      expectEqual(result.fork(), numbers);

      expectEqual(result.next(), DiffResult.Type.Descend);
      expectEqual(result.next(), DiffResult.Type.Key);
      expectEqual(result.base(), null);
      expectEqual(result.fork(), 1);

      expectEqual(result.next(), DiffResult.Type.Key);
      expectEqual(result.base(), null);
      expectEqual(result.fork(), 2);

      expectEqual(result.next(), DiffResult.Type.Ascend);
      expectEqual(result.next(), DiffResult.Type.End);
    }
  }

  private static void apply(RevisionBuilder builder, DiffResult result)
  {
    final int MaxDepth = 16;
    Object[] path = new Object[MaxDepth];
    int depth = 0;
    while (true) {
      DiffResult.Type type = result.next();
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
          builder.delete(path, 0, depth + 1);
          result.skip();
        }
      } break;

      case Value: {
        path[depth + 1] = result.fork();
        builder.insert(Overwrite, path, 0, depth + 2);
      } break;

      default:
        throw new RuntimeException("unexpected result type: " + type);
      }
    }
  }

  @Test
  public void testLowLevelDiffApplication() {
    Column<Integer> number = new Column<Integer>(Integer.class);
    Column<String> name = new Column<String>(String.class);
    Table numbers = new Table(cols(number));

    Revision tail = Revisions.Empty;

    RevisionBuilder builder = tail.builder();

    builder.insert(Throw, numbers, 1, name, "one");
    builder.insert(Throw, numbers, 2, name, "two");
    builder.insert(Throw, numbers, 3, name, "three");
    builder.insert(Throw, numbers, 4, name, "four");
    builder.insert(Throw, numbers, 5, name, "five");
    builder.insert(Throw, numbers, 6, name, "six");
    builder.insert(Throw, numbers, 7, name, "seven");
    builder.insert(Throw, numbers, 8, name, "eight");
    builder.insert(Throw, numbers, 9, name, "nine");

    Revision first = builder.commit();

    builder = tail.builder();

    apply(builder, tail.diff(first, false));
    
    Revision firstApplied = builder.commit();
    
    DiffResult result = first.diff(firstApplied, false);
    
    expectEqual(result.next(), DiffResult.Type.End);

    builder = first.builder();

    builder.insert(Throw, numbers, 10, name, "ten");
    builder.delete(numbers, 2);
    builder.delete(numbers, 3);
    builder.insert(Overwrite, numbers, 4, name, "quatro");
    builder.delete(numbers, 5);
    builder.delete(numbers, 35);
    builder.insert(Throw, numbers, 25, name, "twenty-five");

    Revision second = builder.commit();

    builder = first.builder();

    apply(builder, first.diff(second, false));
    
    Revision secondApplied = builder.commit();
    
    result = second.diff(secondApplied, false);

    expectEqual(result.next(), DiffResult.Type.End);

    builder = tail.builder();

    apply(builder, tail.diff(second, false));

    secondApplied = builder.commit();
    
    result = second.diff(secondApplied, false);

    expectEqual(result.next(), DiffResult.Type.End);

    builder = firstApplied.builder();

    apply(builder, first.diff(second, false));

    secondApplied = builder.commit();
    
    result = second.diff(secondApplied, false);

    expectEqual(result.next(), DiffResult.Type.End);
  }
}

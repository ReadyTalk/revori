package unittests;

import static com.readytalk.revori.util.Util.cols;
import static com.readytalk.revori.DuplicateKeyResolution.Throw;

import com.readytalk.revori.Table;
import com.readytalk.revori.Column;
import com.readytalk.revori.Revision;
import com.readytalk.revori.RevisionBuilder;
import com.readytalk.revori.server.RevisionServer;
import com.readytalk.revori.server.SimpleRevisionServer;
import com.readytalk.revori.server.Bridge;

import org.junit.Test;
import junit.framework.TestCase;

public class BridgeTest extends TestCase {
  private static void expectEqual(Object actual, Object expected) {
    assertEquals(expected, actual);
  }

  @Test
  public void testBasic() {
    RevisionServer left = new SimpleRevisionServer(null, null);
    RevisionServer right = new SimpleRevisionServer(null, null);

    Bridge bridge = new Bridge();

    Column<Integer> number = new Column<Integer>(Integer.class, "number");
    Column<String> name = new Column<String>(String.class, "name");
    Table numbers = new Table(cols(number), "numbers");
    Bridge.Path path = new Bridge.Path(numbers);

    bridge.register(left, path, right, path);

    { Revision base = left.head();
      RevisionBuilder builder = base.builder();

      builder.insert(Throw, numbers, 1, name, "one");

      left.merge(base, builder.commit());
    }

    expectEqual(left.head().query(numbers.primaryKey, 1, name), "one");
    expectEqual(right.head().query(numbers.primaryKey, 1, name), "one");

    { Revision base = right.head();
      RevisionBuilder builder = base.builder();

      builder.insert(Throw, numbers, 2, name, "two");

      right.merge(base, builder.commit());
    }

    expectEqual(left.head().query(numbers.primaryKey, 2, name), "two");
    expectEqual(right.head().query(numbers.primaryKey, 2, name), "two");
  }

  @Test
  public void testAggregate() {
    RevisionServer left1 = new SimpleRevisionServer(null, null);
    RevisionServer left2 = new SimpleRevisionServer(null, null);
    RevisionServer right = new SimpleRevisionServer(null, null);

    Bridge bridge = new Bridge();

    Column<Integer> number = new Column<Integer>(Integer.class, "number");
    Column<String> name = new Column<String>(String.class, "name");
    Table numbers = new Table(cols(number), "numbers");
    Bridge.Path leftPath = new Bridge.Path(numbers);

    Column<Integer> origin = new Column<Integer>(Integer.class, "origin");
    Table originNumbers = new Table(cols(origin, number), "origin numbers");

    bridge.register(left1, leftPath, right, new Bridge.Path(originNumbers, 1));
    bridge.register(left2, leftPath, right, new Bridge.Path(originNumbers, 2));

    { Revision base = left1.head();
      RevisionBuilder builder = base.builder();

      builder.insert(Throw, numbers, 1, name, "one");

      left1.merge(base, builder.commit());
    }

    expectEqual(left1.head().query(numbers.primaryKey, 1, name), "one");
    expectEqual
      (left1.head().query(originNumbers.primaryKey, 1, 1, name), null);
    expectEqual(left2.head().query(numbers.primaryKey, 1, name), null);
    expectEqual
      (left2.head().query(originNumbers.primaryKey, 1, 1, name), null);
    expectEqual(right.head().query(numbers.primaryKey, 1, name), null);
    expectEqual
      (right.head().query(originNumbers.primaryKey, 1, 1, name), "one");

    { Revision base = right.head();
      RevisionBuilder builder = base.builder();

      builder.insert(Throw, originNumbers, 2, 2, name, "two");

      right.merge(base, builder.commit());
    }

    expectEqual(left1.head().query(numbers.primaryKey, 2, name), null);
    expectEqual
      (left1.head().query(originNumbers.primaryKey, 2, 2, name), null);
    expectEqual(left2.head().query(numbers.primaryKey, 2, name), "two");
    expectEqual
      (left2.head().query(originNumbers.primaryKey, 2, 2, name), null);
    expectEqual(right.head().query(numbers.primaryKey, 2, name), null);
    expectEqual
      (right.head().query(originNumbers.primaryKey, 2, 2, name), "two");

    { Revision base = right.head();
      RevisionBuilder builder = base.builder();

      builder.insert(Throw, originNumbers, 1, 2, name, "two");

      right.merge(base, builder.commit());
    }

    expectEqual(left1.head().query(numbers.primaryKey, 2, name), "two");
    expectEqual
      (left1.head().query(originNumbers.primaryKey, 1, 2, name), null);
    expectEqual(left2.head().query(numbers.primaryKey, 2, name), "two");
    expectEqual
      (left2.head().query(originNumbers.primaryKey, 1, 2, name), null);
    expectEqual(right.head().query(numbers.primaryKey, 2, name), null);
    expectEqual
      (right.head().query(originNumbers.primaryKey, 1, 2, name), "two");
    expectEqual
      (right.head().query(originNumbers.primaryKey, 2, 2, name), "two");

    { Revision base = right.head();
      RevisionBuilder builder = base.builder();

      builder.delete(originNumbers, 1, 2);

      right.merge(base, builder.commit());
    }

    expectEqual(left1.head().query(numbers.primaryKey, 2, name), null);
    expectEqual(left2.head().query(numbers.primaryKey, 2, name), "two");
    expectEqual(right.head().query(numbers.primaryKey, 2, name), null);
    expectEqual
      (right.head().query(originNumbers.primaryKey, 1, 2, name), null);
    expectEqual
      (right.head().query(originNumbers.primaryKey, 2, 2, name), "two");

    { Revision base = left2.head();
      RevisionBuilder builder = base.builder();

      builder.delete(numbers, 2);

      left2.merge(base, builder.commit());
    }

    expectEqual(left2.head().query(numbers.primaryKey, 2, name), null);
    expectEqual
      (right.head().query(originNumbers.primaryKey, 2, 2, name), null);
  }
}

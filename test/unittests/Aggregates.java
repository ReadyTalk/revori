package unittests;

import static com.readytalk.oss.dbms.util.Util.list;
import static com.readytalk.oss.dbms.util.Util.set;
import static com.readytalk.oss.dbms.DuplicateKeyResolution.Throw;
import static com.readytalk.oss.dbms.ExpressionFactory.equal;
import static com.readytalk.oss.dbms.ExpressionFactory.reference;
import static com.readytalk.oss.dbms.ExpressionFactory.aggregate;
import static com.readytalk.oss.dbms.ExpressionFactory.constant;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

import junit.framework.TestCase;

import com.readytalk.oss.dbms.Column;
import com.readytalk.oss.dbms.Table;
import com.readytalk.oss.dbms.TableReference;
import com.readytalk.oss.dbms.Revision;
import com.readytalk.oss.dbms.Revisions;
import com.readytalk.oss.dbms.RevisionBuilder;
import com.readytalk.oss.dbms.QueryResult;
import com.readytalk.oss.dbms.QueryTemplate;
import com.readytalk.oss.dbms.Foldables;

public class Aggregates extends TestCase {
  private static void expectEqual(Object actual, Object expected) {
    assertEquals(expected, actual);
  }

  @Test
  public void testCount() {
    Column number = new Column(Integer.class);
    Column name = new Column(String.class);
    Table things = new Table(list(number));
    
    RevisionBuilder builder = Revisions.Empty.builder();

    builder.insert(Throw, things, 1, name, "tree");
    builder.insert(Throw, things, 2, name, "truck");
    builder.insert(Throw, things, 3, name, "planet");
    builder.insert(Throw, things, 4, name, "planet");
    builder.insert(Throw, things, 5, name, "tree");
    builder.insert(Throw, things, 6, name, "tree");

    Revision head = builder.commit();

    TableReference thingsReference = new TableReference(things);

    QueryResult result = Revisions.Empty.diff
      (head, new QueryTemplate
       (list
        (aggregate(Integer.class, Foldables.Count)),
        thingsReference, constant(true)));

    expectEqual(result.nextRow(), QueryResult.Type.Inserted);
    expectEqual(result.nextItem(), 6);
    expectEqual(result.nextRow(), QueryResult.Type.End);

    result = Revisions.Empty.diff
      (head, new QueryTemplate
       (list(reference(thingsReference, name),
             aggregate(Integer.class, Foldables.Count)),
        thingsReference, constant(true),
        set(reference(thingsReference, name))));

    expectEqual(result.nextRow(), QueryResult.Type.Inserted);
    expectEqual(result.nextItem(), "planet");
    expectEqual(result.nextItem(), 2);
    expectEqual(result.nextRow(), QueryResult.Type.Inserted);
    expectEqual(result.nextItem(), "tree");
    expectEqual(result.nextItem(), 3);
    expectEqual(result.nextRow(), QueryResult.Type.Inserted);
    expectEqual(result.nextItem(), "truck");
    expectEqual(result.nextItem(), 1);
    expectEqual(result.nextRow(), QueryResult.Type.End);

    result = Revisions.Empty.diff
      (head, new QueryTemplate
       (list(reference(thingsReference, name),
             aggregate(Integer.class, Foldables.Count)),
        thingsReference, equal
        (aggregate(Integer.class, Foldables.Count), constant(3)),
        set(reference(thingsReference, name))));

    expectEqual(result.nextRow(), QueryResult.Type.Inserted);
    expectEqual(result.nextItem(), "tree");
    expectEqual(result.nextItem(), 3);
    expectEqual(result.nextRow(), QueryResult.Type.End);
  }
}

package unittests;

import static com.readytalk.oss.dbms.util.Util.list;
import static com.readytalk.oss.dbms.util.Util.cols;
import static com.readytalk.oss.dbms.util.Util.set;
import static com.readytalk.oss.dbms.DuplicateKeyResolution.Throw;
import static com.readytalk.oss.dbms.ExpressionFactory.equal;
import static com.readytalk.oss.dbms.ExpressionFactory.reference;
import static com.readytalk.oss.dbms.ExpressionFactory.aggregate;
import static com.readytalk.oss.dbms.ExpressionFactory.constant;

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
import com.readytalk.oss.dbms.View;
import com.readytalk.oss.dbms.Expression;
import com.readytalk.oss.dbms.Comparators;

import java.util.Collections;
import java.util.Set;

public class Views extends TestCase {
  private static void expectEqual(Object actual, Object expected) {
    assertEquals(expected, actual);
  }

  @Test
  public void testCount() {
    Column<Integer> number = new Column<Integer>(Integer.class, "number");
    Column<String> name = new Column<String>(String.class, "name");
    Table things = new Table(cols(number), "things");
    
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

    QueryTemplate group = new QueryTemplate
      (list(reference(thingsReference, name),
            aggregate(Integer.class, Foldables.Count)),
       thingsReference, constant(true),
       set(reference(thingsReference, name)));
    
    result = Revisions.Empty.diff(head, group);

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

    Revision base = head;

    builder = base.builder();

    builder.delete(things, 6);

    head = builder.commit();

    result = Revisions.Empty.diff(head, group);

    expectEqual(result.nextRow(), QueryResult.Type.Inserted);
    expectEqual(result.nextItem(), "planet");
    expectEqual(result.nextItem(), 2);
    expectEqual(result.nextRow(), QueryResult.Type.Inserted);
    expectEqual(result.nextItem(), "tree");
    expectEqual(result.nextItem(), 2);
    expectEqual(result.nextRow(), QueryResult.Type.Inserted);
    expectEqual(result.nextItem(), "truck");
    expectEqual(result.nextItem(), 1);
    expectEqual(result.nextRow(), QueryResult.Type.End);

    result = base.diff(head, group);

    expectEqual(result.nextRow(), QueryResult.Type.Inserted);
    expectEqual(result.nextItem(), "tree");
    expectEqual(result.nextItem(), -1);
    expectEqual(result.nextRow(), QueryResult.Type.End);
  }

  @Test
  public void testView() {
    Column<Integer> number = new Column<Integer>(Integer.class, "number");
    Column<String> name = new Column<String>(String.class, "name");
    Table things = new Table(cols(number), "things");
    
    RevisionBuilder builder = Revisions.Empty.builder();

    builder.insert(Throw, things, 1, name, "tree");
    builder.insert(Throw, things, 2, name, "truck");
    builder.insert(Throw, things, 3, name, "planet");
    builder.insert(Throw, things, 4, name, "planet");
    builder.insert(Throw, things, 5, name, "tree");
    builder.insert(Throw, things, 6, name, "tree");

    TableReference thingsReference = new TableReference(things);

    Column<Integer> sum = new Column<Integer>(Integer.class, "sum");

    View view = new View
      (new QueryTemplate
       (list(reference(thingsReference, name),
             aggregate(Integer.class, Foldables.Sum,
                       reference(thingsReference, number))),
        thingsReference, constant(true),
        set(reference(thingsReference, name))),
       Collections.emptyList(),
       cols(name, sum),
       cols(name),
       list(reference(thingsReference, name)),
       "view");

    builder.add(view);

    Revision head = builder.commit();
    
    TableReference viewReference = new TableReference(view.table);

    QueryTemplate viewQuery = new QueryTemplate
      (list(reference(viewReference, name), reference(viewReference, sum)),
       viewReference, constant(true));

    QueryResult result = Revisions.Empty.diff(head, viewQuery);

    expectEqual(result.nextRow(), QueryResult.Type.Inserted);
    expectEqual(result.nextItem(), "planet");
    expectEqual(result.nextItem(), 7);
    expectEqual(result.nextRow(), QueryResult.Type.Inserted);
    expectEqual(result.nextItem(), "tree");
    expectEqual(result.nextItem(), 12);
    expectEqual(result.nextRow(), QueryResult.Type.Inserted);
    expectEqual(result.nextItem(), "truck");
    expectEqual(result.nextItem(), 2);
    expectEqual(result.nextRow(), QueryResult.Type.End);

    builder = head.builder();

    builder.insert(Throw, things, 7, name, "tree");
    builder.delete(things, 2);

    head = builder.commit();
    
    result = Revisions.Empty.diff(head, viewQuery);

    expectEqual(result.nextRow(), QueryResult.Type.Inserted);
    expectEqual(result.nextItem(), "planet");
    expectEqual(result.nextItem(), 7);
    expectEqual(result.nextRow(), QueryResult.Type.Inserted);
    expectEqual(result.nextItem(), "tree");
    expectEqual(result.nextItem(), 19);
    expectEqual(result.nextRow(), QueryResult.Type.End);
  }

  @Test
  public void testOrderBy() {
    Column<Integer> number = new Column<Integer>(Integer.class, "number");
    Column<String> name = new Column<String>(String.class, "name");
    Table things = new Table(cols(number), "things");
    
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
        (reference(thingsReference, number), reference(thingsReference, name)),
        thingsReference, constant(true),
        (Set<Expression>) (Set) Collections.emptySet(),
        list(new QueryTemplate.OrderExpression
             (reference(thingsReference, name), Comparators.Ascending))));

    expectEqual(result.nextRow(), QueryResult.Type.Inserted);
    expectEqual(result.nextItem(), 3);
    expectEqual(result.nextItem(), "planet");
    expectEqual(result.nextRow(), QueryResult.Type.Inserted);
    expectEqual(result.nextItem(), 4);
    expectEqual(result.nextItem(), "planet");
    expectEqual(result.nextRow(), QueryResult.Type.Inserted);
    expectEqual(result.nextItem(), 1);
    expectEqual(result.nextItem(), "tree");
    expectEqual(result.nextRow(), QueryResult.Type.Inserted);
    expectEqual(result.nextItem(), 5);
    expectEqual(result.nextItem(), "tree");
    expectEqual(result.nextRow(), QueryResult.Type.Inserted);
    expectEqual(result.nextItem(), 6);
    expectEqual(result.nextItem(), "tree");
    expectEqual(result.nextRow(), QueryResult.Type.Inserted);
    expectEqual(result.nextItem(), 2);
    expectEqual(result.nextItem(), "truck");
    expectEqual(result.nextRow(), QueryResult.Type.End);

    result = Revisions.Empty.diff
      (head, new QueryTemplate
       (list
        (reference(thingsReference, number), reference(thingsReference, name)),
        thingsReference, constant(true),
        (Set<Expression>) (Set) Collections.emptySet(),
        list(new QueryTemplate.OrderExpression
             (reference(thingsReference, name), Comparators.Descending))));

    expectEqual(result.nextRow(), QueryResult.Type.Inserted);
    expectEqual(result.nextItem(), 2);
    expectEqual(result.nextItem(), "truck");
    expectEqual(result.nextRow(), QueryResult.Type.Inserted);
    expectEqual(result.nextItem(), 1);
    expectEqual(result.nextItem(), "tree");
    expectEqual(result.nextRow(), QueryResult.Type.Inserted);
    expectEqual(result.nextItem(), 5);
    expectEqual(result.nextItem(), "tree");
    expectEqual(result.nextRow(), QueryResult.Type.Inserted);
    expectEqual(result.nextItem(), 6);
    expectEqual(result.nextItem(), "tree");
    expectEqual(result.nextRow(), QueryResult.Type.Inserted);
    expectEqual(result.nextItem(), 3);
    expectEqual(result.nextItem(), "planet");
    expectEqual(result.nextRow(), QueryResult.Type.Inserted);
    expectEqual(result.nextItem(), 4);
    expectEqual(result.nextItem(), "planet");
    expectEqual(result.nextRow(), QueryResult.Type.End);
  }
}

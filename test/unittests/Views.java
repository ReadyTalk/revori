/* Copyright (c) 2010-2012, Revori Contributors

   Permission to use, copy, modify, and/or distribute this software
   for any purpose with or without fee is hereby granted, provided
   that the above copyright notice and this permission notice appear
   in all copies. */

package unittests;

import static com.readytalk.revori.util.Util.list;
import static com.readytalk.revori.util.Util.cols;
import static com.readytalk.revori.util.Util.set;
import static com.readytalk.revori.DuplicateKeyResolution.Throw;
import static com.readytalk.revori.ExpressionFactory.equal;
import static com.readytalk.revori.ExpressionFactory.reference;
import static com.readytalk.revori.ExpressionFactory.aggregate;
import static com.readytalk.revori.ExpressionFactory.constant;

import org.junit.Test;

import junit.framework.TestCase;

import com.readytalk.revori.Column;
import com.readytalk.revori.Table;
import com.readytalk.revori.TableReference;
import com.readytalk.revori.Revision;
import com.readytalk.revori.Revisions;
import com.readytalk.revori.RevisionBuilder;
import com.readytalk.revori.QueryResult;
import com.readytalk.revori.QueryTemplate;
import com.readytalk.revori.Foldables;
import com.readytalk.revori.View;
import com.readytalk.revori.Expression;
import com.readytalk.revori.Comparators;
import com.readytalk.revori.ConflictResolver;
import com.readytalk.revori.ForeignKeyResolvers;

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

    TableReference thingsReference = new TableReference(things);

    QueryResult result = Revisions.Empty.diff
      (Revisions.Empty, new QueryTemplate
       (list
        (aggregate(Integer.class, Foldables.Count)),
        thingsReference, constant(true)));

    expectEqual(result.nextRow(), QueryResult.Type.Inserted);
    expectEqual(result.nextItem(), 0);
    expectEqual(result.nextRow(), QueryResult.Type.End);

    result = Revisions.Empty.diff
      (Revisions.Empty.builder().table(things).row(1).update(name, "pumpkin")
       .table(things).delete(1).commit(), new QueryTemplate
       (list
        (aggregate(Integer.class, Foldables.Count)),
        thingsReference, constant(true)));

    expectEqual(result.nextRow(), QueryResult.Type.Inserted);
    expectEqual(result.nextItem(), 0);
    expectEqual(result.nextRow(), QueryResult.Type.End);

    result = Revisions.Empty.diff
      (Revisions.Empty.builder().table(things).row(1).update(name, "pumpkin")
       .table(things).delete(1).commit(), new QueryTemplate
       (list
        (aggregate(Integer.class, Foldables.Count)),
        thingsReference,
        equal(reference(thingsReference, name), constant("pumpkin"))));

    expectEqual(result.nextRow(), QueryResult.Type.Inserted);
    expectEqual(result.nextItem(), 0);
    expectEqual(result.nextRow(), QueryResult.Type.End);

    RevisionBuilder builder = Revisions.Empty.builder();

    builder.insert(Throw, things, 1, name, "tree");
    builder.insert(Throw, things, 2, name, "truck");
    builder.insert(Throw, things, 3, name, "planet");
    builder.insert(Throw, things, 4, name, "planet");
    builder.insert(Throw, things, 5, name, "tree");
    builder.insert(Throw, things, 6, name, "tree");

    Revision head = builder.commit();

    result = Revisions.Empty.diff
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

    builder = head.builder();

    builder.insert(Throw, things, 10, name, "bear");

    Revision left = builder.commit();

    builder = head.builder();

    builder.insert(Throw, things, 8, name, "planet");
    builder.insert(Throw, things, 9, name, "bear");
    builder.delete(things, 1);

    Revision right = builder.commit();

    head = head.merge(left, right, new ConflictResolver() {
        public Object resolveConflict(Table table,
                                      Column column,
                                      Object[] primaryKeyValues,
                                      Object baseValue,
                                      Object leftValue,
                                      Object rightValue)
        {
          fail("unexpected conflict in " + table + " " + column + ": base "
               + baseValue + " left " + leftValue + " right " + rightValue);
          throw new RuntimeException();
        }
      }, ForeignKeyResolvers.Delete);

    result = Revisions.Empty.diff(head, viewQuery);

    expectEqual(result.nextRow(), QueryResult.Type.Inserted);
    expectEqual(result.nextItem(), "bear");
    expectEqual(result.nextItem(), 19);
    expectEqual(result.nextRow(), QueryResult.Type.Inserted);
    expectEqual(result.nextItem(), "planet");
    expectEqual(result.nextItem(), 15);
    expectEqual(result.nextRow(), QueryResult.Type.Inserted);
    expectEqual(result.nextItem(), "tree");
    expectEqual(result.nextItem(), 18);
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

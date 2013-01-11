/* Copyright (c) 2010-2012, Revori Contributors

   Permission to use, copy, modify, and/or distribute this software
   for any purpose with or without fee is hereby granted, provided
   that the above copyright notice and this permission notice appear
   in all copies. */

package com.readytalk.revori.test;

import static com.readytalk.revori.DuplicateKeyResolution.Throw;
import static com.readytalk.revori.ExpressionFactory.aggregate;
import static com.readytalk.revori.ExpressionFactory.constant;
import static com.readytalk.revori.ExpressionFactory.equal;
import static com.readytalk.revori.ExpressionFactory.isNull;
import static com.readytalk.revori.ExpressionFactory.not;
import static com.readytalk.revori.ExpressionFactory.reference;
import static com.readytalk.revori.util.Util.cols;
import com.google.common.collect.Lists;
import static com.readytalk.revori.util.Util.set;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.util.Collections;

import org.junit.Test;

import com.readytalk.revori.Column;
import com.readytalk.revori.Comparators;
import com.readytalk.revori.ConflictResolver;
import com.readytalk.revori.Expression;
import com.readytalk.revori.Foldables;
import com.readytalk.revori.ForeignKeyResolvers;
import com.readytalk.revori.QueryResult;
import com.readytalk.revori.QueryTemplate;
import com.readytalk.revori.Revision;
import com.readytalk.revori.RevisionBuilder;
import com.readytalk.revori.Revisions;
import com.readytalk.revori.Table;
import com.readytalk.revori.TableReference;
import com.readytalk.revori.View;

public class ViewsTest {

  @Test
  public void testCount() {
    Column<Integer> number = new Column<Integer>(Integer.class, "number");
    Column<String> name = new Column<String>(String.class, "name");
    Table things = new Table(cols(number), "things");

    TableReference thingsReference = new TableReference(things);

    QueryResult result = Revisions.Empty.diff
      (Revisions.Empty, new QueryTemplate
       (Lists.newArrayList
        (aggregate(Integer.class, Foldables.Count)),
        thingsReference, constant(true)));

    assertEquals(QueryResult.Type.Inserted, result.nextRow());
    assertEquals(0, result.nextItem());
    assertEquals(QueryResult.Type.End, result.nextRow());

    result = Revisions.Empty.diff
      (Revisions.Empty.builder().table(things).row(1).update(name, "pumpkin")
       .table(things).delete(1).commit(), new QueryTemplate
       (Lists.newArrayList
        (aggregate(Integer.class, Foldables.Count)),
        thingsReference, constant(true)));

    assertEquals(QueryResult.Type.Inserted, result.nextRow());
    assertEquals(0, result.nextItem());
    assertEquals(QueryResult.Type.End, result.nextRow());

    result = Revisions.Empty.diff
      (Revisions.Empty.builder().table(things).row(1).update(name, "pumpkin")
       .table(things).delete(1).commit(), new QueryTemplate
       (Lists.newArrayList
        (aggregate(Integer.class, Foldables.Count)),
        thingsReference,
        equal(reference(thingsReference, name), constant("pumpkin"))));

    assertEquals(QueryResult.Type.Inserted, result.nextRow());
    assertEquals(0, result.nextItem());
    assertEquals(QueryResult.Type.End, result.nextRow());

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
       (Lists.newArrayList
        (aggregate(Integer.class, Foldables.Count)),
        thingsReference, constant(true)));

    assertEquals(QueryResult.Type.Inserted, result.nextRow());
    assertEquals(6, result.nextItem());
    assertEquals(QueryResult.Type.End, result.nextRow());

    QueryTemplate group = new QueryTemplate
      (Lists.newArrayList(reference(thingsReference, name),
            aggregate(Integer.class, Foldables.Count)),
       thingsReference, constant(true),
       set(reference(thingsReference, name)));
    
    result = Revisions.Empty.diff(head, group);

    assertEquals(QueryResult.Type.Inserted, result.nextRow());
    assertEquals("planet", result.nextItem());
    assertEquals(2, result.nextItem());
    assertEquals(QueryResult.Type.Inserted, result.nextRow());
    assertEquals("tree", result.nextItem());
    assertEquals(3, result.nextItem());
    assertEquals(QueryResult.Type.Inserted, result.nextRow());
    assertEquals("truck", result.nextItem());
    assertEquals(1, result.nextItem());
    assertEquals(QueryResult.Type.End, result.nextRow());

    result = Revisions.Empty.diff
      (head, new QueryTemplate
       (Lists.newArrayList(reference(thingsReference, name),
             aggregate(Integer.class, Foldables.Count)),
        thingsReference, equal
        (aggregate(Integer.class, Foldables.Count), constant(3)),
        set(reference(thingsReference, name))));

    assertEquals(QueryResult.Type.Inserted, result.nextRow());
    assertEquals("tree", result.nextItem());
    assertEquals(3, result.nextItem());
    assertEquals(QueryResult.Type.End, result.nextRow());

    Revision base = head;

    builder = base.builder();

    builder.delete(things, 6);

    head = builder.commit();

    result = Revisions.Empty.diff(head, group);

    assertEquals(QueryResult.Type.Inserted, result.nextRow());
    assertEquals("planet", result.nextItem());
    assertEquals(2, result.nextItem());
    assertEquals(QueryResult.Type.Inserted, result.nextRow());
    assertEquals("tree", result.nextItem());
    assertEquals(2, result.nextItem());
    assertEquals(QueryResult.Type.Inserted, result.nextRow());
    assertEquals("truck", result.nextItem());
    assertEquals(1, result.nextItem());
    assertEquals(QueryResult.Type.End, result.nextRow());

    result = base.diff(head, group);

    assertEquals(QueryResult.Type.Inserted, result.nextRow());
    assertEquals("tree", result.nextItem());
    assertEquals(result.nextItem(), -1);
    assertEquals(QueryResult.Type.End, result.nextRow());
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
       (Lists.newArrayList(reference(thingsReference, name),
             aggregate(Integer.class, Foldables.Sum,
                       reference(thingsReference, number))),
        thingsReference, constant(true),
        set(reference(thingsReference, name))),
       Collections.emptyList(),
       cols(name, sum),
       cols(name),
       Lists.newArrayList(reference(thingsReference, name)),
       "view");

    builder.add(view);

    Revision head = builder.commit();
    
    TableReference viewReference = new TableReference(view.table);

    QueryTemplate viewQuery = new QueryTemplate
      (Lists.newArrayList(reference(viewReference, name), reference(viewReference, sum)),
       viewReference, constant(true));

    QueryResult result = Revisions.Empty.diff(head, viewQuery);

    assertEquals(QueryResult.Type.Inserted, result.nextRow());
    assertEquals("planet", result.nextItem());
    assertEquals(7, result.nextItem());
    assertEquals(QueryResult.Type.Inserted, result.nextRow());
    assertEquals("tree", result.nextItem());
    assertEquals(12, result.nextItem());
    assertEquals(QueryResult.Type.Inserted, result.nextRow());
    assertEquals("truck", result.nextItem());
    assertEquals(2, result.nextItem());
    assertEquals(QueryResult.Type.End, result.nextRow());

    builder = head.builder();

    builder.insert(Throw, things, 7, name, "tree");
    builder.delete(things, 2);

    head = builder.commit();
    
    result = Revisions.Empty.diff(head, viewQuery);

    assertEquals(QueryResult.Type.Inserted, result.nextRow());
    assertEquals("planet", result.nextItem());
    assertEquals(7, result.nextItem());
    assertEquals(QueryResult.Type.Inserted, result.nextRow());
    assertEquals("tree", result.nextItem());
    assertEquals(19, result.nextItem());
    assertEquals(QueryResult.Type.End, result.nextRow());

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

    assertEquals(QueryResult.Type.Inserted, result.nextRow());
    assertEquals("bear", result.nextItem());
    assertEquals(19, result.nextItem());
    assertEquals(QueryResult.Type.Inserted, result.nextRow());
    assertEquals("planet", result.nextItem());
    assertEquals(15, result.nextItem());
    assertEquals(QueryResult.Type.Inserted, result.nextRow());
    assertEquals("tree", result.nextItem());
    assertEquals(18, result.nextItem());
    assertEquals(QueryResult.Type.End, result.nextRow());
  }

  @Test
  public void testViewNotNull() {
    Column<Integer> number = new Column<Integer>(Integer.class, "number");
    Column<String> name = new Column<String>(String.class, "name");
    Table things = new Table(cols(number), "things");
    
    RevisionBuilder builder = Revisions.Empty.builder();

    builder.table(things).row(1).update(name, "tree");
    builder.table(things).row(2).update(name, "truck");
    builder.table(things).row(3).update(name, "planet");
    builder.table(things).row(4).update(name, "planet");
    builder.table(things).row(5).update(name, "tree");
    builder.table(things).row(6).update(name, "tree");

    TableReference thingsReference = new TableReference(things);

    View view = new View
      (new QueryTemplate
       (Lists.newArrayList(aggregate(Integer.class, Foldables.Count)),
        thingsReference, not(isNull(reference(thingsReference, name)))));

    builder.add(view);

    Revision head = builder.commit();
    
    TableReference viewReference = new TableReference(view.table);

    QueryTemplate viewQuery = new QueryTemplate
      (Lists.newArrayList(reference(viewReference, view.columns.get(0))),
       viewReference, constant(true));

    QueryResult result = Revisions.Empty.diff(head, viewQuery);

    assertEquals(QueryResult.Type.Inserted, result.nextRow());
    assertEquals(6, result.nextItem());
    assertEquals(QueryResult.Type.End, result.nextRow());

    builder = head.builder();

    builder.table(things).row(6).delete(name);

    head = builder.commit();
    
    result = Revisions.Empty.diff(head, viewQuery);

    assertEquals(QueryResult.Type.Inserted, result.nextRow());
    assertEquals(5, result.nextItem());
    assertEquals(QueryResult.Type.End, result.nextRow());
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
       (Lists.newArrayList
        (reference(thingsReference, number), reference(thingsReference, name)),
        thingsReference, constant(true),
        Collections.<Expression>emptySet(),
        Lists.newArrayList(new QueryTemplate.OrderExpression
             (reference(thingsReference, name), Comparators.Ascending))));

    assertEquals(QueryResult.Type.Inserted, result.nextRow());
    assertEquals(3, result.nextItem());
    assertEquals("planet", result.nextItem());
    assertEquals(QueryResult.Type.Inserted, result.nextRow());
    assertEquals(4, result.nextItem());
    assertEquals("planet", result.nextItem());
    assertEquals(QueryResult.Type.Inserted, result.nextRow());
    assertEquals(1, result.nextItem());
    assertEquals("tree", result.nextItem());
    assertEquals(QueryResult.Type.Inserted, result.nextRow());
    assertEquals(5, result.nextItem());
    assertEquals("tree", result.nextItem());
    assertEquals(QueryResult.Type.Inserted, result.nextRow());
    assertEquals(6, result.nextItem());
    assertEquals("tree", result.nextItem());
    assertEquals(QueryResult.Type.Inserted, result.nextRow());
    assertEquals(2, result.nextItem());
    assertEquals("truck", result.nextItem());
    assertEquals(QueryResult.Type.End, result.nextRow());

    result = Revisions.Empty.diff
      (head, new QueryTemplate
       (Lists.newArrayList
        (reference(thingsReference, number), reference(thingsReference, name)),
        thingsReference, constant(true),
        Collections.<Expression>emptySet(),
        Lists.newArrayList(new QueryTemplate.OrderExpression
             (reference(thingsReference, name), Comparators.Descending))));

    assertEquals(QueryResult.Type.Inserted, result.nextRow());
    assertEquals(2, result.nextItem());
    assertEquals("truck", result.nextItem());
    assertEquals(QueryResult.Type.Inserted, result.nextRow());
    assertEquals(1, result.nextItem());
    assertEquals("tree", result.nextItem());
    assertEquals(QueryResult.Type.Inserted, result.nextRow());
    assertEquals(5, result.nextItem());
    assertEquals("tree", result.nextItem());
    assertEquals(QueryResult.Type.Inserted, result.nextRow());
    assertEquals(6, result.nextItem());
    assertEquals("tree", result.nextItem());
    assertEquals(QueryResult.Type.Inserted, result.nextRow());
    assertEquals(3, result.nextItem());
    assertEquals("planet", result.nextItem());
    assertEquals(QueryResult.Type.Inserted, result.nextRow());
    assertEquals(4, result.nextItem());
    assertEquals("planet", result.nextItem());
    assertEquals(QueryResult.Type.End, result.nextRow());
  }
}

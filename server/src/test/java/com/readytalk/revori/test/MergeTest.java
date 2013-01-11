/* Copyright (c) 2010-2012, Revori Contributors

   Permission to use, copy, modify, and/or distribute this software
   for any purpose with or without fee is hereby granted, provided
   that the above copyright notice and this permission notice appear
   in all copies. */

package com.readytalk.revori.test;

import static com.readytalk.revori.util.Util.cols;
import com.google.common.collect.Lists;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import org.junit.Test;

import com.readytalk.revori.BinaryOperation;
import com.readytalk.revori.Column;
import com.readytalk.revori.ColumnReference;
import com.readytalk.revori.ConflictResolver;
import com.readytalk.revori.ConflictResolvers;
import com.readytalk.revori.Constant;
import com.readytalk.revori.DeleteTemplate;
import com.readytalk.revori.DuplicateKeyResolution;
import com.readytalk.revori.Expression;
import com.readytalk.revori.ForeignKeyResolvers;
import com.readytalk.revori.Index;
import com.readytalk.revori.InsertTemplate;
import com.readytalk.revori.Parameter;
import com.readytalk.revori.PatchTemplate;
import com.readytalk.revori.QueryResult;
import com.readytalk.revori.QueryTemplate;
import com.readytalk.revori.Revision;
import com.readytalk.revori.RevisionBuilder;
import com.readytalk.revori.Revisions;
import com.readytalk.revori.Table;
import com.readytalk.revori.TableReference;
import com.readytalk.revori.UpdateTemplate;

public class MergeTest {
    
    @Test
    public void testMerges(){

        final Column<Integer> number = new Column<Integer>(Integer.class);
        final Column<String> name = new Column<String>(String.class);
        final Table numbers = new Table(cols(number));

        Revision tail = Revisions.Empty;

        PatchTemplate insert = new InsertTemplate
          (numbers,
           cols(number, name),
           Lists.newArrayList((Expression) new Parameter(), new Parameter()),
           DuplicateKeyResolution.Throw);

        RevisionBuilder builder = tail.builder();

        builder.apply(insert,  1, "one");
        builder.apply(insert,  2, "two");
        builder.apply(insert,  6, "six");
        builder.apply(insert,  7, "seven");
        builder.apply(insert,  8, "eight");
        builder.apply(insert,  9, "nine");
        builder.apply(insert, 13, "thirteen");

        Revision base = builder.commit();

        builder = base.builder();

        builder.apply(insert, 4, "four");

        Revision left = builder.commit();

        TableReference numbersReference = new TableReference(numbers);

        PatchTemplate update = new UpdateTemplate
          (numbersReference,
           new BinaryOperation
           (BinaryOperation.Type.Equal,
            new ColumnReference(numbersReference, number),
            new Parameter()),
           cols(name),
           Lists.newArrayList((Expression) new Parameter()));

        builder = base.builder();

        builder.apply(update,  6, "roku");
        builder.apply(insert, 42, "forty two");

        Revision right = builder.commit();

        Revision merge = base.merge(left, right, null, null);

        QueryTemplate any = new QueryTemplate
          (Lists.newArrayList((Expression) new ColumnReference(numbersReference, name)),
           numbersReference,
           new Constant(true));

        QueryResult result = tail.diff(merge, any);

        assertEquals(QueryResult.Type.Inserted, result.nextRow());
        assertEquals("one", result.nextItem());
        assertEquals(QueryResult.Type.Inserted, result.nextRow());
        assertEquals("two", result.nextItem());
        assertEquals(QueryResult.Type.Inserted, result.nextRow());
        assertEquals("four", result.nextItem());
        assertEquals(QueryResult.Type.Inserted, result.nextRow());
        assertEquals("roku", result.nextItem());
        assertEquals(QueryResult.Type.Inserted, result.nextRow());
        assertEquals("seven", result.nextItem());
        assertEquals(QueryResult.Type.Inserted, result.nextRow());
        assertEquals("eight", result.nextItem());
        assertEquals(QueryResult.Type.Inserted, result.nextRow());
        assertEquals("nine", result.nextItem());
        assertEquals(QueryResult.Type.Inserted, result.nextRow());
        assertEquals("thirteen", result.nextItem());
        assertEquals(QueryResult.Type.Inserted, result.nextRow());
        assertEquals("forty two", result.nextItem());
        assertEquals(QueryResult.Type.End, result.nextRow());
        
        builder = base.builder();

        builder.apply(insert, 4, "four");

        left = builder.commit();

        builder = base.builder();

        builder.apply(insert, 4, "four");

        right = builder.commit();

        merge = base.merge(left, right, null, null);

        result = base.diff(merge, any);

        assertEquals(QueryResult.Type.Inserted, result.nextRow());
        assertEquals("four", result.nextItem());
        assertEquals(QueryResult.Type.End, result.nextRow());
        
        PatchTemplate delete = new DeleteTemplate
          (numbersReference,
           new BinaryOperation
           (BinaryOperation.Type.Equal,
            new ColumnReference(numbersReference, number),
            new Parameter()));

        builder = base.builder();

        builder.apply(delete, 8);

        left = builder.commit();

        builder = base.builder();

        builder.apply(update, 8, "hachi");

        right = builder.commit();

        merge = base.merge(left, right, null, null);

        result = base.diff(merge, any);

        assertEquals(QueryResult.Type.Deleted, result.nextRow());
        assertEquals("eight", result.nextItem());
        assertEquals(QueryResult.Type.End, result.nextRow());

        builder = base.builder();

        builder.apply(insert, 4, "four");

        left = builder.commit();

        builder = base.builder();

        builder.apply(insert, 4, "shi");

        right = builder.commit();

        merge = base.merge(left, right, new ConflictResolver() {
            public Object resolveConflict(Table table,
                                          Column column,
                                          Object[] primaryKeyValues,
                                          Object baseValue,
                                          Object leftValue,
                                          Object rightValue)
            {
              assertEquals(numbers, table);
              assertEquals(name, column);
              assertEquals(1, primaryKeyValues.length);
              assertEquals(4, primaryKeyValues[0]);
              assertNull(baseValue);
              assertEquals("four", leftValue);
              assertEquals("shi", rightValue);
              
              return "cuatro";
            }
          }, null);

        result = base.diff(merge, any);

        assertEquals(QueryResult.Type.Inserted, result.nextRow());
        assertEquals("cuatro", result.nextItem());
        assertEquals(QueryResult.Type.End, result.nextRow());

        builder = base.builder();

        builder.apply(update, 1, "ichi");

        left = builder.commit();

        builder = base.builder();

        builder.apply(update, 1, "uno");

        right = builder.commit();

        merge = base.merge(left, right, new ConflictResolver() {
            public Object resolveConflict(Table table,
                                          Column column,
                                          Object[] primaryKeyValues,
                                          Object baseValue,
                                          Object leftValue,
                                          Object rightValue)
            {
              assertEquals(numbers, table);
              assertEquals(name, column);
              assertEquals(1, primaryKeyValues.length);
              assertEquals(1, primaryKeyValues[0]);
              assertEquals("one", baseValue);
              assertEquals("ichi", leftValue);
              assertEquals("uno", rightValue);
              
              return "unit";
            }
          }, null);

        result = base.diff(merge, any);

        assertEquals(QueryResult.Type.Deleted, result.nextRow());
        assertEquals("one", result.nextItem());
        assertEquals(QueryResult.Type.Inserted, result.nextRow());
        assertEquals("unit", result.nextItem());
        assertEquals(QueryResult.Type.End, result.nextRow());

        builder = tail.builder();

        builder.apply(insert, 1, "one");

        Revision t1 = builder.commit();

        builder = tail.builder();

        builder.apply(insert, 1, "uno");

        Revision t2 = builder.commit();

        merge = tail.merge(t1, t2, new ConflictResolver() {
            public Object resolveConflict(Table table,
                                          Column column,
                                          Object[] primaryKeyValues,
                                          Object baseValue,
                                          Object leftValue,
                                          Object rightValue)
            {
              assertEquals(numbers, table);
              assertEquals(name, column);
              assertEquals(1, primaryKeyValues.length);
              assertEquals(1, primaryKeyValues[0]);
              assertNull(baseValue);
              assertEquals("one", leftValue);
              assertEquals("uno", rightValue);
              
              return "unit";
            }
          }, null);

        result = tail.diff(merge, any);

        assertEquals(QueryResult.Type.Inserted, result.nextRow());
        assertEquals("unit", result.nextItem());
        assertEquals(QueryResult.Type.End, result.nextRow());

        builder = tail.builder();

        builder.apply(insert, 1, "one");
        builder.apply(insert, 2, "two");

        t1 = builder.commit();

        builder = tail.builder();

        builder.apply(insert, 1, "uno");
        builder.apply(insert, 3, "tres");

        t2 = builder.commit();

        merge = tail.merge(t1, t2, new ConflictResolver() {
            public Object resolveConflict(Table table,
                                          Column column,
                                          Object[] primaryKeyValues,
                                          Object baseValue,
                                          Object leftValue,
                                          Object rightValue)
            {
              assertEquals(numbers, table);
              assertEquals(name, column);
              assertEquals(1, primaryKeyValues.length);
              assertEquals(1, primaryKeyValues[0]);
              assertNull(baseValue);
              assertEquals("one", leftValue);
              assertEquals("uno", rightValue);
              
              return "unit";
            }
          }, null);

        result = tail.diff(merge, any);

        assertEquals(QueryResult.Type.Inserted, result.nextRow());
        assertEquals("unit", result.nextItem());
        assertEquals(QueryResult.Type.Inserted, result.nextRow());
        assertEquals("two", result.nextItem());
        assertEquals(QueryResult.Type.Inserted, result.nextRow());
        assertEquals("tres", result.nextItem());
        assertEquals(QueryResult.Type.End, result.nextRow());
    }

  @Test
  public void testDeleteAndInsert() {
    Column<Integer> first = new Column<Integer>(Integer.class, "first");
    Column<Integer> second = new Column<Integer>(Integer.class, "second");
    Column<Integer> third = new Column<Integer>(Integer.class, "third");
    Column<String> name = new Column<String>(String.class, "name");
    Column<String> value = new Column<String>(String.class, "value");
    Table table = new Table(cols(first, second, third), "table");
    Index key = table.primaryKey;

    Revision base = Revisions.Empty.builder().table(table).row(1, 2, 1)
      .update(name, "foo").commit();

    Revision fork1 = base.builder().table(table).delete(1, 2, 1).commit();

    Revision fork2 = base.builder().table(table).row(1, 2, 2).update
      (name, "bar").commit();

    Revision merged = base.merge
      (fork1, fork2, ConflictResolvers.Restrict, ForeignKeyResolvers.Restrict);

    assertNull(merged.query(name, key, 1, 2, 1));
    assertEquals("bar", merged.query(name, key, 1, 2, 2));

    merged = base.merge
      (fork2, fork1, ConflictResolvers.Restrict, ForeignKeyResolvers.Restrict);

    assertNull(merged.query(name, key, 1, 2, 1));
    assertEquals("bar", merged.query(name, key, 1, 2, 2));

    base = Revisions.Empty.builder().table(table).row(1, 2, 1)
      .update(name, "foo").commit();

    fork1 = base.builder().table(table).row(1, 2, 1)
      .delete(name).commit();

    fork2 = base.builder().table(table).row(1, 2, 1).update
      (value, "bar").commit();

    merged = base.merge
      (fork1, fork2, ConflictResolvers.Restrict, ForeignKeyResolvers.Restrict);

    assertNull(merged.query(name, key, 1, 2, 1));
    assertEquals("bar", merged.query(value, key, 1, 2, 1));

    merged = base.merge
      (fork2, fork1, ConflictResolvers.Restrict, ForeignKeyResolvers.Restrict);

    assertNull(merged.query(name, key, 1, 2, 1));
    assertEquals("bar", merged.query(value, key, 1, 2, 1));
  }

  @Test
  public void testDeleteInBothForks() {
    Column<Integer> first = new Column<Integer>(Integer.class, "first");
    Column<String> name = new Column<String>(String.class, "name");
    Table table = new Table(cols(first), "table");
    Index key = table.primaryKey;

    Revision base = Revisions.Empty.builder().table(table).row(1)
      .update(name, "foo").commit();

    Revision fork1 = base.builder().table(table).delete(1).commit();

    Revision fork2 = fork1.builder().table(table).row(2).update
      (name, "bar").commit();

    Revision merged = base.merge
      (fork1, fork2, ConflictResolvers.Restrict, ForeignKeyResolvers.Restrict);

    assertNull(merged.query(name, key, 1));
    assertEquals("bar", merged.query(name, key, 2));

    merged = base.merge
      (fork2, fork1, ConflictResolvers.Restrict, ForeignKeyResolvers.Restrict);

    assertNull(merged.query(name, key, 1));
    assertEquals("bar", merged.query(name, key, 2));
  }
}

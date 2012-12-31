/* Copyright (c) 2010-2012, Revori Contributors

   Permission to use, copy, modify, and/or distribute this software
   for any purpose with or without fee is hereby granted, provided
   that the above copyright notice and this permission notice appear
   in all copies. */

package com.readytalk.revori.test;

import static com.readytalk.revori.ExpressionFactory.isNull;
import static com.readytalk.revori.ExpressionFactory.not;
import static com.readytalk.revori.ExpressionFactory.reference;
import static com.readytalk.revori.util.Util.cols;
import static com.readytalk.revori.util.Util.list;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

import com.readytalk.revori.Column;
import com.readytalk.revori.Constant;
import com.readytalk.revori.DuplicateKeyResolution;
import com.readytalk.revori.Expression;
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

public class DiffsTest {

  @Test
  public void testUpdateNonQueriedColumns() {
    Column<Integer> number = new Column<Integer>(Integer.class);
    Column<String> name = new Column<String>(String.class);
    Column<String> origin = new Column<String>(String.class);
    Table numbers = new Table(cols(number));
  
    Revision tail = Revisions.Empty;
  
    PatchTemplate insert = new InsertTemplate
      (numbers,
       cols(number, name, origin),
       list((Expression) new Parameter(), new Parameter(), new Parameter()),
       DuplicateKeyResolution.Throw);
  
    RevisionBuilder builder = tail.builder();
  
    builder.apply(insert, 0, "zero", "far too recently");
    builder.apply(insert, 1, "one", "long ago");
    builder.apply(insert, 2, "two", "long ago");
    builder.apply(insert, 3, "three", "a little less long ago");

    Revision first = builder.commit();
    
    TableReference numbersReference = new TableReference(numbers);
    
    QueryTemplate query = new QueryTemplate
    (list((Expression) reference(numbersReference, name)),
     numbersReference,
     new Constant(true));

    QueryResult result = tail.diff(first, query);

    assertEquals(QueryResult.Type.Inserted, result.nextRow());
    assertEquals("zero", result.nextItem());
    assertEquals(QueryResult.Type.Inserted, result.nextRow());
    assertEquals("one", result.nextItem());
    assertEquals(QueryResult.Type.Inserted, result.nextRow());
    assertEquals("two", result.nextItem());
    assertEquals(QueryResult.Type.Inserted, result.nextRow());
    assertEquals("three", result.nextItem());
    assertEquals(QueryResult.Type.End, result.nextRow());

    
    builder = first.builder();
  
    // rewrite history
    builder.table(numbers)
      .row(0)
      .update(origin, "not long after <one>");

    Revision second = builder.commit();

    result = first.diff(second, query);

    // we didn't change any of the queried columns, so we shouldn't see any difference.
    assertEquals(QueryResult.Type.End, result.nextRow());
    
  }

  @Test
  public void testRemoveQueriedColumns() {
    Column<Integer> number = new Column<Integer>(Integer.class);
    Column<String> name = new Column<String>(String.class);
    Column<String> origin = new Column<String>(String.class);
    Table numbers = new Table(cols(number));
  
    Revision tail = Revisions.Empty;
  
    PatchTemplate insert = new InsertTemplate
      (numbers,
       cols(number, name, origin),
       list((Expression) new Parameter(), new Parameter(), new Parameter()),
       DuplicateKeyResolution.Throw);
  
    RevisionBuilder builder = tail.builder();
  
    builder.apply(insert, 0, "zero", "far too recently");
    builder.apply(insert, 1, "one", "long ago");
    builder.apply(insert, 2, "two", "long ago");
    builder.apply(insert, 3, "three", "a little less long ago");

    Revision first = builder.commit();
    
    TableReference numbersReference = new TableReference(numbers);
    
    QueryTemplate query = new QueryTemplate
    (list((Expression)
      reference(numbersReference, number),
      reference(numbersReference, name),
      reference(numbersReference, origin)),
     numbersReference,
     not(isNull(reference(numbersReference, origin))));
    
    builder = first.builder();
  
    builder.table(numbers)
      .row(0)
      .delete(origin);

    Revision second = builder.commit();

    QueryResult result = first.diff(second, query);

    assertEquals(QueryResult.Type.Deleted, result.nextRow());
    assertEquals(false, result.rowUpdated());
    assertEquals(0, result.nextItem());
    assertEquals("zero", result.nextItem());
    assertEquals("far too recently", result.nextItem());
    assertEquals(QueryResult.Type.End, result.nextRow());
    
  }

  @Test
  public void testUpdate() {
    Column<Integer> number = new Column<Integer>(Integer.class);
    Column<String> name = new Column<String>(String.class);
    Table numbers = new Table(cols(number));
  
    Revision tail = Revisions.Empty;
  
    RevisionBuilder builder = tail.builder();
  
    builder.insert(DuplicateKeyResolution.Throw, numbers, 0, name, "zero");

    Revision first = builder.commit();
    
    builder = tail.builder();
  
    builder.insert(DuplicateKeyResolution.Overwrite, numbers, 0, name, "none");

    Revision second = builder.commit();
    
    TableReference numbersReference = new TableReference(numbers);

    QueryTemplate query = new QueryTemplate
      (list((Expression) reference(numbersReference, name)),
       numbersReference,
       new Constant(true));

    QueryResult result = first.diff(second, query);

    assertEquals(QueryResult.Type.Deleted, result.nextRow());
    assertTrue(result.rowUpdated());
    assertEquals("zero", result.nextItem());
    assertEquals(QueryResult.Type.Inserted, result.nextRow());
    assertEquals("none", result.nextItem());
    assertEquals(QueryResult.Type.End, result.nextRow());
  }
}

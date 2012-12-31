/* Copyright (c) 2010-2012, Revori Contributors

   Permission to use, copy, modify, and/or distribute this software
   for any purpose with or without fee is hereby granted, provided
   that the above copyright notice and this permission notice appear
   in all copies. */

package com.readytalk.revori.test;

import static com.readytalk.revori.ExpressionFactory.and;
import static com.readytalk.revori.ExpressionFactory.constant;
import static com.readytalk.revori.ExpressionFactory.equal;
import static com.readytalk.revori.ExpressionFactory.not;
import static com.readytalk.revori.ExpressionFactory.reference;
import static com.readytalk.revori.SourceFactory.reference;
import static com.readytalk.revori.util.Util.cols;
import static com.readytalk.revori.util.Util.list;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.Iterator;

import org.junit.Test;

import com.readytalk.revori.BinaryOperation;
import com.readytalk.revori.Column;
import com.readytalk.revori.Constant;
import com.readytalk.revori.DeleteTemplate;
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
import com.readytalk.revori.UpdateTemplate;

public class GeneralTest {
	@Test
    public void testSimpleInsertDiffs(){
        Column<Integer> number = new Column<Integer>(Integer.class);
        Column<String> name = new Column<String>(String.class);
        Table numbers = new Table(cols(number));
        
        Revision tail = Revisions.Empty;
        
        RevisionBuilder builder = tail.builder();
        
        PatchTemplate insert = new InsertTemplate
        (numbers,
         cols(number, name),
         list((Expression) new Parameter(), new Parameter()),
         DuplicateKeyResolution.Throw);
        
        builder.apply(insert, 42, "forty two");
        
        Revision first = builder.commit();
        
        TableReference numbersReference = new TableReference(numbers);
        QueryTemplate equal = new QueryTemplate
        (list(reference(numbersReference, name)),
                numbersReference,
                new BinaryOperation
                (BinaryOperation.Type.Equal,
                    reference(numbersReference, number),
                        new Parameter()));
        
        QueryResult result = tail.diff(first, equal, 42);
        
        assertEquals(result.nextRow(), QueryResult.Type.Inserted);
        assertEquals(result.nextItem(), "forty two");
        assertEquals(result.nextRow(), QueryResult.Type.End);
        
        Object[] parameters = { 42 };
        result = first.diff(tail, equal, parameters);
        
        assertEquals(result.nextRow(), QueryResult.Type.Deleted);
        assertEquals(result.nextItem(), "forty two");
        assertEquals(result.nextRow(), QueryResult.Type.End);
        
        result = tail.diff(first, equal, 43);
        
        assertEquals(result.nextRow(), QueryResult.Type.End);
        Object[] parameters1 = { 42 };
        
        result = tail.diff(tail, equal, parameters1);
        
        assertEquals(result.nextRow(), QueryResult.Type.End);
        Object[] parameters2 = { 42 };
        
        result = first.diff(first, equal, parameters2);
        
        assertEquals(result.nextRow(), QueryResult.Type.End);
        }
  @Test
    public void testLargerInsertDiffs(){
        Column<Integer> number = new Column<Integer>(Integer.class);
        Column<String> name = new Column<String>(String.class);
        Table numbers = new Table(cols(number));
        
        Revision tail = Revisions.Empty;
        
        PatchTemplate insert = new InsertTemplate
        (numbers,
         cols(number, name),
         list((Expression) new Parameter(), new Parameter()),
         DuplicateKeyResolution.Throw);
        
        RevisionBuilder builder = tail.builder();
        
        builder.apply(insert, 42, "forty two");
        builder.apply(insert, 43, "forty three");
        builder.apply(insert, 44, "forty four");
        builder.apply(insert,  2, "two");
        builder.apply(insert, 65, "sixty five");
        builder.apply(insert,  8, "eight");
        
        Revision first = builder.commit();
        
        TableReference numbersReference = new TableReference(numbers);

        QueryTemplate equal = new QueryTemplate
        (list(reference(numbersReference, name)),
                numbersReference,
                new BinaryOperation
                (BinaryOperation.Type.Equal,
                        reference(numbersReference, number),
                        new Parameter()));
        
        QueryResult result = tail.diff(first, equal, 42);
        
        assertEquals(result.nextRow(), QueryResult.Type.Inserted);
        assertEquals(result.nextItem(), "forty two");
        assertEquals(result.nextRow(), QueryResult.Type.End);
        result = first.diff(tail, equal, 42);
        
        assertEquals(result.nextRow(), QueryResult.Type.Deleted);
        assertEquals(result.nextItem(), "forty two");
        assertEquals(result.nextRow(), QueryResult.Type.End);
        result = tail.diff(first, equal, 43);
        
        assertEquals(result.nextRow(), QueryResult.Type.Inserted);
        assertEquals(result.nextItem(), "forty three");
        assertEquals(result.nextRow(), QueryResult.Type.End);
        Object[] parameters = { 42 };
        result = tail.diff(tail, equal, parameters);
        
        assertEquals(result.nextRow(), QueryResult.Type.End);
        Object[] parameters1 = { 42 };
        
        result = first.diff(first, equal, parameters1);
        
        assertEquals(result.nextRow(), QueryResult.Type.End);
        
        builder = first.builder();
        
        builder.apply(insert, 1, "one");
        builder.apply(insert, 3, "three");
        builder.apply(insert, 5, "five");
        builder.apply(insert, 6, "six");
        
        Revision second = builder.commit();
        Object[] parameters2 = { 43 };
        
        result = tail.diff(second, equal, parameters2);
        
        assertEquals(result.nextRow(), QueryResult.Type.Inserted);
        assertEquals(result.nextItem(), "forty three");
        assertEquals(result.nextRow(), QueryResult.Type.End);
        Object[] parameters3 = { 43 };
        
        result = first.diff(second, equal, parameters3);
        
        assertEquals(result.nextRow(), QueryResult.Type.End);
        Object[] parameters4 = { 5 };
        
        result = first.diff(second, equal, parameters4);
        
        assertEquals(result.nextRow(), QueryResult.Type.Inserted);
        assertEquals(result.nextItem(), "five");
        assertEquals(result.nextRow(), QueryResult.Type.End);
        Object[] parameters5 = { 5 };
        
        result = tail.diff(first, equal, parameters5);
        
        assertEquals(result.nextRow(), QueryResult.Type.End);
        Object[] parameters6 = { 5 };
        
        result = second.diff(first, equal, parameters6);
        
        assertEquals(result.nextRow(), QueryResult.Type.Deleted);
        assertEquals(result.nextItem(), "five");
        assertEquals(result.nextRow(), QueryResult.Type.End);
        }

    @Test
    public void testDeleteDiffs(){
    
    Column<Integer> number = new Column<Integer>(Integer.class);
    Column<String> name = new Column<String>(String.class);
    Table numbers = new Table(cols(number));

    Revision tail = Revisions.Empty;

    PatchTemplate insert = new InsertTemplate
      (numbers,
       cols(number, name),
       list((Expression) new Parameter(), new Parameter()),
       DuplicateKeyResolution.Throw);

    RevisionBuilder builder = tail.builder();

    builder.apply(insert, 42, "forty two");
    builder.apply(insert, 43, "forty three");
    builder.apply(insert, 44, "forty four");
    builder.apply(insert,  2, "two");
    builder.apply(insert, 65, "sixty five");
    builder.apply(insert,  8, "eight");

    Revision first = builder.commit();

    TableReference numbersReference = new TableReference(numbers);
    QueryTemplate equal = new QueryTemplate
      (list(reference(numbersReference, name)),
       numbersReference,
       new BinaryOperation
       (BinaryOperation.Type.Equal,
        reference(numbersReference, number),
        new Parameter()));

    PatchTemplate delete = new DeleteTemplate
      (numbersReference,
       new BinaryOperation
       (BinaryOperation.Type.Equal,
        reference(numbersReference, number),
        new Parameter()));

    builder = first.builder();

    builder.apply(delete, 43);

    Revision second = builder.commit();

    Object[] parameters = { 43 };
    QueryResult result = first.diff(second, equal, parameters);

    assertEquals(result.nextRow(), QueryResult.Type.Deleted);
    assertEquals(result.nextItem(), "forty three");
    assertEquals(result.nextRow(), QueryResult.Type.End);
    Object[] parameters1 = { 43 };

    result = second.diff(first, equal, parameters1);

    assertEquals(result.nextRow(), QueryResult.Type.Inserted);
    assertEquals(result.nextItem(), "forty three");
    assertEquals(result.nextRow(), QueryResult.Type.End);
    Object[] parameters2 = { 43 };

    result = tail.diff(second, equal, parameters2);

    assertEquals(result.nextRow(), QueryResult.Type.End);
    Object[] parameters3 = { 42 };
    
    result = first.diff(second, equal, parameters3);

    assertEquals(result.nextRow(), QueryResult.Type.End);
    Object[] parameters4 = { 42 };
    
    result = tail.diff(second, equal, parameters4);

    assertEquals(result.nextRow(), QueryResult.Type.Inserted);
    assertEquals(result.nextItem(), "forty two");
    assertEquals(result.nextRow(), QueryResult.Type.End);
    Object[] parameters5 = { 42 };
    
    result = second.diff(tail, equal, parameters5);

    assertEquals(result.nextRow(), QueryResult.Type.Deleted);
    assertEquals(result.nextItem(), "forty two");
    assertEquals(result.nextRow(), QueryResult.Type.End);

    builder = first.builder();

    builder.apply(delete, 43);
    builder.apply(delete, 42);
    builder.apply(delete, 65);

    second = builder.commit();
    Object[] parameters6 = { 43 };

    result = first.diff(second, equal, parameters6);

    assertEquals(result.nextRow(), QueryResult.Type.Deleted);
    assertEquals(result.nextItem(), "forty three");
    assertEquals(result.nextRow(), QueryResult.Type.End);
    Object[] parameters7 = { 43 };

    result = second.diff(first, equal, parameters7);

    assertEquals(result.nextRow(), QueryResult.Type.Inserted);
    assertEquals(result.nextItem(), "forty three");
    assertEquals(result.nextRow(), QueryResult.Type.End);
    Object[] parameters8 = { 42 };

    result = first.diff(second, equal, parameters8);

    assertEquals(result.nextRow(), QueryResult.Type.Deleted);
    assertEquals(result.nextItem(), "forty two");
    assertEquals(result.nextRow(), QueryResult.Type.End);
    Object[] parameters9 = { 42 };

    result = second.diff(first, equal, parameters9);

    assertEquals(result.nextRow(), QueryResult.Type.Inserted);
    assertEquals(result.nextItem(), "forty two");
    assertEquals(result.nextRow(), QueryResult.Type.End);
    Object[] parameters10 = { 65 };

    result = first.diff(second, equal, parameters10);

    assertEquals(result.nextRow(), QueryResult.Type.Deleted);
    assertEquals(result.nextItem(), "sixty five");
    assertEquals(result.nextRow(), QueryResult.Type.End);
    Object[] parameters11 = { 65 };

    result = second.diff(first, equal, parameters11);

    assertEquals(result.nextRow(), QueryResult.Type.Inserted);
    assertEquals(result.nextItem(), "sixty five");
    assertEquals(result.nextRow(), QueryResult.Type.End);
    Object[] parameters12 = { 2 };
    
    result = first.diff(second, equal, parameters12);

    assertEquals(result.nextRow(), QueryResult.Type.End);
    Object[] parameters13 = { 2 };
    
    result = second.diff(first, equal, parameters13);

    assertEquals(result.nextRow(), QueryResult.Type.End);

    builder = second.builder();

    builder.apply(delete, 44);
    builder.apply(delete,  2);
    builder.apply(delete,  8);

    Revision third = builder.commit();
    Object[] parameters14 = { 44 };

    result = second.diff(third, equal, parameters14);

    assertEquals(result.nextRow(), QueryResult.Type.Deleted);
    assertEquals(result.nextItem(), "forty four");
    assertEquals(result.nextRow(), QueryResult.Type.End);
    Object[] parameters15 = { 44 };

    result = third.diff(second, equal, parameters15);

    assertEquals(result.nextRow(), QueryResult.Type.Inserted);
    assertEquals(result.nextItem(), "forty four");
    assertEquals(result.nextRow(), QueryResult.Type.End);
    Object[] parameters16 = { 44 };

    result = tail.diff(third, equal, parameters16);

    assertEquals(result.nextRow(), QueryResult.Type.End);
    Object[] parameters17 = { 42 };

    result = tail.diff(third, equal, parameters17);

    assertEquals(result.nextRow(), QueryResult.Type.End);
    	
    }
    
    @Test
    public void testUpdateDiffs(){
        Column<Integer> number = new Column<Integer>(Integer.class);
        Column<String> name = new Column<String>(String.class);
        Table numbers = new Table(cols(number));

        Revision tail = Revisions.Empty;

        PatchTemplate insert = new InsertTemplate
        (numbers,
         cols(number, name),
         list((Expression) new Parameter(), new Parameter()),
         DuplicateKeyResolution.Throw);
        
        RevisionBuilder builder = tail.builder();

        builder.apply(insert,  1, "one");
        builder.apply(insert,  2, "two");
        builder.apply(insert,  3, "three");
        builder.apply(insert,  4, "four");
        builder.apply(insert,  5, "five");
        builder.apply(insert,  6, "six");
        builder.apply(insert,  7, "seven");
        builder.apply(insert,  8, "eight");
        builder.apply(insert,  9, "nine");
        builder.apply(insert, 10, "ten");
        builder.apply(insert, 11, "eleven");
        builder.apply(insert, 12, "twelve");
        builder.apply(insert, 13, "thirteen");

        Revision first = builder.commit();

        TableReference numbersReference = new TableReference(numbers);

        PatchTemplate update = new UpdateTemplate
        (numbersReference,
         new BinaryOperation
         (BinaryOperation.Type.Equal,
          reference(numbersReference, number),
          new Parameter()),
         cols(name),
         list((Expression) new Parameter()));

        builder = first.builder();

        builder.apply(update, 1, "ichi");

        Revision second = builder.commit();

        QueryTemplate equal = new QueryTemplate
        (list(reference(numbersReference, name)),
                numbersReference,
                new BinaryOperation
                (BinaryOperation.Type.Equal,
                        reference(numbersReference, number),
                        new Parameter()));

        Object[] parameters = { 1 };
        QueryResult result = first.diff(second, equal, parameters);

        assertEquals(result.nextRow(), QueryResult.Type.Deleted);
        assertEquals(result.nextItem(), "one");
        assertEquals(result.nextRow(), QueryResult.Type.Inserted);
        assertEquals(result.nextItem(), "ichi");
        assertEquals(result.nextRow(), QueryResult.Type.End);
        Object[] parameters1 = { 1 };

        result = second.diff(first, equal, parameters1);

        assertEquals(result.nextRow(), QueryResult.Type.Deleted);
        assertEquals(result.nextItem(), "ichi");
        assertEquals(result.nextRow(), QueryResult.Type.Inserted);
        assertEquals(result.nextItem(), "one");
        assertEquals(result.nextRow(), QueryResult.Type.End);
        Object[] parameters2 = { 2 };

        result = first.diff(second, equal, parameters2);

        assertEquals(result.nextRow(), QueryResult.Type.End);
        Object[] parameters3 = { 1 };

        result = tail.diff(first, equal, parameters3);

        assertEquals(result.nextRow(), QueryResult.Type.Inserted);
        assertEquals(result.nextItem(), "one");
        assertEquals(result.nextRow(), QueryResult.Type.End);
        Object[] parameters4 = { 1 };

        result = tail.diff(second, equal, parameters4);

        assertEquals(result.nextRow(), QueryResult.Type.Inserted);
        assertEquals(result.nextItem(), "ichi");
        assertEquals(result.nextRow(), QueryResult.Type.End);

        builder = second.builder();

        builder.apply(update, 11, "ju ichi");
        builder.apply(update,  6, "roku");
        builder.apply(update,  7, "shichi");

        Revision third = builder.commit();

        QueryTemplate any = new QueryTemplate
        (list(reference(numbersReference, name)),
                numbersReference,
                new Constant(true));
        Object[] parameters5 = {};

        result = second.diff(third, any, parameters5);

        assertEquals(result.nextRow(), QueryResult.Type.Deleted);
        assertEquals(result.nextItem(), "six");
        assertEquals(result.nextRow(), QueryResult.Type.Inserted);
        assertEquals(result.nextItem(), "roku");
        assertEquals(result.nextRow(), QueryResult.Type.Deleted);
        assertEquals(result.nextItem(), "seven");
        assertEquals(result.nextRow(), QueryResult.Type.Inserted);
        assertEquals(result.nextItem(), "shichi");
        assertEquals(result.nextRow(), QueryResult.Type.Deleted);
        assertEquals(result.nextItem(), "eleven");
        assertEquals(result.nextRow(), QueryResult.Type.Inserted);
        assertEquals(result.nextItem(), "ju ichi");
        assertEquals(result.nextRow(), QueryResult.Type.End);
    }
    
    @Test
    public void testNonIndexedQueries(){
        Column<Integer> number = new Column<Integer>(Integer.class);
        Column<String> name = new Column<String>(String.class);
        Table numbers = new Table(cols(number));

        Revision tail = Revisions.Empty;

        PatchTemplate insert = new InsertTemplate
          (numbers,
           cols(number, name),
           list((Expression) new Parameter(), new Parameter()),
           DuplicateKeyResolution.Throw);

        RevisionBuilder builder = tail.builder();

        builder.apply(insert,  1, "one");
        builder.apply(insert,  2, "two");
        builder.apply(insert,  3, "three");
        builder.apply(insert,  4, "four");
        builder.apply(insert,  5, "five");
        builder.apply(insert,  6, "six");
        builder.apply(insert,  7, "seven");
        builder.apply(insert,  8, "eight");
        builder.apply(insert,  9, "nine");
        builder.apply(insert, 10, "ten");
        builder.apply(insert, 11, "eleven");
        builder.apply(insert, 12, "twelve");
        builder.apply(insert, 13, "thirteen");

        Revision first = builder.commit();

        TableReference numbersReference = new TableReference(numbers);

        QueryTemplate nameEqual = new QueryTemplate
          (list(reference(numbersReference, number),
                reference(numbersReference, name)),
           numbersReference,
           new BinaryOperation
           (BinaryOperation.Type.Equal,
            reference(numbersReference, name),
            new Parameter()));

        Object[] parameters = { "nine" };
        QueryResult result = tail.diff(first, nameEqual, parameters);

        assertEquals(result.nextRow(), QueryResult.Type.Inserted);
        assertEquals(result.nextItem(), 9);
        assertEquals(result.nextItem(), "nine");
        assertEquals(result.nextRow(), QueryResult.Type.End);
        Object[] parameters1 = { "nine" };

        result = first.diff(tail, nameEqual, parameters1);

        assertEquals(result.nextRow(), QueryResult.Type.Deleted);
        assertEquals(result.nextItem(), 9);
        assertEquals(result.nextItem(), "nine");
        assertEquals(result.nextRow(), QueryResult.Type.End);

        QueryTemplate nameLessThan = new QueryTemplate
          (list(reference(numbersReference, number),
                reference(numbersReference, name)),
           numbersReference,
           new BinaryOperation
           (BinaryOperation.Type.LessThan,
            reference(numbersReference, name),
            new Parameter()));
        Object[] parameters2 = { "nine" };

        result = tail.diff(first, nameLessThan, parameters2);

        assertEquals(result.nextRow(), QueryResult.Type.Inserted);
        assertEquals(result.nextItem(), 4);
        assertEquals(result.nextItem(), "four");
        assertEquals(result.nextRow(), QueryResult.Type.Inserted);
        assertEquals(result.nextItem(), 5);
        assertEquals(result.nextItem(), "five");
        assertEquals(result.nextRow(), QueryResult.Type.Inserted);
        assertEquals(result.nextItem(), 8);
        assertEquals(result.nextItem(), "eight");
        assertEquals(result.nextRow(), QueryResult.Type.Inserted);
        assertEquals(result.nextItem(), 11);
        assertEquals(result.nextItem(), "eleven");
        assertEquals(result.nextRow(), QueryResult.Type.End);
    }
    
    @Test
    public void testIndexedColumnUpdates(){
        Column<Integer> number = new Column<Integer>(Integer.class);
        Column<String> name = new Column<String>(String.class);
        Table numbers = new Table(cols(number));

        Revision tail = Revisions.Empty;

        PatchTemplate insert = new InsertTemplate
          (numbers,
           cols(number, name),
           list((Expression) new Parameter(), new Parameter()),
           DuplicateKeyResolution.Throw);

        RevisionBuilder builder = tail.builder();

        builder.apply(insert, 1, "one");
        builder.apply(insert, 2, "two");
        builder.apply(insert, 3, "three");

        Revision first = builder.commit();

        TableReference numbersReference = new TableReference(numbers);

        PatchTemplate updateNumberWhereNumberEqual = new UpdateTemplate
          (numbersReference,
           new BinaryOperation
           (BinaryOperation.Type.Equal,
            reference(numbersReference, number),
            new Parameter()),
           cols(number),
           list((Expression) new Parameter()));

        builder = first.builder();

        builder.apply(updateNumberWhereNumberEqual, 3, 4);

        Revision second = builder.commit();

        QueryTemplate numberEqual = new QueryTemplate
          (list(reference(numbersReference, name)),
           numbersReference,
           new BinaryOperation
           (BinaryOperation.Type.Equal,
            reference(numbersReference, number),
            new Parameter()));

        Object[] parameters = { 4 };
        QueryResult result = tail.diff(second, numberEqual, parameters);

        assertEquals(result.nextRow(), QueryResult.Type.Inserted);
        assertEquals(result.nextItem(), "three");
        assertEquals(result.nextRow(), QueryResult.Type.End);
        Object[] parameters1 = { 3 };

        result = tail.diff(second, numberEqual, parameters1);

        assertEquals(result.nextRow(), QueryResult.Type.End);
    }

  @Test
  public void testQueryIterator() {
    Column<Integer> number = new Column<Integer>(Integer.class);
    Column<String> name = new Column<String>(String.class);
    Table numbers = new Table(cols(number));

    Revision tail = Revisions.Empty;

    PatchTemplate insert = new InsertTemplate
      (numbers,
       cols(number, name),
       list((Expression) new Parameter(), new Parameter()),
       DuplicateKeyResolution.Throw);

    RevisionBuilder builder = tail.builder();

    builder.apply(insert,  1, "one");
    builder.apply(insert,  2, "two");
    builder.apply(insert,  3, "three");
    builder.apply(insert,  4, "four");
    builder.apply(insert,  5, "five");
    builder.apply(insert,  6, "six");
    builder.apply(insert,  7, "seven");
    builder.apply(insert,  8, "eight");
    builder.apply(insert,  9, "nine");
    builder.apply(insert, 10, "ten");
    builder.apply(insert, 11, "eleven");
    builder.apply(insert, 12, "twelve");
    builder.apply(insert, 13, "thirteen");

    Revision head = builder.commit();

    { Iterator<Integer> it = head.queryAll(number, numbers.primaryKey);

      assertTrue(it.hasNext());

      assertEquals( 1, (int) it.next());
      assertEquals( 2, (int) it.next());
      assertEquals( 3, (int) it.next());
      assertEquals( 4, (int) it.next());
      assertEquals( 5, (int) it.next());
      assertEquals( 6, (int) it.next());
      assertEquals( 7, (int) it.next());
      assertEquals( 8, (int) it.next());
      assertEquals( 9, (int) it.next());
      assertEquals(10, (int) it.next());
      assertEquals(11, (int) it.next());
      assertEquals(12, (int) it.next());
      assertEquals(13, (int) it.next());

      assertTrue(! it.hasNext());
    }

    { Iterator<String> it = head.queryAll(name, numbers.primaryKey);

      assertTrue(it.hasNext());

      assertEquals("one",      it.next());
      assertEquals("two",      it.next());
      assertEquals("three",    it.next());
      assertEquals("four",     it.next());
      assertEquals("five",     it.next());
      assertEquals("six",      it.next());
      assertEquals("seven",    it.next());
      assertEquals("eight",    it.next());
      assertEquals("nine",     it.next());
      assertEquals("ten",      it.next());
      assertEquals("eleven",   it.next());
      assertEquals("twelve",   it.next());
      assertEquals("thirteen", it.next());

      assertTrue(! it.hasNext());
    }
  }

  @Test
  public void testQueryIteratorSubset() {
    Column<Integer> numerator = new Column<Integer>(Integer.class);
    Column<Integer> denominator = new Column<Integer>(Integer.class);
    Column<String> name = new Column<String>(String.class);
    Table fractions = new Table(cols(numerator, denominator));

    Revision head = Revisions.Empty.builder().table(fractions)
      .row(1, 2).update(name, "one half")
      .row(1, 3).update(name, "one third")
      .row(1, 4).update(name, "one quarter")
      .row(2, 3).update(name, "two thirds")
      .row(2, 5).update(name, "two fifths")
      .commit();

    { Iterator<String> it = head.queryAll(name, fractions.primaryKey);

      assertTrue(it.hasNext());

      assertEquals("one half",    it.next());
      assertEquals("one third",   it.next());
      assertEquals("one quarter", it.next());
      assertEquals("two thirds",  it.next());
      assertEquals("two fifths",  it.next());

      assertTrue(! it.hasNext());
    }

    { Iterator<String> it = head.queryAll(name, fractions.primaryKey, 1);

      assertTrue(it.hasNext());

      assertEquals("one half",    it.next());
      assertEquals("one third",   it.next());
      assertEquals("one quarter", it.next());

      assertTrue(! it.hasNext());
    }

    { Iterator<String> it = head.queryAll(name, fractions.primaryKey, 2);

      assertTrue(it.hasNext());

      assertEquals("two thirds",  it.next());
      assertEquals("two fifths",  it.next());

      assertTrue(! it.hasNext());
    }

    { Iterator<String> it = head.queryAll(name, fractions.primaryKey, 1, 3);

      assertTrue(it.hasNext());

      assertEquals("one third",   it.next());

      assertTrue(! it.hasNext());
    }
  }

  @Test
  public void testDeleteNotEqual() {
    Column<Integer> number = new Column<Integer>(Integer.class);
    Column<String> name = new Column<String>(String.class);
    Table numbers = new Table(cols(number));

    TableReference numbersReference = reference(numbers);

    Revision head = Revisions.Empty.builder()
      .table(numbers)
      .row(1).update(name, "ichi")
      .row(2).update(name, "ni")
      .commit();

    assertEquals("ichi", head.query(name, numbers.primaryKey, 1));
    assertEquals("ni",   head.query(name, numbers.primaryKey, 2));

    RevisionBuilder builder = head.builder();

    builder.apply
      (new DeleteTemplate
       (numbersReference, and
        (not(equal(reference(numbersReference, name), constant("ni"))),
         equal(reference(numbersReference, number), constant(1)))));

    Revision next = builder.commit();

    assertEquals("ichi", head.query(name, numbers.primaryKey, 1));
    assertEquals("ni",   head.query(name, numbers.primaryKey, 2));
    assertEquals(null,   next.query(name, numbers.primaryKey, 1));
    assertEquals("ni",   next.query(name, numbers.primaryKey, 2));
  }
}

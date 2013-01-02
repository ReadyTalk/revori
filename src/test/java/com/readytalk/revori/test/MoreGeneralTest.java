/* Copyright (c) 2010-2012, Revori Contributors

   Permission to use, copy, modify, and/or distribute this software
   for any purpose with or without fee is hereby granted, provided
   that the above copyright notice and this permission notice appear
   in all copies. */

package com.readytalk.revori.test;

import static com.readytalk.revori.ExpressionFactory.reference;
import static com.readytalk.revori.util.Util.cols;
import static com.readytalk.revori.util.Util.list;
import static org.junit.Assert.assertEquals;

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

public class MoreGeneralTest {
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
        Object[] parameters = { 42 };
        
        QueryResult result = tail.diff(first, equal, parameters);
        
        assertEquals(QueryResult.Type.Inserted, result.nextRow());
        assertEquals("forty two", result.nextItem());
        assertEquals(QueryResult.Type.End, result.nextRow());
        Object[] parameters1 = { 42 };
        
        result = first.diff(tail, equal, parameters1);
        
        assertEquals(QueryResult.Type.Deleted, result.nextRow());
        assertEquals("forty two", result.nextItem());
        assertEquals(QueryResult.Type.End, result.nextRow());
        Object[] parameters2 = { 43 };
        
        result = tail.diff(first, equal, parameters2);
        
        assertEquals(QueryResult.Type.End, result.nextRow());
        Object[] parameters3 = { 42 };
        
        result = tail.diff(tail, equal, parameters3);
        
        assertEquals(QueryResult.Type.End, result.nextRow());
        Object[] parameters4 = { 42 };
        
        result = first.diff(first, equal, parameters4);
        
        assertEquals(QueryResult.Type.End, result.nextRow());
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
        Object[] parameters = { 42 };
        
        QueryResult result = tail.diff(first, equal, parameters);
        
        assertEquals(QueryResult.Type.Inserted, result.nextRow());
        assertEquals("forty two", result.nextItem());
        assertEquals(QueryResult.Type.End, result.nextRow());
        Object[] parameters1 = { 42 };
        result = first.diff(tail, equal, parameters1);
        
        assertEquals(QueryResult.Type.Deleted, result.nextRow());
        assertEquals("forty two", result.nextItem());
        assertEquals(QueryResult.Type.End, result.nextRow());
        Object[] parameters2 = { 43 };
        result = tail.diff(first, equal, parameters2);
        
        assertEquals(QueryResult.Type.Inserted, result.nextRow());
        assertEquals("forty three", result.nextItem());
        assertEquals(QueryResult.Type.End, result.nextRow());
        Object[] parameters3 = { 42 };
        result = tail.diff(tail, equal, parameters3);
        
        assertEquals(QueryResult.Type.End, result.nextRow());
        Object[] parameters4 = { 42 };
        
        result = first.diff(first, equal, parameters4);
        
        assertEquals(QueryResult.Type.End, result.nextRow());
        
        builder = first.builder();
        
        builder.apply(insert, 1, "one");
        builder.apply(insert, 3, "three");
        builder.apply(insert, 5, "five");
        builder.apply(insert, 6, "six");
        
        Revision second = builder.commit();
        Object[] parameters5 = { 43 };
        
        result = tail.diff(second, equal, parameters5);
        
        assertEquals(QueryResult.Type.Inserted, result.nextRow());
        assertEquals("forty three", result.nextItem());
        assertEquals(QueryResult.Type.End, result.nextRow());
        Object[] parameters6 = { 43 };
        
        result = first.diff(second, equal, parameters6);
        
        assertEquals(QueryResult.Type.End, result.nextRow());
        Object[] parameters7 = { 5 };
        
        result = first.diff(second, equal, parameters7);
        
        assertEquals(QueryResult.Type.Inserted, result.nextRow());
        assertEquals("five", result.nextItem());
        assertEquals(QueryResult.Type.End, result.nextRow());
        Object[] parameters8 = { 5 };
        
        result = tail.diff(first, equal, parameters8);
        
        assertEquals(QueryResult.Type.End, result.nextRow());
        Object[] parameters9 = { 5 };
        
        result = second.diff(first, equal, parameters9);
        
        assertEquals(QueryResult.Type.Deleted, result.nextRow());
        assertEquals("five", result.nextItem());
        assertEquals(QueryResult.Type.End, result.nextRow());
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

    assertEquals(QueryResult.Type.Deleted, result.nextRow());
    assertEquals("forty three", result.nextItem());
    assertEquals(QueryResult.Type.End, result.nextRow());
    Object[] parameters1 = { 43 };

    result = second.diff(first, equal, parameters1);

    assertEquals(QueryResult.Type.Inserted, result.nextRow());
    assertEquals("forty three", result.nextItem());
    assertEquals(QueryResult.Type.End, result.nextRow());
    Object[] parameters2 = { 43 };

    result = tail.diff(second, equal, parameters2);

    assertEquals(QueryResult.Type.End, result.nextRow());
    Object[] parameters3 = { 42 };
    
    result = first.diff(second, equal, parameters3);

    assertEquals(QueryResult.Type.End, result.nextRow());
    Object[] parameters4 = { 42 };
    
    result = tail.diff(second, equal, parameters4);

    assertEquals(QueryResult.Type.Inserted, result.nextRow());
    assertEquals("forty two", result.nextItem());
    assertEquals(QueryResult.Type.End, result.nextRow());
    Object[] parameters5 = { 42 };
    
    result = second.diff(tail, equal, parameters5);

    assertEquals(QueryResult.Type.Deleted, result.nextRow());
    assertEquals("forty two", result.nextItem());
    assertEquals(QueryResult.Type.End, result.nextRow());

    builder = first.builder();

    builder.apply(delete, 43);
    builder.apply(delete, 42);
    builder.apply(delete, 65);

    second = builder.commit();
    Object[] parameters6 = { 43 };

    result = first.diff(second, equal, parameters6);

    assertEquals(QueryResult.Type.Deleted, result.nextRow());
    assertEquals("forty three", result.nextItem());
    assertEquals(QueryResult.Type.End, result.nextRow());
    Object[] parameters7 = { 43 };

    result = second.diff(first, equal, parameters7);

    assertEquals(QueryResult.Type.Inserted, result.nextRow());
    assertEquals("forty three", result.nextItem());
    assertEquals(QueryResult.Type.End, result.nextRow());
    Object[] parameters8 = { 42 };

    result = first.diff(second, equal, parameters8);

    assertEquals(QueryResult.Type.Deleted, result.nextRow());
    assertEquals("forty two", result.nextItem());
    assertEquals(QueryResult.Type.End, result.nextRow());
    Object[] parameters9 = { 42 };

    result = second.diff(first, equal, parameters9);

    assertEquals(QueryResult.Type.Inserted, result.nextRow());
    assertEquals("forty two", result.nextItem());
    assertEquals(QueryResult.Type.End, result.nextRow());
    Object[] parameters10 = { 65 };

    result = first.diff(second, equal, parameters10);

    assertEquals(QueryResult.Type.Deleted, result.nextRow());
    assertEquals("sixty five", result.nextItem());
    assertEquals(QueryResult.Type.End, result.nextRow());
    Object[] parameters11 = { 65 };

    result = second.diff(first, equal, parameters11);

    assertEquals(QueryResult.Type.Inserted, result.nextRow());
    assertEquals("sixty five", result.nextItem());
    assertEquals(QueryResult.Type.End, result.nextRow());
    Object[] parameters12 = { 2 };
    
    result = first.diff(second, equal, parameters12);

    assertEquals(QueryResult.Type.End, result.nextRow());
    Object[] parameters13 = { 2 };
    
    result = second.diff(first, equal, parameters13);

    assertEquals(QueryResult.Type.End, result.nextRow());

    builder = second.builder();

    builder.apply(delete, 44);
    builder.apply(delete,  2);
    builder.apply(delete,  8);

    Revision third = builder.commit();
    Object[] parameters14 = { 44 };

    result = second.diff(third, equal, parameters14);

    assertEquals(QueryResult.Type.Deleted, result.nextRow());
    assertEquals("forty four", result.nextItem());
    assertEquals(QueryResult.Type.End, result.nextRow());
    Object[] parameters15 = { 44 };

    result = third.diff(second, equal, parameters15);

    assertEquals(QueryResult.Type.Inserted, result.nextRow());
    assertEquals("forty four", result.nextItem());
    assertEquals(QueryResult.Type.End, result.nextRow());
    Object[] parameters16 = { 44 };

    result = tail.diff(third, equal, parameters16);

    assertEquals(QueryResult.Type.End, result.nextRow());
    Object[] parameters17 = { 42 };

    result = tail.diff(third, equal, parameters17);

    assertEquals(QueryResult.Type.End, result.nextRow());
    	
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

        assertEquals(QueryResult.Type.Deleted, result.nextRow());
        assertEquals("one", result.nextItem());
        assertEquals(QueryResult.Type.Inserted, result.nextRow());
        assertEquals("ichi", result.nextItem());
        assertEquals(QueryResult.Type.End, result.nextRow());
        Object[] parameters1 = { 1 };

        result = second.diff(first, equal, parameters1);

        assertEquals(QueryResult.Type.Deleted, result.nextRow());
        assertEquals("ichi", result.nextItem());
        assertEquals(QueryResult.Type.Inserted, result.nextRow());
        assertEquals("one", result.nextItem());
        assertEquals(QueryResult.Type.End, result.nextRow());
        Object[] parameters2 = { 2 };

        result = first.diff(second, equal, parameters2);

        assertEquals(QueryResult.Type.End, result.nextRow());
        Object[] parameters3 = { 1 };

        result = tail.diff(first, equal, parameters3);

        assertEquals(QueryResult.Type.Inserted, result.nextRow());
        assertEquals("one", result.nextItem());
        assertEquals(QueryResult.Type.End, result.nextRow());
        Object[] parameters4 = { 1 };

        result = tail.diff(second, equal, parameters4);

        assertEquals(QueryResult.Type.Inserted, result.nextRow());
        assertEquals("ichi", result.nextItem());
        assertEquals(QueryResult.Type.End, result.nextRow());

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

        assertEquals(QueryResult.Type.Deleted, result.nextRow());
        assertEquals("six", result.nextItem());
        assertEquals(QueryResult.Type.Inserted, result.nextRow());
        assertEquals("roku", result.nextItem());
        assertEquals(QueryResult.Type.Deleted, result.nextRow());
        assertEquals("seven", result.nextItem());
        assertEquals(QueryResult.Type.Inserted, result.nextRow());
        assertEquals("shichi", result.nextItem());
        assertEquals(QueryResult.Type.Deleted, result.nextRow());
        assertEquals("eleven", result.nextItem());
        assertEquals(QueryResult.Type.Inserted, result.nextRow());
        assertEquals("ju ichi", result.nextItem());
        assertEquals(QueryResult.Type.End, result.nextRow());
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

        assertEquals(QueryResult.Type.Inserted, result.nextRow());
        assertEquals(9, result.nextItem());
        assertEquals("nine", result.nextItem());
        assertEquals(QueryResult.Type.End, result.nextRow());
        Object[] parameters1 = { "nine" };

        result = first.diff(tail, nameEqual, parameters1);

        assertEquals(QueryResult.Type.Deleted, result.nextRow());
        assertEquals(9, result.nextItem());
        assertEquals("nine", result.nextItem());
        assertEquals(QueryResult.Type.End, result.nextRow());

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

        assertEquals(QueryResult.Type.Inserted, result.nextRow());
        assertEquals(4, result.nextItem());
        assertEquals("four", result.nextItem());
        assertEquals(QueryResult.Type.Inserted, result.nextRow());
        assertEquals(5, result.nextItem());
        assertEquals("five", result.nextItem());
        assertEquals(QueryResult.Type.Inserted, result.nextRow());
        assertEquals(8, result.nextItem());
        assertEquals("eight", result.nextItem());
        assertEquals(QueryResult.Type.Inserted, result.nextRow());
        assertEquals(11, result.nextItem());
        assertEquals("eleven", result.nextItem());
        assertEquals(QueryResult.Type.End, result.nextRow());
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

        assertEquals(QueryResult.Type.Inserted, result.nextRow());
        assertEquals("three", result.nextItem());
        assertEquals(QueryResult.Type.End, result.nextRow());
        Object[] parameters1 = { 3 };

        result = tail.diff(second, numberEqual, parameters1);

        assertEquals(QueryResult.Type.End, result.nextRow());
    }
}

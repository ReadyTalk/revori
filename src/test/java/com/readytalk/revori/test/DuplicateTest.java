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
import static org.junit.Assert.fail;

import org.junit.Test;

import com.readytalk.revori.BinaryOperation;
import com.readytalk.revori.Column;
import com.readytalk.revori.Constant;
import com.readytalk.revori.DuplicateKeyException;
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

public class DuplicateTest {

    
    @Test
    public void testDuplicateInsertsThrowAndOverwrite(){
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

        try {
          first.builder().apply(insert, 1, "uno");
          fail("Inside testDuplicateInserts: Expected Duplicate Key Exception... ");
        } catch (DuplicateKeyException e) { }

        try {
          first.builder().apply(insert, 2, "dos");
          fail("Inside testDuplicateInserts: Expected Duplicate Key Exception... ");
        } catch (DuplicateKeyException e) { }

        try {
          first.builder().apply(insert, 3, "tres");
          fail("Inside testDuplicateInserts: Expected Duplicate Key Exception... ");
        } catch (DuplicateKeyException e) { }

        builder = first.builder();

        builder.apply(insert, 4, "cuatro");

        PatchTemplate insertOrUpdate = new InsertTemplate
          (numbers,
           cols(number, name),
           list((Expression) new Parameter(), new Parameter()),
           DuplicateKeyResolution.Overwrite);

        builder.apply(insertOrUpdate, 1, "uno");

        Revision second = builder.commit();

        TableReference numbersReference = new TableReference(numbers);

        QueryTemplate any = new QueryTemplate
          (list((Expression) reference(numbersReference, name)),
           numbersReference,
           new Constant(true));

        QueryResult result = tail.diff(second, any);

        assertEquals(QueryResult.Type.Inserted, result.nextRow());
        assertEquals("uno", result.nextItem());
        assertEquals(QueryResult.Type.Inserted, result.nextRow());
        assertEquals("two", result.nextItem());
        assertEquals(QueryResult.Type.Inserted, result.nextRow());
        assertEquals("three", result.nextItem());
        assertEquals(QueryResult.Type.Inserted, result.nextRow());
        assertEquals("cuatro", result.nextItem());
        assertEquals(QueryResult.Type.End, result.nextRow());	
    }
    
    @Test
    public void testDuplicateInsertsSkip(){
        Column<Integer> number = new Column<Integer>(Integer.class);
        Column<String> name = new Column<String>(String.class);
        Table numbers = new Table(cols(number));

        Revision tail = Revisions.Empty;

        PatchTemplate insert = new InsertTemplate
          (numbers,
           cols(number, name),
           list((Expression) new Parameter(), new Parameter()),
           DuplicateKeyResolution.Skip);

        RevisionBuilder builder = tail.builder();

        builder.apply(insert, 1, "one");
        builder.apply(insert, 2, "two");
        builder.apply(insert, 3, "three");

        Revision first = builder.commit();
        
        TableReference numbersReference = new TableReference(numbers);
        QueryTemplate q1 = new QueryTemplate
        (list(reference(numbersReference, number), 
        		reference(numbersReference, name)),
         numbersReference,
         new Constant(true));
        
        QueryResult result1 = tail.diff(first, q1);
        
        assertEquals(QueryResult.Type.Inserted, result1.nextRow());
        assertEquals(1, result1.nextItem());
        assertEquals("one", result1.nextItem());
        assertEquals(QueryResult.Type.Inserted, result1.nextRow());
        assertEquals(2, result1.nextItem());
        assertEquals("two", result1.nextItem());
        assertEquals(QueryResult.Type.Inserted, result1.nextRow());
        assertEquals(3, result1.nextItem());
        assertEquals("three", result1.nextItem());
        assertEquals(QueryResult.Type.End, result1.nextRow());        
        
        builder = first.builder();

        builder.apply(insert, 1, "uno");
        builder.apply(insert, 2, "dos");
        builder.apply(insert, 3, "tres");
        builder.apply(insert, 4, "cuatro");

        Revision second = builder.commit();

        numbersReference = new TableReference(numbers);

        QueryTemplate any = new QueryTemplate
          (list(reference(numbersReference, number), 
        		  reference(numbersReference, name)),
           numbersReference,
           new Constant(true));

        QueryResult result2 = tail.diff(second, any);

        assertEquals(QueryResult.Type.Inserted, result2.nextRow());
        assertEquals(1, result2.nextItem());
        assertEquals("one", result2.nextItem());
        assertEquals(QueryResult.Type.Inserted, result2.nextRow());
        assertEquals(2, result2.nextItem());
        assertEquals("two", result2.nextItem());
        assertEquals(QueryResult.Type.Inserted, result2.nextRow());
        assertEquals(3, result2.nextItem());
        assertEquals("three", result2.nextItem());
        assertEquals(QueryResult.Type.Inserted, result2.nextRow());
        assertEquals(4, result2.nextItem());
        assertEquals("cuatro", result2.nextItem());
        assertEquals(QueryResult.Type.End, result2.nextRow());
        
        QueryResult result3 = first.diff(second, any);
        
        assertEquals(QueryResult.Type.Inserted, result3.nextRow());
        assertEquals(4, result3.nextItem());
        assertEquals("cuatro", result3.nextItem());
        assertEquals(QueryResult.Type.End, result3.nextRow());
    }
    
    @Test
    public void testDuplicateInsertsOverwrite(){
        Column<Integer> number = new Column<Integer>(Integer.class);
        Column<String> name = new Column<String>(String.class);
        Table numbers = new Table(cols(number));

        Revision tail = Revisions.Empty;

        PatchTemplate insert = new InsertTemplate
          (numbers,
           cols(number, name),
           list((Expression) new Parameter(),
                new Parameter()), DuplicateKeyResolution.Overwrite);

        RevisionBuilder builder = tail.builder();

        builder.apply(insert, 1, "one");
        builder.apply(insert, 2, "two");
        builder.apply(insert, 3, "three");

        Revision first = builder.commit();
        
        TableReference numbersReference = new TableReference(numbers);
        QueryTemplate q1 = new QueryTemplate
        (list(reference(numbersReference, number),
        		reference(numbersReference, name)),
         numbersReference,
         new Constant(true));
        
        QueryResult result1 = tail.diff(first, q1);
        
        assertEquals(QueryResult.Type.Inserted, result1.nextRow());
        assertEquals(1, result1.nextItem());
        assertEquals("one", result1.nextItem());
        assertEquals(QueryResult.Type.Inserted, result1.nextRow());
        assertEquals(2, result1.nextItem());
        assertEquals("two", result1.nextItem());
        assertEquals(QueryResult.Type.Inserted, result1.nextRow());
        assertEquals(3, result1.nextItem());
        assertEquals("three", result1.nextItem());
        assertEquals(QueryResult.Type.End, result1.nextRow());        
        
        builder = first.builder();

        builder.apply(insert, 1, "uno");
        builder.apply(insert, 2, "dos");
        builder.apply(insert, 3, "tres");
        builder.apply(insert, 4, "cuatro");

        Revision second = builder.commit();

        numbersReference = new TableReference(numbers);

        QueryTemplate any = new QueryTemplate
          (list(reference(numbersReference, number), 
        		  reference(numbersReference, name)),
           numbersReference,
           new Constant(true));

        QueryResult result2 = tail.diff(second, any);

        assertEquals(QueryResult.Type.Inserted, result2.nextRow());
        assertEquals(1, result2.nextItem());
        assertEquals("uno", result2.nextItem());
        assertEquals(QueryResult.Type.Inserted, result2.nextRow());
        assertEquals(2, result2.nextItem());
        assertEquals("dos", result2.nextItem());
        assertEquals(QueryResult.Type.Inserted, result2.nextRow());
        assertEquals(3, result2.nextItem());
        assertEquals("tres", result2.nextItem());
        assertEquals(QueryResult.Type.Inserted, result2.nextRow());
        assertEquals(4, result2.nextItem());
        assertEquals("cuatro", result2.nextItem());
        assertEquals(QueryResult.Type.End, result2.nextRow());
        
        QueryResult result3 = first.diff(second, any);
        
        assertEquals(QueryResult.Type.Deleted, result3.nextRow());
        assertEquals(1, result3.nextItem());
        assertEquals("one", result3.nextItem());
        assertEquals(QueryResult.Type.Inserted, result3.nextRow());
        assertEquals(1, result3.nextItem());
        assertEquals("uno", result3.nextItem());
        assertEquals(QueryResult.Type.Deleted, result3.nextRow());
        assertEquals(2, result3.nextItem());
        assertEquals("two", result3.nextItem());
        assertEquals(QueryResult.Type.Inserted, result3.nextRow());
        assertEquals(2, result3.nextItem());
        assertEquals("dos", result3.nextItem());
        assertEquals(QueryResult.Type.Deleted, result3.nextRow());
        assertEquals(3, result3.nextItem());
        assertEquals("three", result3.nextItem());
        assertEquals(QueryResult.Type.Inserted, result3.nextRow());
        assertEquals(3, result3.nextItem());
        assertEquals("tres", result3.nextItem());
        assertEquals(QueryResult.Type.Inserted, result3.nextRow());
        assertEquals(4, result3.nextItem());
        assertEquals("cuatro", result3.nextItem());
        assertEquals(QueryResult.Type.End, result3.nextRow());
    }
    
    @Test
    public void testDuplicateInsertsMultiKeyThrow(){
        Column<Integer> number = new Column<Integer>(Integer.class);
        Column<Integer> key = new Column<Integer>(Integer.class);
        Column<String> name = new Column<String>(String.class);
        Table numbers = new Table(cols(number, key));

        Revision tail = Revisions.Empty;

        PatchTemplate insert = new InsertTemplate
          (numbers,
           cols(number, name, key),
           list((Expression) new Parameter(), new Parameter(),
                new Parameter()), DuplicateKeyResolution.Throw);

        RevisionBuilder builder = tail.builder();

        builder.apply(insert, 1, "one", 1);
        builder.apply(insert, 2, "two", 2);
        builder.apply(insert, 3, "three", 3);

        Revision first = builder.commit();
        
        TableReference numbersReference = new TableReference(numbers);
        QueryTemplate q1 = new QueryTemplate
        (list(reference(numbersReference, number),
        		reference(numbersReference, name),
        		reference(numbersReference, key)),
         numbersReference,
         new Constant(true));
        
        QueryResult result1 = tail.diff(first, q1);
        
        assertEquals(QueryResult.Type.Inserted, result1.nextRow());
        assertEquals(1, result1.nextItem());
        assertEquals("one", result1.nextItem());
        assertEquals(1, result1.nextItem());
        assertEquals(QueryResult.Type.Inserted, result1.nextRow());
        assertEquals(2, result1.nextItem());
        assertEquals("two", result1.nextItem());
        assertEquals(2, result1.nextItem());
        assertEquals(QueryResult.Type.Inserted, result1.nextRow());
        assertEquals(3, result1.nextItem());
        assertEquals("three", result1.nextItem());
        assertEquals(3, result1.nextItem());
        assertEquals(QueryResult.Type.End, result1.nextRow());        
        
        builder = first.builder();

        try{
            builder.apply(insert, 1, "uno", 1);
            fail("Inside testDuplicateInsertsMultiKeyThrow: Expected Duplicate Key Exception...");
        }catch(DuplicateKeyException expected){}
        builder = first.builder();
        try{
            builder.apply(insert, 2, "dos", 2);
            fail("Inside testDuplicateInsertsMultiKeyThrow: Expected Duplicate Key Exception...");
        }catch(DuplicateKeyException expected){}
        builder = first.builder();
        try{
            builder.apply(insert, 3, "tres", 3);
            fail("Inside testDuplicateInsertsMultiKeyThrow: Expected Duplicate Key Exception...");
        }catch(DuplicateKeyException expected){}
        builder = first.builder();
        
        builder.apply(insert, 4, "cuatro", 4);

        Revision second = builder.commit();

        numbersReference = new TableReference(numbers);

        QueryTemplate any = new QueryTemplate
          (list(reference(numbersReference, number), 
        		  reference(numbersReference, name),
        		  reference(numbersReference, key)),
           numbersReference,
           new Constant(true));

        QueryResult result2 = tail.diff(second, any);

        assertEquals(QueryResult.Type.Inserted, result2.nextRow());
        assertEquals(1, result2.nextItem());
        assertEquals("one", result2.nextItem());
        assertEquals(1, result2.nextItem());
        assertEquals(QueryResult.Type.Inserted, result2.nextRow());
        assertEquals(2, result2.nextItem());
        assertEquals("two", result2.nextItem());
        assertEquals(2, result2.nextItem());
        assertEquals(QueryResult.Type.Inserted, result2.nextRow());
        assertEquals(3, result2.nextItem());
        assertEquals("three", result2.nextItem());
        assertEquals(3, result2.nextItem());
        assertEquals(QueryResult.Type.Inserted, result2.nextRow());
        assertEquals(4, result2.nextItem());
        assertEquals("cuatro", result2.nextItem());
        assertEquals(4, result2.nextItem());
        assertEquals(QueryResult.Type.End, result2.nextRow());
        
        QueryResult result3 = first.diff(second, any);
        
        assertEquals(QueryResult.Type.Inserted, result3.nextRow());
        assertEquals(4, result3.nextItem());
        assertEquals("cuatro", result3.nextItem());
        assertEquals(4, result3.nextItem());
        assertEquals(QueryResult.Type.End, result3.nextRow());
    }
    
    @Test
    public void testDuplicateInsertsMultiKeySkip(){
        Column<Integer> number = new Column<Integer>(Integer.class);
        Column<Integer> key = new Column<Integer>(Integer.class);
        Column<String> name = new Column<String>(String.class);
        Table numbers = new Table(cols(number, key));

        Revision tail = Revisions.Empty;

        PatchTemplate insert = new InsertTemplate
          (numbers,
           cols(number, name, key),
           list((Expression) new Parameter(), new Parameter(),
                new Parameter()), DuplicateKeyResolution.Skip);

        RevisionBuilder builder = tail.builder();

        builder.apply(insert, 1, "one", 1);
        builder.apply(insert, 2, "two", 2);
        builder.apply(insert, 3, "three", 3);

        Revision first = builder.commit();
        
        TableReference numbersReference = new TableReference(numbers);
        QueryTemplate q1 = new QueryTemplate
        (list(reference(numbersReference, number),
        		reference(numbersReference, name),
        		reference(numbersReference, key)),
         numbersReference,
         new Constant(true));
        
        QueryResult result1 = tail.diff(first, q1);
        
        assertEquals(QueryResult.Type.Inserted, result1.nextRow());
        assertEquals(1, result1.nextItem());
        assertEquals("one", result1.nextItem());
        assertEquals(1, result1.nextItem());
        assertEquals(QueryResult.Type.Inserted, result1.nextRow());
        assertEquals(2, result1.nextItem());
        assertEquals("two", result1.nextItem());
        assertEquals(2, result1.nextItem());
        assertEquals(QueryResult.Type.Inserted, result1.nextRow());
        assertEquals(3, result1.nextItem());
        assertEquals("three", result1.nextItem());
        assertEquals(3, result1.nextItem());
        assertEquals(QueryResult.Type.End, result1.nextRow());        
        
        builder = first.builder();

        builder.apply(insert, 1, "uno", 1);
        builder.apply(insert, 2, "dos", 2);
        builder.apply(insert, 3, "tres", 3);
        builder.apply(insert, 4, "cuatro", 4);

        Revision second = builder.commit();

        numbersReference = new TableReference(numbers);

        QueryTemplate any = new QueryTemplate
          (list(reference(numbersReference, number), 
        		  reference(numbersReference, name),
        		  reference(numbersReference, key)),
           numbersReference,
           new Constant(true));

        QueryResult result2 = tail.diff(second, any);

        assertEquals(QueryResult.Type.Inserted, result2.nextRow());
        assertEquals(1, result2.nextItem());
        assertEquals("one", result2.nextItem());
        assertEquals(1, result2.nextItem());
        assertEquals(QueryResult.Type.Inserted, result2.nextRow());
        assertEquals(2, result2.nextItem());
        assertEquals("two", result2.nextItem());
        assertEquals(2, result2.nextItem());
        assertEquals(QueryResult.Type.Inserted, result2.nextRow());
        assertEquals(3, result2.nextItem());
        assertEquals("three", result2.nextItem());
        assertEquals(3, result2.nextItem());
        assertEquals(QueryResult.Type.Inserted, result2.nextRow());
        assertEquals(4, result2.nextItem());
        assertEquals("cuatro", result2.nextItem());
        assertEquals(4, result2.nextItem());
        assertEquals(QueryResult.Type.End, result2.nextRow());
        
        QueryResult result3 = first.diff(second, any);
        
        assertEquals(QueryResult.Type.Inserted, result3.nextRow());
        assertEquals(4, result3.nextItem());
        assertEquals("cuatro", result3.nextItem());
        assertEquals(4, result3.nextItem());
        assertEquals(QueryResult.Type.End, result3.nextRow());
    }
    
    @Test
    public void testDuplicateInsertsMultiKeyOverwrite(){
        Column<Integer> number = new Column<Integer>(Integer.class);
        Column<Integer> key = new Column<Integer>(Integer.class);
        Column<String> name = new Column<String>(String.class);
        Table numbers = new Table(cols(number, key));

        Revision tail = Revisions.Empty;

        PatchTemplate insert = new InsertTemplate
          (numbers,
           cols(number, name, key),
           list((Expression) new Parameter(), new Parameter(),
                new Parameter()), DuplicateKeyResolution.Overwrite);

        RevisionBuilder builder = tail.builder();

        builder.apply(insert, 1, "one", 1);
        builder.apply(insert, 2, "two", 2);
        builder.apply(insert, 3, "three", 3);

        Revision first = builder.commit();
        
        TableReference numbersReference = new TableReference(numbers);
        QueryTemplate q1 = new QueryTemplate
        (list(reference(numbersReference, number),
        		reference(numbersReference, name),
        		reference(numbersReference, key)),
         numbersReference,
         new Constant(true));
        
        QueryResult result1 = tail.diff(first, q1);
        
        assertEquals(QueryResult.Type.Inserted, result1.nextRow());
        assertEquals(1, result1.nextItem());
        assertEquals("one", result1.nextItem());
        assertEquals(1, result1.nextItem());
        assertEquals(QueryResult.Type.Inserted, result1.nextRow());
        assertEquals(2, result1.nextItem());
        assertEquals("two", result1.nextItem());
        assertEquals(2, result1.nextItem());
        assertEquals(QueryResult.Type.Inserted, result1.nextRow());
        assertEquals(3, result1.nextItem());
        assertEquals("three", result1.nextItem());
        assertEquals(3, result1.nextItem());
        assertEquals(QueryResult.Type.End, result1.nextRow());        
        
        builder = first.builder();

        builder.apply(insert, 1, "uno", 1);
        builder.apply(insert, 2, "dos", 2);
        builder.apply(insert, 3, "tres", 3);
        builder.apply(insert, 4, "cuatro", 4);

        Revision second = builder.commit();

        numbersReference = new TableReference(numbers);

        QueryTemplate any = new QueryTemplate
          (list(reference(numbersReference, number), 
        		  reference(numbersReference, name),
        		  reference(numbersReference, key)),
           numbersReference,
           new Constant(true));

        QueryResult result2 = tail.diff(second, any);

        assertEquals(QueryResult.Type.Inserted, result2.nextRow());
        assertEquals(1, result2.nextItem());
        assertEquals("uno", result2.nextItem());
        assertEquals(1, result2.nextItem());
        assertEquals(QueryResult.Type.Inserted, result2.nextRow());
        assertEquals(2, result2.nextItem());
        assertEquals("dos", result2.nextItem());
        assertEquals(2, result2.nextItem());
        assertEquals(QueryResult.Type.Inserted, result2.nextRow());
        assertEquals(3, result2.nextItem());
        assertEquals("tres", result2.nextItem());
        assertEquals(3, result2.nextItem());
        assertEquals(QueryResult.Type.Inserted, result2.nextRow());
        assertEquals(4, result2.nextItem());
        assertEquals("cuatro", result2.nextItem());
        assertEquals(4, result2.nextItem());
        assertEquals(QueryResult.Type.End, result2.nextRow());
        
        QueryResult result3 = first.diff(second, any);
        
        assertEquals(QueryResult.Type.Deleted, result3.nextRow());
        assertEquals(1, result3.nextItem());
        assertEquals("one", result3.nextItem());
        assertEquals(1, result3.nextItem());
        assertEquals(QueryResult.Type.Inserted, result3.nextRow());
        assertEquals(1, result3.nextItem());
        assertEquals("uno", result3.nextItem());
        assertEquals(1, result3.nextItem());
        assertEquals(QueryResult.Type.Deleted, result3.nextRow());
        assertEquals(2, result3.nextItem());
        assertEquals("two", result3.nextItem());
        assertEquals(2, result3.nextItem());
        assertEquals(QueryResult.Type.Inserted, result3.nextRow());
        assertEquals(2, result3.nextItem());
        assertEquals("dos", result3.nextItem());
        assertEquals(2, result3.nextItem());
        assertEquals(QueryResult.Type.Deleted, result3.nextRow());
        assertEquals(3, result3.nextItem());
        assertEquals("three", result3.nextItem());
        assertEquals(3, result3.nextItem());
        assertEquals(QueryResult.Type.Inserted, result3.nextRow());
        assertEquals(3, result3.nextItem());
        assertEquals("tres", result3.nextItem());
        assertEquals(3, result3.nextItem());
        assertEquals(QueryResult.Type.Inserted, result3.nextRow());
        assertEquals(4, result3.nextItem());
        assertEquals("cuatro", result3.nextItem());
        assertEquals(4, result3.nextItem());
        assertEquals(QueryResult.Type.End, result3.nextRow());
    }
    

    @Test
    public void testDuplicateUpdates(){
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

        try {
          first.builder().apply(updateNumberWhereNumberEqual, 1, 2);
          throw new RuntimeException();
        } catch (DuplicateKeyException e) { }

        try {
          first.builder().apply(updateNumberWhereNumberEqual, 2, 3);
          throw new RuntimeException();
        } catch (DuplicateKeyException e) { }

        builder = first.builder();

        builder.apply(updateNumberWhereNumberEqual, 3, 3);
        builder.apply(updateNumberWhereNumberEqual, 4, 2);
        builder.apply(updateNumberWhereNumberEqual, 3, 4);

        Revision second = builder.commit();

        QueryTemplate any = new QueryTemplate
          (list(reference(numbersReference, number),
                reference(numbersReference, name)),
           numbersReference,
           new Constant(true));

        QueryResult result = tail.diff(second, any);

        assertEquals(QueryResult.Type.Inserted, result.nextRow());
        assertEquals(1, result.nextItem());
        assertEquals("one", result.nextItem());
        assertEquals(QueryResult.Type.Inserted, result.nextRow());
        assertEquals(2, result.nextItem());
        assertEquals("two", result.nextItem());
        assertEquals(QueryResult.Type.Inserted, result.nextRow());
        assertEquals(4, result.nextItem());
        assertEquals("three", result.nextItem());
        assertEquals(QueryResult.Type.End, result.nextRow());
    	
    }

}

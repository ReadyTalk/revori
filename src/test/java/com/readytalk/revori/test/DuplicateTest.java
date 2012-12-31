/* Copyright (c) 2010-2012, Revori Contributors

   Permission to use, copy, modify, and/or distribute this software
   for any purpose with or without fee is hereby granted, provided
   that the above copyright notice and this permission notice appear
   in all copies. */

package com.readytalk.revori.test;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.fail;

import junit.framework.TestCase;

import org.junit.Test;

import static com.readytalk.revori.util.Util.list;
import static com.readytalk.revori.util.Util.cols;
import static com.readytalk.revori.ExpressionFactory.reference;

import com.readytalk.revori.BinaryOperation;
import com.readytalk.revori.Column;
import com.readytalk.revori.DuplicateKeyException;
import com.readytalk.revori.Revisions;
import com.readytalk.revori.Table;
import com.readytalk.revori.Expression;
import com.readytalk.revori.Revision;
import com.readytalk.revori.RevisionBuilder;
import com.readytalk.revori.PatchTemplate;
import com.readytalk.revori.InsertTemplate;
import com.readytalk.revori.UpdateTemplate;
import com.readytalk.revori.TableReference;
import com.readytalk.revori.ColumnReference;
import com.readytalk.revori.QueryTemplate;
import com.readytalk.revori.QueryResult;
import com.readytalk.revori.Parameter;
import com.readytalk.revori.Constant;
import com.readytalk.revori.DuplicateKeyResolution;

public class DuplicateTest {

  private static void expectEqual(Object actual, Object expected) {
    assertEquals(expected, actual);
  }  
    
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

        expectEqual(result.nextRow(), QueryResult.Type.Inserted);
        expectEqual(result.nextItem(), "uno");
        expectEqual(result.nextRow(), QueryResult.Type.Inserted);
        expectEqual(result.nextItem(), "two");
        expectEqual(result.nextRow(), QueryResult.Type.Inserted);
        expectEqual(result.nextItem(), "three");
        expectEqual(result.nextRow(), QueryResult.Type.Inserted);
        expectEqual(result.nextItem(), "cuatro");
        expectEqual(result.nextRow(), QueryResult.Type.End);	
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
        
        expectEqual(result1.nextRow(), QueryResult.Type.Inserted);
        expectEqual(result1.nextItem(), 1);
        expectEqual(result1.nextItem(), "one");
        expectEqual(result1.nextRow(), QueryResult.Type.Inserted);
        expectEqual(result1.nextItem(), 2);
        expectEqual(result1.nextItem(), "two");
        expectEqual(result1.nextRow(), QueryResult.Type.Inserted);
        expectEqual(result1.nextItem(), 3);
        expectEqual(result1.nextItem(), "three");
        expectEqual(result1.nextRow(), QueryResult.Type.End);        
        
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

        expectEqual(result2.nextRow(), QueryResult.Type.Inserted);
        expectEqual(result2.nextItem(), 1);
        expectEqual(result2.nextItem(), "one");
        expectEqual(result2.nextRow(), QueryResult.Type.Inserted);
        expectEqual(result2.nextItem(), 2);
        expectEqual(result2.nextItem(), "two");
        expectEqual(result2.nextRow(), QueryResult.Type.Inserted);
        expectEqual(result2.nextItem(), 3);
        expectEqual(result2.nextItem(), "three");
        expectEqual(result2.nextRow(), QueryResult.Type.Inserted);
        expectEqual(result2.nextItem(), 4);
        expectEqual(result2.nextItem(), "cuatro");
        expectEqual(result2.nextRow(), QueryResult.Type.End);
        
        QueryResult result3 = first.diff(second, any);
        
        expectEqual(result3.nextRow(), QueryResult.Type.Inserted);
        expectEqual(result3.nextItem(), 4);
        expectEqual(result3.nextItem(), "cuatro");
        expectEqual(result3.nextRow(), QueryResult.Type.End);
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
        
        expectEqual(result1.nextRow(), QueryResult.Type.Inserted);
        expectEqual(result1.nextItem(), 1);
        expectEqual(result1.nextItem(), "one");
        expectEqual(result1.nextRow(), QueryResult.Type.Inserted);
        expectEqual(result1.nextItem(), 2);
        expectEqual(result1.nextItem(), "two");
        expectEqual(result1.nextRow(), QueryResult.Type.Inserted);
        expectEqual(result1.nextItem(), 3);
        expectEqual(result1.nextItem(), "three");
        expectEqual(result1.nextRow(), QueryResult.Type.End);        
        
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

        expectEqual(result2.nextRow(), QueryResult.Type.Inserted);
        expectEqual(result2.nextItem(), 1);
        expectEqual(result2.nextItem(), "uno");
        expectEqual(result2.nextRow(), QueryResult.Type.Inserted);
        expectEqual(result2.nextItem(), 2);
        expectEqual(result2.nextItem(), "dos");
        expectEqual(result2.nextRow(), QueryResult.Type.Inserted);
        expectEqual(result2.nextItem(), 3);
        expectEqual(result2.nextItem(), "tres");
        expectEqual(result2.nextRow(), QueryResult.Type.Inserted);
        expectEqual(result2.nextItem(), 4);
        expectEqual(result2.nextItem(), "cuatro");
        expectEqual(result2.nextRow(), QueryResult.Type.End);
        
        QueryResult result3 = first.diff(second, any);
        
        expectEqual(result3.nextRow(), QueryResult.Type.Deleted);
        expectEqual(result3.nextItem(), 1);
        expectEqual(result3.nextItem(), "one");
        expectEqual(result3.nextRow(), QueryResult.Type.Inserted);
        expectEqual(result3.nextItem(), 1);
        expectEqual(result3.nextItem(), "uno");
        expectEqual(result3.nextRow(), QueryResult.Type.Deleted);
        expectEqual(result3.nextItem(), 2);
        expectEqual(result3.nextItem(), "two");
        expectEqual(result3.nextRow(), QueryResult.Type.Inserted);
        expectEqual(result3.nextItem(), 2);
        expectEqual(result3.nextItem(), "dos");
        expectEqual(result3.nextRow(), QueryResult.Type.Deleted);
        expectEqual(result3.nextItem(), 3);
        expectEqual(result3.nextItem(), "three");
        expectEqual(result3.nextRow(), QueryResult.Type.Inserted);
        expectEqual(result3.nextItem(), 3);
        expectEqual(result3.nextItem(), "tres");
        expectEqual(result3.nextRow(), QueryResult.Type.Inserted);
        expectEqual(result3.nextItem(), 4);
        expectEqual(result3.nextItem(), "cuatro");
        expectEqual(result3.nextRow(), QueryResult.Type.End);
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
        
        expectEqual(result1.nextRow(), QueryResult.Type.Inserted);
        expectEqual(result1.nextItem(), 1);
        expectEqual(result1.nextItem(), "one");
        expectEqual(result1.nextItem(), 1);
        expectEqual(result1.nextRow(), QueryResult.Type.Inserted);
        expectEqual(result1.nextItem(), 2);
        expectEqual(result1.nextItem(), "two");
        expectEqual(result1.nextItem(), 2);
        expectEqual(result1.nextRow(), QueryResult.Type.Inserted);
        expectEqual(result1.nextItem(), 3);
        expectEqual(result1.nextItem(), "three");
        expectEqual(result1.nextItem(), 3);
        expectEqual(result1.nextRow(), QueryResult.Type.End);        
        
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

        expectEqual(result2.nextRow(), QueryResult.Type.Inserted);
        expectEqual(result2.nextItem(), 1);
        expectEqual(result2.nextItem(), "one");
        expectEqual(result2.nextItem(), 1);
        expectEqual(result2.nextRow(), QueryResult.Type.Inserted);
        expectEqual(result2.nextItem(), 2);
        expectEqual(result2.nextItem(), "two");
        expectEqual(result2.nextItem(), 2);
        expectEqual(result2.nextRow(), QueryResult.Type.Inserted);
        expectEqual(result2.nextItem(), 3);
        expectEqual(result2.nextItem(), "three");
        expectEqual(result2.nextItem(), 3);
        expectEqual(result2.nextRow(), QueryResult.Type.Inserted);
        expectEqual(result2.nextItem(), 4);
        expectEqual(result2.nextItem(), "cuatro");
        expectEqual(result2.nextItem(), 4);
        expectEqual(result2.nextRow(), QueryResult.Type.End);
        
        QueryResult result3 = first.diff(second, any);
        
        expectEqual(result3.nextRow(), QueryResult.Type.Inserted);
        expectEqual(result3.nextItem(), 4);
        expectEqual(result3.nextItem(), "cuatro");
        expectEqual(result3.nextItem(), 4);
        expectEqual(result3.nextRow(), QueryResult.Type.End);
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
        
        expectEqual(result1.nextRow(), QueryResult.Type.Inserted);
        expectEqual(result1.nextItem(), 1);
        expectEqual(result1.nextItem(), "one");
        expectEqual(result1.nextItem(), 1);
        expectEqual(result1.nextRow(), QueryResult.Type.Inserted);
        expectEqual(result1.nextItem(), 2);
        expectEqual(result1.nextItem(), "two");
        expectEqual(result1.nextItem(), 2);
        expectEqual(result1.nextRow(), QueryResult.Type.Inserted);
        expectEqual(result1.nextItem(), 3);
        expectEqual(result1.nextItem(), "three");
        expectEqual(result1.nextItem(), 3);
        expectEqual(result1.nextRow(), QueryResult.Type.End);        
        
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

        expectEqual(result2.nextRow(), QueryResult.Type.Inserted);
        expectEqual(result2.nextItem(), 1);
        expectEqual(result2.nextItem(), "one");
        expectEqual(result2.nextItem(), 1);
        expectEqual(result2.nextRow(), QueryResult.Type.Inserted);
        expectEqual(result2.nextItem(), 2);
        expectEqual(result2.nextItem(), "two");
        expectEqual(result2.nextItem(), 2);
        expectEqual(result2.nextRow(), QueryResult.Type.Inserted);
        expectEqual(result2.nextItem(), 3);
        expectEqual(result2.nextItem(), "three");
        expectEqual(result2.nextItem(), 3);
        expectEqual(result2.nextRow(), QueryResult.Type.Inserted);
        expectEqual(result2.nextItem(), 4);
        expectEqual(result2.nextItem(), "cuatro");
        expectEqual(result2.nextItem(), 4);
        expectEqual(result2.nextRow(), QueryResult.Type.End);
        
        QueryResult result3 = first.diff(second, any);
        
        expectEqual(result3.nextRow(), QueryResult.Type.Inserted);
        expectEqual(result3.nextItem(), 4);
        expectEqual(result3.nextItem(), "cuatro");
        expectEqual(result3.nextItem(), 4);
        expectEqual(result3.nextRow(), QueryResult.Type.End);
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
        
        expectEqual(result1.nextRow(), QueryResult.Type.Inserted);
        expectEqual(result1.nextItem(), 1);
        expectEqual(result1.nextItem(), "one");
        expectEqual(result1.nextItem(), 1);
        expectEqual(result1.nextRow(), QueryResult.Type.Inserted);
        expectEqual(result1.nextItem(), 2);
        expectEqual(result1.nextItem(), "two");
        expectEqual(result1.nextItem(), 2);
        expectEqual(result1.nextRow(), QueryResult.Type.Inserted);
        expectEqual(result1.nextItem(), 3);
        expectEqual(result1.nextItem(), "three");
        expectEqual(result1.nextItem(), 3);
        expectEqual(result1.nextRow(), QueryResult.Type.End);        
        
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

        expectEqual(result2.nextRow(), QueryResult.Type.Inserted);
        expectEqual(result2.nextItem(), 1);
        expectEqual(result2.nextItem(), "uno");
        expectEqual(result2.nextItem(), 1);
        expectEqual(result2.nextRow(), QueryResult.Type.Inserted);
        expectEqual(result2.nextItem(), 2);
        expectEqual(result2.nextItem(), "dos");
        expectEqual(result2.nextItem(), 2);
        expectEqual(result2.nextRow(), QueryResult.Type.Inserted);
        expectEqual(result2.nextItem(), 3);
        expectEqual(result2.nextItem(), "tres");
        expectEqual(result2.nextItem(), 3);
        expectEqual(result2.nextRow(), QueryResult.Type.Inserted);
        expectEqual(result2.nextItem(), 4);
        expectEqual(result2.nextItem(), "cuatro");
        expectEqual(result2.nextItem(), 4);
        expectEqual(result2.nextRow(), QueryResult.Type.End);
        
        QueryResult result3 = first.diff(second, any);
        
        expectEqual(result3.nextRow(), QueryResult.Type.Deleted);
        expectEqual(result3.nextItem(), 1);
        expectEqual(result3.nextItem(), "one");
        expectEqual(result3.nextItem(), 1);
        expectEqual(result3.nextRow(), QueryResult.Type.Inserted);
        expectEqual(result3.nextItem(), 1);
        expectEqual(result3.nextItem(), "uno");
        expectEqual(result3.nextItem(), 1);
        expectEqual(result3.nextRow(), QueryResult.Type.Deleted);
        expectEqual(result3.nextItem(), 2);
        expectEqual(result3.nextItem(), "two");
        expectEqual(result3.nextItem(), 2);
        expectEqual(result3.nextRow(), QueryResult.Type.Inserted);
        expectEqual(result3.nextItem(), 2);
        expectEqual(result3.nextItem(), "dos");
        expectEqual(result3.nextItem(), 2);
        expectEqual(result3.nextRow(), QueryResult.Type.Deleted);
        expectEqual(result3.nextItem(), 3);
        expectEqual(result3.nextItem(), "three");
        expectEqual(result3.nextItem(), 3);
        expectEqual(result3.nextRow(), QueryResult.Type.Inserted);
        expectEqual(result3.nextItem(), 3);
        expectEqual(result3.nextItem(), "tres");
        expectEqual(result3.nextItem(), 3);
        expectEqual(result3.nextRow(), QueryResult.Type.Inserted);
        expectEqual(result3.nextItem(), 4);
        expectEqual(result3.nextItem(), "cuatro");
        expectEqual(result3.nextItem(), 4);
        expectEqual(result3.nextRow(), QueryResult.Type.End);
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

        expectEqual(result.nextRow(), QueryResult.Type.Inserted);
        expectEqual(result.nextItem(), 1);
        expectEqual(result.nextItem(), "one");
        expectEqual(result.nextRow(), QueryResult.Type.Inserted);
        expectEqual(result.nextItem(), 2);
        expectEqual(result.nextItem(), "two");
        expectEqual(result.nextRow(), QueryResult.Type.Inserted);
        expectEqual(result.nextItem(), 4);
        expectEqual(result.nextItem(), "three");
        expectEqual(result.nextRow(), QueryResult.Type.End);
    	
    }

}

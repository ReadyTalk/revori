/* Copyright (c) 2010-2012, Revori Contributors

   Permission to use, copy, modify, and/or distribute this software
   for any purpose with or without fee is hereby granted, provided
   that the above copyright notice and this permission notice appear
   in all copies. */

package com.readytalk.revori.test;

import static com.readytalk.revori.ExpressionFactory.reference;
import static com.readytalk.revori.util.Util.cols;
import com.google.common.collect.Lists;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

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

public class SimpleTest {
    
	@Test
    public void testSimpleInsertQuery(){
        Column<Integer> key = new Column<Integer>(Integer.class);
        Column<String> firstName = new Column<String>(String.class);
        Column<String> lastName = new Column<String>(String.class);
        Table names = new Table(cols(key));
        Revision tail = Revisions.Empty;
        
        RevisionBuilder builder = tail.builder();
        PatchTemplate insert = new InsertTemplate
         (names,
          cols(key, firstName, lastName),
          Lists.newArrayList((Expression) new Parameter(),
               new Parameter(),
               new Parameter()), DuplicateKeyResolution.Throw);
        builder.apply(insert, 1, "Charles", "Norris");
        builder.apply(insert, 2, "Chuck", "Norris");
        builder.apply(insert, 3, "Chuck", "Taylor");
        
        Revision first = builder.commit();
        
        TableReference namesReference = new TableReference(names);
        
        QueryTemplate any = new QueryTemplate
          (Lists.newArrayList(reference(namesReference, key),
        		  reference(namesReference, firstName),
                  reference(namesReference, lastName)),
                  namesReference,
                  new Constant(true));
        Object[] parameters = {};

        QueryResult result = tail.diff(first, any, parameters);
        
        assertEquals(QueryResult.Type.Inserted, result.nextRow());
        assertEquals(1, result.nextItem());
        assertEquals("Charles", result.nextItem());
        assertEquals("Norris", result.nextItem());
        assertEquals(QueryResult.Type.Inserted, result.nextRow());
        assertEquals(2, result.nextItem());
        assertEquals("Chuck", result.nextItem());
        assertEquals("Norris", result.nextItem());
        assertEquals(QueryResult.Type.Inserted, result.nextRow());
        assertEquals(3, result.nextItem());
        assertEquals("Chuck", result.nextItem());
        assertEquals("Taylor", result.nextItem());
        assertEquals(QueryResult.Type.End, result.nextRow());
        }
    
    @Test
    public void testMultipleColumnInsertQuery(){
        Column<Integer> key = new Column<Integer>(Integer.class);
        Column<String> firstName = new Column<String>(String.class);
        Column<String> lastName = new Column<String>(String.class);
        Column<String> city = new Column<String>(String.class);
        Column<Integer> age = new Column<Integer>(Integer.class);
        Table names = new Table(cols(key));
        Revision tail = Revisions.Empty;
        //Insert 4 columns
        RevisionBuilder builder = tail.builder();
        PatchTemplate insert = new InsertTemplate
         (names,
                 cols(key, firstName, lastName, city),
                 Lists.newArrayList((Expression) new Parameter(),
                         new Parameter(),
                         new Parameter(),
                         new Parameter()), DuplicateKeyResolution.Throw);
        builder.apply(insert, 1, "Charles", "Norris", "Montreal");
        
        Revision first = builder.commit();
        
        //Insert 2 columns
        builder = first.builder();
        insert = new InsertTemplate
         (names,
                 cols(key, firstName),
                 Lists.newArrayList((Expression) new Parameter(),
                         new Parameter()), DuplicateKeyResolution.Throw);
        builder.apply(insert, 2, "Charleston");
        
        Revision second = builder.commit();
        
        //Insert 5 columns
        builder = second.builder();
        insert = new InsertTemplate
         (names,
                 cols(key, firstName, lastName, city, age),
                 Lists.newArrayList((Expression) new Parameter(),
                         new Parameter(),
                         new Parameter(),
                         new Parameter(),
                         new Parameter()), DuplicateKeyResolution.Throw);
        builder.apply(insert, 3, "Wickerson", "Jones", "Lancaster", 54);
        
        Revision third = builder.commit();
        
        
        //Validate data
        TableReference namesReference = new TableReference(names);
        
        QueryTemplate any = new QueryTemplate
          (Lists.newArrayList(reference(namesReference, key),
        		  reference(namesReference, firstName),
                  reference(namesReference, lastName),
                  reference(namesReference, city),
                  reference(namesReference, age)),
                  namesReference,
                  new Constant(true));
        Object[] parameters = {};

          QueryResult result = tail.diff(third, any, parameters);
        
        assertEquals(QueryResult.Type.Inserted, result.nextRow());
        assertEquals(1, result.nextItem());
        assertEquals("Charles", result.nextItem());
        assertEquals("Norris", result.nextItem());
        assertEquals("Montreal", result.nextItem());
        assertNull(result.nextItem());
        assertEquals(QueryResult.Type.Inserted, result.nextRow());
        assertEquals(2, result.nextItem());
        assertEquals("Charleston", result.nextItem());
        assertNull(result.nextItem());
        assertNull(result.nextItem());
        assertNull(result.nextItem());
        assertEquals(QueryResult.Type.Inserted, result.nextRow());
        assertEquals(3, result.nextItem());
        assertEquals("Wickerson", result.nextItem());
        assertEquals("Jones", result.nextItem());
        assertEquals("Lancaster", result.nextItem());
        assertEquals(54, result.nextItem());
        assertEquals(QueryResult.Type.End, result.nextRow());
        }
    
    
    @Test
    public void testNotEnoughColumnsForPrimaryKeyQuery(){
        Column<Integer> key = new Column<Integer>(Integer.class);
        Column<String> firstName = new Column<String>(String.class);
        Column<String> lastName = new Column<String>(String.class);
        Column<String> city = new Column<String>(String.class);
        Table names = new Table(cols(key, city));
        
        try{
          new InsertTemplate
           (names,
            cols(key, firstName, lastName),
            Lists.newArrayList((Expression) new Parameter(),
                           new Parameter(),
                           new Parameter()), DuplicateKeyResolution.Throw);
          fail("Expected IllegalArgumentException...");
        } catch(IllegalArgumentException expected){}
    }
    
    @Test
    public void test() {
    	
    	System.out.println(getClass().getName());
    }
}

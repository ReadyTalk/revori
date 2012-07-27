/* Copyright (c) 2010-2012, Revori Contributors

   Permission to use, copy, modify, and/or distribute this software
   for any purpose with or without fee is hereby granted, provided
   that the above copyright notice and this permission notice appear
   in all copies. */

package Suites;

import junit.framework.TestCase;

import org.junit.Test;

import static com.readytalk.revori.util.Util.list;
import static com.readytalk.revori.util.Util.cols;

import static com.readytalk.revori.ExpressionFactory.reference;

import com.readytalk.revori.BinaryOperation;
import com.readytalk.revori.Column;
import com.readytalk.revori.Parameter;
import com.readytalk.revori.Expression;
import com.readytalk.revori.Revisions;
import com.readytalk.revori.Table;
import com.readytalk.revori.Revision;
import com.readytalk.revori.RevisionBuilder;
import com.readytalk.revori.PatchTemplate;
import com.readytalk.revori.InsertTemplate;
import com.readytalk.revori.UpdateTemplate;
import com.readytalk.revori.TableReference;
import com.readytalk.revori.DuplicateKeyResolution;


public class KellySandbox extends TestCase{
   @Test
   public void testColumnTypes(){
	    Column<Integer> number = new Column<Integer>(Integer.class);
	    Column<String> name = new Column<String>(String.class);
	    Table numbers = new Table(cols(number));

	    Revision tail = Revisions.Empty;

	    PatchTemplate insert = new InsertTemplate
	      (numbers,
	       cols(number, name),
	       list((Expression) new Parameter(),
	            new Parameter()), DuplicateKeyResolution.Throw);

	    try {
	      tail.builder().apply(insert, "1", "one");
	      throw new RuntimeException();
	    } catch (ClassCastException e) { }

	    try {
	      tail.builder().apply(insert, 1, 1);
	      throw new RuntimeException();
	    } catch (ClassCastException e) { }

	    RevisionBuilder builder = tail.builder();

	    builder.apply(insert, 1, "one");

	    Revision first = builder.commit();

	    TableReference numbersReference = new TableReference(numbers);

	    PatchTemplate updateNameWhereNumberEqual = new UpdateTemplate
	      (numbersReference,
	       new BinaryOperation
	       (BinaryOperation.Type.Equal,
	        reference(numbersReference, number),
	        new Parameter()),
	       cols(name),
	       list((Expression) new Parameter()));

	    try {
	      first.builder().apply(updateNameWhereNumberEqual, 1, 2);
	      throw new RuntimeException();
	    } catch (ClassCastException e) { }
   }
   
   @Test (expected=IllegalArgumentException.class)
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
                    list((Expression) new Parameter(),
                            new Parameter(),
                            new Parameter()), DuplicateKeyResolution.Throw);
           fail("Expecting IllegalArgumentException...");
       } catch(IllegalArgumentException expected){}
      
   }
}

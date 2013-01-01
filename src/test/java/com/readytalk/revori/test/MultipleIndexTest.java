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
import com.readytalk.revori.ConflictResolver;
import com.readytalk.revori.DeleteTemplate;
import com.readytalk.revori.DuplicateKeyResolution;
import com.readytalk.revori.Expression;
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


public class MultipleIndexTest {
    @Test
    public void testMultipleIndexInserts(){
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

        Index nameIndex = new Index(numbers, cols(name));

        builder.add(nameIndex);

        builder.apply(insert, 1, "one");
        builder.apply(insert, 2, "two");
        builder.apply(insert, 3, "three");
        builder.apply(insert, 4, "four");
        builder.apply(insert, 5, "five");
        builder.apply(insert, 6, "six");
        builder.apply(insert, 7, "seven");
        builder.apply(insert, 8, "eight");
        builder.apply(insert, 9, "nine");

        Revision first = builder.commit();

        TableReference numbersReference = new TableReference(numbers);
        
        QueryTemplate greaterThanAndLessThan = new QueryTemplate
          (list(reference(numbersReference, name)),
           numbersReference,
           new BinaryOperation
           (BinaryOperation.Type.And,
            new BinaryOperation
            (BinaryOperation.Type.GreaterThan,
             reference(numbersReference, name),
             new Parameter()),
            new BinaryOperation
            (BinaryOperation.Type.LessThan,
             reference(numbersReference, name),
             new Parameter())));
        Object[] parameters = { "four", "two" };

        QueryResult result = tail.diff(first, greaterThanAndLessThan, parameters);

        // We assume here that, by defining a query which is implemented
        // most efficiently in terms of the index on numbers.name, the
        // MyDBMS will actually use that index to execute it, and thus we
        // will visit the results in alphabetical order.

        assertEquals(QueryResult.Type.Inserted, result.nextRow());
        assertEquals("nine", result.nextItem());
        assertEquals(QueryResult.Type.Inserted, result.nextRow());
        assertEquals("one", result.nextItem());
        assertEquals(QueryResult.Type.Inserted, result.nextRow());
        assertEquals("seven", result.nextItem());
        assertEquals(QueryResult.Type.Inserted, result.nextRow());
        assertEquals("six", result.nextItem());
        assertEquals(QueryResult.Type.Inserted, result.nextRow());
        assertEquals("three", result.nextItem());
        assertEquals(QueryResult.Type.End, result.nextRow());

        builder = tail.builder();

        builder.apply(insert, 1, "one");
        builder.apply(insert, 2, "two");
        builder.apply(insert, 3, "three");
        builder.apply(insert, 4, "four");

        builder.add(nameIndex);

        builder.apply(insert, 5, "five");
        builder.apply(insert, 6, "six");
        builder.apply(insert, 7, "seven");
        builder.apply(insert, 8, "eight");
        builder.apply(insert, 9, "nine");

        first = builder.commit();
        Object[] parameters1 = { "four", "two" };

        result = tail.diff(first, greaterThanAndLessThan, parameters1);

        assertEquals(QueryResult.Type.Inserted, result.nextRow());
        assertEquals("nine", result.nextItem());
        assertEquals(QueryResult.Type.Inserted, result.nextRow());
        assertEquals("one", result.nextItem());
        assertEquals(QueryResult.Type.Inserted, result.nextRow());
        assertEquals("seven", result.nextItem());
        assertEquals(QueryResult.Type.Inserted, result.nextRow());
        assertEquals("six", result.nextItem());
        assertEquals(QueryResult.Type.Inserted, result.nextRow());
        assertEquals("three", result.nextItem());
        assertEquals(QueryResult.Type.End, result.nextRow());

        builder = tail.builder();

        builder.apply(insert, 1, "one");
        builder.apply(insert, 2, "two");
        builder.apply(insert, 3, "three");
        builder.apply(insert, 4, "four");
        builder.apply(insert, 5, "five");
        builder.apply(insert, 6, "six");
        builder.apply(insert, 7, "seven");
        builder.apply(insert, 8, "eight");
        builder.apply(insert, 9, "nine");

        builder.add(nameIndex);

        first = builder.commit();
        Object[] parameters2 = { "four", "two" };

        result = tail.diff(first, greaterThanAndLessThan, parameters2);

        assertEquals(QueryResult.Type.Inserted, result.nextRow());
        assertEquals("nine", result.nextItem());
        assertEquals(QueryResult.Type.Inserted, result.nextRow());
        assertEquals("one", result.nextItem());
        assertEquals(QueryResult.Type.Inserted, result.nextRow());
        assertEquals("seven", result.nextItem());
        assertEquals(QueryResult.Type.Inserted, result.nextRow());
        assertEquals("six", result.nextItem());
        assertEquals(QueryResult.Type.Inserted, result.nextRow());
        assertEquals("three", result.nextItem());
        assertEquals(QueryResult.Type.End, result.nextRow());

        builder = first.builder();

        builder.remove(nameIndex);

        Revision second = builder.commit();
        Object[] parameters3 = { "four", "two" };

        result = tail.diff(second, greaterThanAndLessThan, parameters3);

        assertEquals(QueryResult.Type.Inserted, result.nextRow());
        assertEquals("one", result.nextItem());
        assertEquals(QueryResult.Type.Inserted, result.nextRow());
        assertEquals("three", result.nextItem());
        assertEquals(QueryResult.Type.Inserted, result.nextRow());
        assertEquals("six", result.nextItem());
        assertEquals(QueryResult.Type.Inserted, result.nextRow());
        assertEquals("seven", result.nextItem());
        assertEquals(QueryResult.Type.Inserted, result.nextRow());
        assertEquals("nine", result.nextItem());
        assertEquals(QueryResult.Type.End, result.nextRow());
    }
    
    @Test
    public void testMultipleIndexUpdates(){
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
        builder.apply(insert, 4, "four");
        builder.apply(insert, 5, "five");
        builder.apply(insert, 6, "six");
        builder.apply(insert, 7, "seven");
        builder.apply(insert, 8, "eight");
        builder.apply(insert, 9, "nine");

        Index nameIndex = new Index(numbers, cols(name));

        builder.add(nameIndex);

        TableReference numbersReference = new TableReference(numbers);

        PatchTemplate updateNameWhereNumberEqual = new UpdateTemplate
          (numbersReference,
           new BinaryOperation
           (BinaryOperation.Type.Equal,
            reference(numbersReference, number),
            new Parameter()),
           cols(name),
           list((Expression) new Parameter()));

        builder.apply(updateNameWhereNumberEqual, 1, "uno");
        builder.apply(updateNameWhereNumberEqual, 2, "dos");
        builder.apply(updateNameWhereNumberEqual, 3, "tres");
        builder.apply(updateNameWhereNumberEqual, 8, "ocho");

        Revision first = builder.commit();

        QueryTemplate greaterThanAndLessThan = new QueryTemplate
          (list(reference(numbersReference, name)),
           numbersReference,
           new BinaryOperation
           (BinaryOperation.Type.And,
            new BinaryOperation
            (BinaryOperation.Type.GreaterThan,
             reference(numbersReference, name),
             new Parameter()),
            new BinaryOperation
            (BinaryOperation.Type.LessThan,
             reference(numbersReference, name),
             new Parameter())));
        Object[] parameters = { "four", "two" };

        QueryResult result = tail.diff(first, greaterThanAndLessThan, parameters);

        // System.out.println("first: " + first);

        assertEquals(QueryResult.Type.Inserted, result.nextRow());
        assertEquals("nine", result.nextItem());
        assertEquals(QueryResult.Type.Inserted, result.nextRow());
        assertEquals("ocho", result.nextItem());
        assertEquals(QueryResult.Type.Inserted, result.nextRow());
        assertEquals("seven", result.nextItem());
        assertEquals(QueryResult.Type.Inserted, result.nextRow());
        assertEquals("six", result.nextItem());
        assertEquals(QueryResult.Type.Inserted, result.nextRow());
        assertEquals("tres", result.nextItem());
        assertEquals(QueryResult.Type.End, result.nextRow());

        builder = first.builder();

        builder.remove(nameIndex);

        Revision second = builder.commit();
        Object[] parameters1 = { "four", "two" };

        result = tail.diff(second, greaterThanAndLessThan, parameters1);

        assertEquals(QueryResult.Type.Inserted, result.nextRow());
        assertEquals("tres", result.nextItem());
        assertEquals(QueryResult.Type.Inserted, result.nextRow());
        assertEquals("six", result.nextItem());
        assertEquals(QueryResult.Type.Inserted, result.nextRow());
        assertEquals("seven", result.nextItem());
        assertEquals(QueryResult.Type.Inserted, result.nextRow());
        assertEquals("ocho", result.nextItem());
        assertEquals(QueryResult.Type.Inserted, result.nextRow());
        assertEquals("nine", result.nextItem());
        assertEquals(QueryResult.Type.End, result.nextRow());
    }
    
    @Test
    public void testMultipleIndexDeletes(){
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
        builder.apply(insert, 4, "four");
        builder.apply(insert, 5, "five");
        builder.apply(insert, 6, "six");
        builder.apply(insert, 7, "seven");
        builder.apply(insert, 8, "eight");
        builder.apply(insert, 9, "nine");

        Index nameIndex = new Index(numbers, cols(name));

        builder.add(nameIndex);

        TableReference numbersReference = new TableReference(numbers);

        PatchTemplate deleteWhereNumberEqual = new DeleteTemplate
          (numbersReference,
           new BinaryOperation
           (BinaryOperation.Type.Equal,
            reference(numbersReference, number),
            new Parameter()));

        builder.apply(deleteWhereNumberEqual, 6);

        PatchTemplate deleteWhereNameEqual = new DeleteTemplate
          (numbersReference,
           new BinaryOperation
           (BinaryOperation.Type.Equal,
            reference(numbersReference, name),
            new Parameter()));

        builder.apply(deleteWhereNameEqual, "four");

        Revision first = builder.commit();

        QueryTemplate greaterThanAndLessThanName = new QueryTemplate
          (list(reference(numbersReference, name)),
           numbersReference,
           new BinaryOperation
           (BinaryOperation.Type.And,
            new BinaryOperation
            (BinaryOperation.Type.GreaterThan,
             reference(numbersReference, name),
             new Parameter()),
            new BinaryOperation
            (BinaryOperation.Type.LessThan,
             reference(numbersReference, name),
             new Parameter())));
        Object[] parameters = { "f", "t" };

        QueryResult result = tail.diff(first, greaterThanAndLessThanName, parameters);

        assertEquals(QueryResult.Type.Inserted, result.nextRow());
        assertEquals("five", result.nextItem());
        assertEquals(QueryResult.Type.Inserted, result.nextRow());
        assertEquals("nine", result.nextItem());
        assertEquals(QueryResult.Type.Inserted, result.nextRow());
        assertEquals("one", result.nextItem());
        assertEquals(QueryResult.Type.Inserted, result.nextRow());
        assertEquals("seven", result.nextItem());
        assertEquals(QueryResult.Type.End, result.nextRow());

        QueryTemplate greaterThanAndLessThanNumber = new QueryTemplate
          (list(reference(numbersReference, name)),
           numbersReference,
           new BinaryOperation
           (BinaryOperation.Type.And,
            new BinaryOperation
            (BinaryOperation.Type.GreaterThan,
             reference(numbersReference, number),
             new Parameter()),
            new BinaryOperation
            (BinaryOperation.Type.LessThan,
             reference(numbersReference, number),
             new Parameter())));
        Object[] parameters1 = { 2, 8 };

        result = tail.diff(first, greaterThanAndLessThanNumber, parameters1);

        assertEquals(QueryResult.Type.Inserted, result.nextRow());
        assertEquals("three", result.nextItem());
        assertEquals(QueryResult.Type.Inserted, result.nextRow());
        assertEquals("five", result.nextItem());
        assertEquals(QueryResult.Type.Inserted, result.nextRow());
        assertEquals("seven", result.nextItem());
        assertEquals(QueryResult.Type.End, result.nextRow());

        builder = first.builder();

        builder.remove(nameIndex);

        Revision second = builder.commit();
        Object[] parameters2 = { "f", "t" };

        result = tail.diff(second, greaterThanAndLessThanName, parameters2);

        assertEquals(QueryResult.Type.Inserted, result.nextRow());
        assertEquals("one", result.nextItem());
        assertEquals(QueryResult.Type.Inserted, result.nextRow());
        assertEquals("five", result.nextItem());
        assertEquals(QueryResult.Type.Inserted, result.nextRow());
        assertEquals("seven", result.nextItem());
        assertEquals(QueryResult.Type.Inserted, result.nextRow());
        assertEquals("nine", result.nextItem());
        assertEquals(QueryResult.Type.End, result.nextRow());
    }
    
    @Test
    public void testMultipleIndexMerges(){
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

        Index nameIndex = new Index(numbers, cols(name));

        builder.add(nameIndex);

        builder.apply(insert, 1, "one");
        builder.apply(insert, 2, "two");
        builder.apply(insert, 3, "three");
        builder.apply(insert, 4, "four");
        builder.apply(insert, 5, "five");

        Revision left = builder.commit();

        builder = tail.builder();

        builder.apply(insert, 4, "four");
        builder.apply(insert, 5, "five");
        builder.apply(insert, 6, "six");
        builder.apply(insert, 7, "seven");
        builder.apply(insert, 8, "eight");
        builder.apply(insert, 9, "nine");

        Revision right = builder.commit();

        Revision merge = tail.merge(left, right, new ConflictResolver() {
          public Object resolveConflict(Table table,
                                        Column column,
                                        Object[] primaryKeyValues,
                                        Object baseValue,
                                        Object leftValue,
                                        Object rightValue)
          {
            throw new RuntimeException();
          }
        }, null);

        TableReference numbersReference = new TableReference(numbers);

        QueryTemplate greaterThanAndLessThan = new QueryTemplate
          (list(reference(numbersReference, name)),
           numbersReference,
           new BinaryOperation
           (BinaryOperation.Type.And,
            new BinaryOperation
            (BinaryOperation.Type.GreaterThan,
             reference(numbersReference, name),
             new Parameter()),
            new BinaryOperation
            (BinaryOperation.Type.LessThan,
             reference(numbersReference, name),
             new Parameter())));
        Object[] parameters = { "four", "two" };

        QueryResult result = tail.diff(merge, greaterThanAndLessThan, parameters);

        assertEquals(QueryResult.Type.Inserted, result.nextRow());
        assertEquals("nine", result.nextItem());
        assertEquals(QueryResult.Type.Inserted, result.nextRow());
        assertEquals("one", result.nextItem());
        assertEquals(QueryResult.Type.Inserted, result.nextRow());
        assertEquals("seven", result.nextItem());
        assertEquals(QueryResult.Type.Inserted, result.nextRow());
        assertEquals("six", result.nextItem());
        assertEquals(QueryResult.Type.Inserted, result.nextRow());
        assertEquals("three", result.nextItem());
        assertEquals(QueryResult.Type.End, result.nextRow());

        builder = merge.builder();

        builder.remove(nameIndex);

        Revision second = builder.commit();
        Object[] parameters1 = { "four", "two" };

        result = tail.diff(second, greaterThanAndLessThan, parameters1);

        assertEquals(QueryResult.Type.Inserted, result.nextRow());
        assertEquals("one", result.nextItem());
        assertEquals(QueryResult.Type.Inserted, result.nextRow());
        assertEquals("three", result.nextItem());
        assertEquals(QueryResult.Type.Inserted, result.nextRow());
        assertEquals("six", result.nextItem());
        assertEquals(QueryResult.Type.Inserted, result.nextRow());
        assertEquals("seven", result.nextItem());
        assertEquals(QueryResult.Type.Inserted, result.nextRow());
        assertEquals("nine", result.nextItem());
        assertEquals(QueryResult.Type.End, result.nextRow());

        builder = merge.builder();

        PatchTemplate updateNameWhereNumberEqual = new UpdateTemplate
          (numbersReference,
           new BinaryOperation
           (BinaryOperation.Type.Equal,
            reference(numbersReference, number),
            new Parameter()),
           cols(name),
           list((Expression) new Parameter()));

        builder.apply(updateNameWhereNumberEqual, 1, "uno");
        builder.apply(updateNameWhereNumberEqual, 3, "tres");

        left = builder.commit();

        builder = merge.builder();

        PatchTemplate deleteWhereNumberEqual = new DeleteTemplate
          (numbersReference,
           new BinaryOperation
           (BinaryOperation.Type.Equal,
            reference(numbersReference, number),
            new Parameter()));

        builder.apply(deleteWhereNumberEqual, 1);
        builder.apply(deleteWhereNumberEqual, 6);

        right = builder.commit();

        merge = merge.merge(left, right, new ConflictResolver() {
          public Object resolveConflict(Table table,
                                        Column column,
                                        Object[] primaryKeyValues,
                                        Object baseValue,
                                        Object leftValue,
                                        Object rightValue)
          {
            throw new RuntimeException();
          }
        }, null);
        Object[] parameters2 = { "four", "two" };

        result = tail.diff(merge, greaterThanAndLessThan, parameters2);

        assertEquals(QueryResult.Type.Inserted, result.nextRow());
        assertEquals("nine", result.nextItem());
        assertEquals(QueryResult.Type.Inserted, result.nextRow());
        assertEquals("seven", result.nextItem());
        assertEquals(QueryResult.Type.Inserted, result.nextRow());
        assertEquals("tres", result.nextItem());
        assertEquals(QueryResult.Type.End, result.nextRow());

        builder = merge.builder();

        builder.remove(nameIndex);

        Revision third = builder.commit();
        Object[] parameters3 = { "four", "two" };

        result = tail.diff(third, greaterThanAndLessThan, parameters3);

        assertEquals(QueryResult.Type.Inserted, result.nextRow());
        assertEquals("tres", result.nextItem());
        assertEquals(QueryResult.Type.Inserted, result.nextRow());
        assertEquals("seven", result.nextItem());
        assertEquals(QueryResult.Type.Inserted, result.nextRow());
        assertEquals("nine", result.nextItem());
        assertEquals(QueryResult.Type.End, result.nextRow());
    }
}

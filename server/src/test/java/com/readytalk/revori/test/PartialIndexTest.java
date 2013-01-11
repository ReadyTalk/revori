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

import org.junit.Test;

import com.readytalk.revori.BinaryOperation;
import com.readytalk.revori.Column;
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

public class PartialIndexTest {
    @Test
    public void testUpdateOnPartialIndex(){
        Column<Integer> number = new Column<Integer>(Integer.class);
        Column<String> color = new Column<String>(String.class);
        Column<String> shape = new Column<String>(String.class);
        Table numbers = new Table(cols(number, color));

        Revision tail = Revisions.Empty;

        PatchTemplate insert = new InsertTemplate
          (numbers,
           cols(number, color, shape),
           Lists.newArrayList((Expression) new Parameter(),
                new Parameter(),
                new Parameter()), DuplicateKeyResolution.Throw);

        RevisionBuilder builder = tail.builder();

        builder.apply(insert, 1, "red", "triangle");
        builder.apply(insert, 1, "green", "circle");
        builder.apply(insert, 2, "yellow", "circle");
        builder.apply(insert, 3, "blue", "square");
        builder.apply(insert, 3, "orange", "square");

        Revision first = builder.commit();

        TableReference numbersReference = new TableReference(numbers);

        QueryTemplate numberEqual = new QueryTemplate
          (Lists.newArrayList(reference(numbersReference, color),
                reference(numbersReference, shape)),
           numbersReference,
           new BinaryOperation
           (BinaryOperation.Type.Equal,
            reference(numbersReference, number),
            new Parameter()));
        Object[] parameters = { 1 };

        QueryResult result = tail.diff(first, numberEqual, parameters);

        assertEquals(QueryResult.Type.Inserted, result.nextRow());
        assertEquals("green", result.nextItem());
        assertEquals("circle", result.nextItem());
        assertEquals(QueryResult.Type.Inserted, result.nextRow());
        assertEquals("red", result.nextItem());
        assertEquals("triangle", result.nextItem());
        assertEquals(QueryResult.Type.End, result.nextRow());

        PatchTemplate updateShapeWhereNumberEqual = new UpdateTemplate
          (numbersReference,
           new BinaryOperation
           (BinaryOperation.Type.Equal,
            reference(numbersReference, number),
            new Parameter()),
           cols(shape),
           Lists.newArrayList((Expression) new Parameter()));

        builder = first.builder();

        builder.apply(updateShapeWhereNumberEqual, 1, "pentagon");

        Revision second = builder.commit();
        Object[] parameters1 = { 1 };

        result = tail.diff(second, numberEqual, parameters1);

        assertEquals(QueryResult.Type.Inserted, result.nextRow());
        assertEquals("green", result.nextItem());
        assertEquals("pentagon", result.nextItem());
        assertEquals(QueryResult.Type.Inserted, result.nextRow());
        assertEquals("red", result.nextItem());
        assertEquals("pentagon", result.nextItem());
        assertEquals(QueryResult.Type.End, result.nextRow());
    }

    @Test
    public void testDeleteOnPartialIndex(){
        Column<Integer> number = new Column<Integer>(Integer.class);
        Column<String> color = new Column<String>(String.class);
        Column<String> shape = new Column<String>(String.class);
        Table numbers = new Table(cols(number, color));

        Revision tail = Revisions.Empty;

        PatchTemplate insert = new InsertTemplate
          (numbers,
           cols(number, color, shape),
           Lists.newArrayList((Expression) new Parameter(),
                new Parameter(),
                new Parameter()), DuplicateKeyResolution.Throw);

        RevisionBuilder builder = tail.builder();

        builder.apply(insert, 1, "red", "triangle");
        builder.apply(insert, 1, "green", "circle");
        builder.apply(insert, 2, "yellow", "circle");
        builder.apply(insert, 3, "blue", "square");
        builder.apply(insert, 3, "orange", "square");

        Revision first = builder.commit();

        TableReference numbersReference = new TableReference(numbers);

        QueryTemplate numberEqual = new QueryTemplate
          (Lists.newArrayList(reference(numbersReference, color),
                reference(numbersReference, shape)),
           numbersReference,
           new BinaryOperation
           (BinaryOperation.Type.Equal,
            reference(numbersReference, number),
            new Parameter()));
        Object[] parameters = { 1 };

        QueryResult result = tail.diff(first, numberEqual, parameters);

        assertEquals(QueryResult.Type.Inserted, result.nextRow());
        assertEquals("green", result.nextItem());
        assertEquals("circle", result.nextItem());
        assertEquals(QueryResult.Type.Inserted, result.nextRow());
        assertEquals("red", result.nextItem());
        assertEquals("triangle", result.nextItem());
        assertEquals(QueryResult.Type.End, result.nextRow());

        PatchTemplate deleteWhereNumberEqual = new DeleteTemplate
          (numbersReference,
           new BinaryOperation
           (BinaryOperation.Type.Equal,
            reference(numbersReference, number),
            new Parameter()));

        builder = first.builder();

        builder.apply(deleteWhereNumberEqual, 1);

        Revision second = builder.commit();
        Object[] parameters1 = { 1 };

        result = tail.diff(second, numberEqual, parameters1);

        assertEquals(QueryResult.Type.End, result.nextRow());
    }
}

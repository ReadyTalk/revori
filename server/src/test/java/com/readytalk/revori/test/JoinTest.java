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
import static org.junit.Assert.assertNull;

import org.junit.Test;

import com.readytalk.revori.BinaryOperation;
import com.readytalk.revori.Column;
import com.readytalk.revori.DuplicateKeyResolution;
import com.readytalk.revori.Expression;
import com.readytalk.revori.InsertTemplate;
import com.readytalk.revori.Join;
import com.readytalk.revori.Parameter;
import com.readytalk.revori.PatchTemplate;
import com.readytalk.revori.QueryResult;
import com.readytalk.revori.QueryTemplate;
import com.readytalk.revori.Revision;
import com.readytalk.revori.RevisionBuilder;
import com.readytalk.revori.Revisions;
import com.readytalk.revori.Table;
import com.readytalk.revori.TableReference;

public class JoinTest {
    
    @Test
    public void testSimpleJoins(){
    	
        Column<Integer> id = new Column<Integer>(Integer.class);
        Column<String> name = new Column<String>(String.class);
        Table names = new Table(cols(id));

        Column<String> nickname = new Column<String>(String.class);
        Table nicknames = new Table(cols(id, nickname));

        Revision tail = Revisions.Empty;

        PatchTemplate nameInsert = new InsertTemplate
          (names,
           cols(id, name),
           list((Expression) new Parameter(), new Parameter()),
           DuplicateKeyResolution.Throw);

        PatchTemplate nicknameInsert = new InsertTemplate
          (nicknames,
           cols(id, nickname),
           list((Expression) new Parameter(), new Parameter()),
           DuplicateKeyResolution.Throw);

        RevisionBuilder builder = tail.builder();

        builder.apply(nameInsert, 1, "tom");
        builder.apply(nameInsert, 2, "ted");
        builder.apply(nameInsert, 3, "tim");
        builder.apply(nameInsert, 4, "tod");
        builder.apply(nameInsert, 5, "tes");

        builder.apply(nicknameInsert, 1, "moneybags");
        builder.apply(nicknameInsert, 3, "eight ball");
        builder.apply(nicknameInsert, 4, "baldy");
        builder.apply(nicknameInsert, 5, "knuckles");
        builder.apply(nicknameInsert, 6, "no name");

        Revision first = builder.commit();
       
        TableReference namesReference = new TableReference(names);
        TableReference nicknamesReference = new TableReference(nicknames);

        QueryTemplate namesInnerNicknames = new QueryTemplate
          (list(reference(namesReference, name),
                reference(nicknamesReference, nickname)),
           new Join
           (Join.Type.Inner,
            namesReference,
            nicknamesReference),
           new BinaryOperation
           (BinaryOperation.Type.Equal,
            reference(namesReference, id),
            reference(nicknamesReference, id)));
        Object[] parameters = {};
        
        QueryResult result = tail.diff(first, namesInnerNicknames, parameters);

        assertEquals(QueryResult.Type.Inserted, result.nextRow());
        assertEquals("tom", result.nextItem());
        assertEquals("moneybags", result.nextItem());
        assertEquals(QueryResult.Type.Inserted, result.nextRow());
        assertEquals("tim", result.nextItem());
        assertEquals("eight ball", result.nextItem());
        assertEquals(QueryResult.Type.Inserted, result.nextRow());
        assertEquals("tod", result.nextItem());
        assertEquals("baldy", result.nextItem());
        assertEquals(QueryResult.Type.Inserted, result.nextRow());
        assertEquals("tes", result.nextItem());
        assertEquals("knuckles", result.nextItem());
        assertEquals(QueryResult.Type.End, result.nextRow());

        QueryTemplate namesLeftNicknames = new QueryTemplate
          (list(reference(namesReference, name),
                reference(nicknamesReference, nickname)),
           new Join
           (Join.Type.LeftOuter,
            namesReference,
            nicknamesReference),
           new BinaryOperation
           (BinaryOperation.Type.Equal,
            reference(namesReference, id),
            reference(nicknamesReference, id)));
        Object[] parameters1 = {};
        
        result = tail.diff(first, namesLeftNicknames, parameters1);

        assertEquals(QueryResult.Type.Inserted, result.nextRow());
        assertEquals("tom", result.nextItem());
        assertEquals("moneybags", result.nextItem());
        assertEquals(QueryResult.Type.Inserted, result.nextRow());
        assertEquals("ted", result.nextItem());
        assertNull(result.nextItem());
        assertEquals(QueryResult.Type.Inserted, result.nextRow());
        assertEquals("tim", result.nextItem());
        assertEquals("eight ball", result.nextItem());
        assertEquals(QueryResult.Type.Inserted, result.nextRow());
        assertEquals("tod", result.nextItem());
        assertEquals("baldy", result.nextItem());
        assertEquals(QueryResult.Type.Inserted, result.nextRow());
        assertEquals("tes", result.nextItem());
        assertEquals("knuckles", result.nextItem());
        assertEquals(QueryResult.Type.End, result.nextRow());

        builder = first.builder();

        builder.apply(nameInsert, 6, "rapunzel");
        builder.apply(nameInsert, 7, "carlos");
        builder.apply(nameInsert, 8, "benjamin");

        builder.apply(nicknameInsert, 1, "big bucks");
        builder.apply(nicknameInsert, 8, "jellybean");

        Revision second = builder.commit();
        Object[] parameters2 = {};

        result = first.diff(second, namesLeftNicknames, parameters2);

        assertEquals(QueryResult.Type.Inserted, result.nextRow());
        assertEquals("tom", result.nextItem());
        assertEquals("big bucks", result.nextItem());
        assertEquals(QueryResult.Type.Inserted, result.nextRow());
        assertEquals("rapunzel", result.nextItem());
        assertEquals("no name", result.nextItem());
        assertEquals(QueryResult.Type.Inserted, result.nextRow());
        assertEquals("carlos", result.nextItem());
        assertNull(result.nextItem());
        assertEquals(QueryResult.Type.Inserted, result.nextRow());
        assertEquals("benjamin", result.nextItem());
        assertEquals("jellybean", result.nextItem());
        assertEquals(QueryResult.Type.End, result.nextRow());
        Object[] parameters3 = {};

        result = tail.diff(second, namesLeftNicknames, parameters3);

        assertEquals(QueryResult.Type.Inserted, result.nextRow());
        assertEquals("tom", result.nextItem());
        assertEquals("big bucks", result.nextItem());
        assertEquals(QueryResult.Type.Inserted, result.nextRow());
        assertEquals("tom", result.nextItem());
        assertEquals("moneybags", result.nextItem());
        assertEquals(QueryResult.Type.Inserted, result.nextRow());
        assertEquals("ted", result.nextItem());
        assertNull(result.nextItem());
        assertEquals(QueryResult.Type.Inserted, result.nextRow());
        assertEquals("tim", result.nextItem());
        assertEquals("eight ball", result.nextItem());
        assertEquals(QueryResult.Type.Inserted, result.nextRow());
        assertEquals("tod", result.nextItem());
        assertEquals("baldy", result.nextItem());
        assertEquals(QueryResult.Type.Inserted, result.nextRow());
        assertEquals("tes", result.nextItem());
        assertEquals("knuckles", result.nextItem());
        assertEquals(QueryResult.Type.Inserted, result.nextRow());
        assertEquals("rapunzel", result.nextItem());
        assertEquals("no name", result.nextItem());
        assertEquals(QueryResult.Type.Inserted, result.nextRow());
        assertEquals("carlos", result.nextItem());
        assertNull(result.nextItem());
        assertEquals(QueryResult.Type.Inserted, result.nextRow());
        assertEquals("benjamin", result.nextItem());
        assertEquals("jellybean", result.nextItem());
        assertEquals(QueryResult.Type.End, result.nextRow());
    	
    }
    
    @Test
    public void testCompoundJoins(){
    	
        Column<Integer> id = new Column<Integer>(Integer.class);
        Column<String> name = new Column<String>(String.class);
        Table names = new Table(cols(id));

        Column<String> nickname = new Column<String>(String.class);
        Table nicknames = new Table(cols(id, nickname));

        Column<String> lastname = new Column<String>(String.class);
        Table lastnames = new Table(cols(name));

        Column<String> string = new Column<String>(String.class);
        Column<String> color = new Column<String>(String.class);
        Table colors = new Table(cols(string));

        Revision tail = Revisions.Empty;

        PatchTemplate nameInsert = new InsertTemplate
          (names,
           cols(id, name),
           list((Expression) new Parameter(), new Parameter()),
           DuplicateKeyResolution.Throw);

        PatchTemplate nicknameInsert = new InsertTemplate
          (nicknames,
           cols(id, nickname),
           list((Expression) new Parameter(), new Parameter()),
           DuplicateKeyResolution.Throw);

        PatchTemplate lastnameInsert = new InsertTemplate
          (lastnames,
           cols(name, lastname),
           list((Expression) new Parameter(), new Parameter()),
           DuplicateKeyResolution.Throw);

        PatchTemplate colorInsert = new InsertTemplate
          (colors,
           cols(string, color),
           list((Expression) new Parameter(), new Parameter()),
           DuplicateKeyResolution.Throw);

        RevisionBuilder builder = tail.builder();

        builder.apply(nameInsert, 1, "tom");
        builder.apply(nameInsert, 2, "ted");
        builder.apply(nameInsert, 3, "tim");
        builder.apply(nameInsert, 4, "tod");
        builder.apply(nameInsert, 5, "tes");

        builder.apply(nicknameInsert, 1, "moneybags");
        builder.apply(nicknameInsert, 1, "big bucks");
        builder.apply(nicknameInsert, 3, "eight ball");
        builder.apply(nicknameInsert, 4, "baldy");
        builder.apply(nicknameInsert, 5, "knuckles");
        builder.apply(nicknameInsert, 6, "no name");

        builder.apply(lastnameInsert, "tom", "thumb");
        builder.apply(lastnameInsert, "ted", "thomson");
        builder.apply(lastnameInsert, "tes", "teasdale");

        builder.apply(colorInsert, "big bucks", "red");
        builder.apply(colorInsert, "baldy", "green");
        builder.apply(colorInsert, "no name", "pink");
        builder.apply(colorInsert, "eight ball", "sky blue");

        Revision first = builder.commit();
       
        TableReference namesReference = new TableReference(names);
        TableReference nicknamesReference = new TableReference(nicknames);
        TableReference lastnamesReference = new TableReference(lastnames);
        TableReference colorsReference = new TableReference(colors);

        QueryTemplate namesInnerNicknamesInnerColors = new QueryTemplate
          (list(reference(namesReference, name),
                reference(nicknamesReference, nickname),
                reference(colorsReference, color)),
           new Join
           (Join.Type.Inner,
            new Join
            (Join.Type.Inner,
             namesReference,
             nicknamesReference),
            colorsReference),
           new BinaryOperation
           (BinaryOperation.Type.And,
            new BinaryOperation
            (BinaryOperation.Type.Equal,
             reference(namesReference, id),
             reference(nicknamesReference, id)),
            new BinaryOperation
            (BinaryOperation.Type.Equal,
             reference(colorsReference, string),
             reference(nicknamesReference, nickname))));
        Object[] parameters = {};
        
        QueryResult result = tail.diff(first, namesInnerNicknamesInnerColors, parameters);

        assertEquals(QueryResult.Type.Inserted, result.nextRow());
        assertEquals("tom", result.nextItem());
        assertEquals("big bucks", result.nextItem());
        assertEquals("red", result.nextItem());
        assertEquals(QueryResult.Type.Inserted, result.nextRow());
        assertEquals("tim", result.nextItem());
        assertEquals("eight ball", result.nextItem());
        assertEquals("sky blue", result.nextItem());
        assertEquals(QueryResult.Type.Inserted, result.nextRow());
        assertEquals("tod", result.nextItem());
        assertEquals("baldy", result.nextItem());
        assertEquals("green", result.nextItem());
        assertEquals(QueryResult.Type.End, result.nextRow());

        QueryTemplate namesLeftNicknamesInnerColors = new QueryTemplate
          (list(reference(namesReference, name),
                reference(nicknamesReference, nickname),
                reference(colorsReference, color)),
           new Join
           (Join.Type.Inner,
            new Join
            (Join.Type.LeftOuter,
             namesReference,
             nicknamesReference),
            colorsReference),
           new BinaryOperation
           (BinaryOperation.Type.And,
            new BinaryOperation
            (BinaryOperation.Type.Equal,
             reference(namesReference, id),
             reference(nicknamesReference, id)),
            new BinaryOperation
            (BinaryOperation.Type.Equal,
             reference(colorsReference, string),
             reference(nicknamesReference, nickname))));
        Object[] parameters1 = {};
        
        result = tail.diff(first, namesLeftNicknamesInnerColors, parameters1);

        assertEquals(QueryResult.Type.Inserted, result.nextRow());
        assertEquals("tom", result.nextItem());
        assertEquals("big bucks", result.nextItem());
        assertEquals("red", result.nextItem());
        assertEquals(QueryResult.Type.Inserted, result.nextRow());
        assertEquals("tim", result.nextItem());
        assertEquals("eight ball", result.nextItem());
        assertEquals("sky blue", result.nextItem());
        assertEquals(QueryResult.Type.Inserted, result.nextRow());
        assertEquals("tod", result.nextItem());
        assertEquals("baldy", result.nextItem());
        assertEquals("green", result.nextItem());
        assertEquals(QueryResult.Type.End, result.nextRow());

        QueryTemplate namesInnerNicknamesLeftColors = new QueryTemplate
          (list(reference(namesReference, name),
                reference(nicknamesReference, nickname),
                reference(colorsReference, color)),
           new Join
           (Join.Type.LeftOuter,
            new Join
            (Join.Type.Inner,
             namesReference,
             nicknamesReference),
            colorsReference),
           new BinaryOperation
           (BinaryOperation.Type.And,
            new BinaryOperation
            (BinaryOperation.Type.Equal,
             reference(namesReference, id),
             reference(nicknamesReference, id)),
            new BinaryOperation
            (BinaryOperation.Type.Equal,
             reference(colorsReference, string),
             reference(nicknamesReference, nickname))));
        Object[] parameters2 = {};
        
        result = tail.diff(first, namesInnerNicknamesLeftColors, parameters2);

        assertEquals(QueryResult.Type.Inserted, result.nextRow());
        assertEquals("tom", result.nextItem());
        assertEquals("big bucks", result.nextItem());
        assertEquals("red", result.nextItem());
        assertEquals(QueryResult.Type.Inserted, result.nextRow());
        assertEquals("tom", result.nextItem());
        assertEquals("moneybags", result.nextItem());
        assertNull(result.nextItem());
        assertEquals(QueryResult.Type.Inserted, result.nextRow());
        assertEquals("tim", result.nextItem());
        assertEquals("eight ball", result.nextItem());
        assertEquals("sky blue", result.nextItem());
        assertEquals(QueryResult.Type.Inserted, result.nextRow());
        assertEquals("tod", result.nextItem());
        assertEquals("baldy", result.nextItem());
        assertEquals("green", result.nextItem());
        assertEquals(QueryResult.Type.Inserted, result.nextRow());
        assertEquals("tes", result.nextItem());
        assertEquals("knuckles", result.nextItem());
        assertNull(result.nextItem());
        assertEquals(QueryResult.Type.End, result.nextRow());

        QueryTemplate namesInnerLastnamesLeftNicknamesLeftColors
          = new QueryTemplate
          (list(reference(namesReference, name),
                reference(lastnamesReference, lastname),
                reference(nicknamesReference, nickname),
                reference(colorsReference, color)),
           new Join
           (Join.Type.LeftOuter,
            new Join
            (Join.Type.Inner,
             namesReference,
             lastnamesReference),
            new Join
            (Join.Type.LeftOuter,
             nicknamesReference,
             colorsReference)),
           new BinaryOperation
           (BinaryOperation.Type.And,
            new BinaryOperation
            (BinaryOperation.Type.Equal,
             reference(namesReference, name),
             reference(lastnamesReference, name)),
            new BinaryOperation
            (BinaryOperation.Type.And,
             new BinaryOperation
             (BinaryOperation.Type.Equal,
              reference(namesReference, id),
              reference(nicknamesReference, id)),
             new BinaryOperation
             (BinaryOperation.Type.Equal,
              reference(colorsReference, string),
              reference(nicknamesReference, nickname)))));
        Object[] parameters3 = {};
        
        result = tail.diff(first, namesInnerLastnamesLeftNicknamesLeftColors, parameters3);

        assertEquals(QueryResult.Type.Inserted, result.nextRow());
        assertEquals("tom", result.nextItem());
        assertEquals("thumb", result.nextItem());
        assertEquals("big bucks", result.nextItem());
        assertEquals("red", result.nextItem());
        assertEquals(QueryResult.Type.Inserted, result.nextRow());
        assertEquals("tom", result.nextItem());
        assertEquals("thumb", result.nextItem());
        assertEquals("moneybags", result.nextItem());
        assertNull(result.nextItem());
        assertEquals(QueryResult.Type.Inserted, result.nextRow());
        assertEquals("ted", result.nextItem());
        assertEquals("thomson", result.nextItem());
        assertNull(result.nextItem());
        assertNull(result.nextItem());
        assertEquals(QueryResult.Type.Inserted, result.nextRow());
        assertEquals("tes", result.nextItem());
        assertEquals("teasdale", result.nextItem());
        assertEquals("knuckles", result.nextItem());
        assertNull(result.nextItem());
        assertEquals(QueryResult.Type.End, result.nextRow());	
    }
}

package unittests;

import junit.framework.TestCase;

import org.junit.Test;

import static com.readytalk.oss.dbms.util.Util.list;
import static org.junit.Assert.*;

import com.readytalk.oss.dbms.BinaryOperation;
import com.readytalk.oss.dbms.Column;
import com.readytalk.oss.dbms.Join;
import com.readytalk.oss.dbms.Revisions;
import com.readytalk.oss.dbms.Table;
import com.readytalk.oss.dbms.Expression;
import com.readytalk.oss.dbms.Revision;
import com.readytalk.oss.dbms.RevisionBuilder;
import com.readytalk.oss.dbms.PatchTemplate;
import com.readytalk.oss.dbms.InsertTemplate;
import com.readytalk.oss.dbms.TableReference;
import com.readytalk.oss.dbms.QueryTemplate;
import com.readytalk.oss.dbms.QueryResult;
import com.readytalk.oss.dbms.Parameter;
import com.readytalk.oss.dbms.ColumnReference;
import com.readytalk.oss.dbms.DuplicateKeyResolution;

public class JoinTest extends TestCase{
    
    @Test
    public void testSimpleJoins(){
    	
        Column id = new Column(Integer.class);
        Column name = new Column(String.class);
        Table names = new Table(list(id));

        Column nickname = new Column(String.class);
        Table nicknames = new Table(list(id, nickname));

        Revision tail = Revisions.Empty;

        PatchTemplate nameInsert = new InsertTemplate
          (names,
           list(id, name),
           list((Expression) new Parameter(), new Parameter()),
           DuplicateKeyResolution.Throw);

        PatchTemplate nicknameInsert = new InsertTemplate
          (nicknames,
           list(id, nickname),
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
          (list((Expression) new ColumnReference(namesReference, name),
                (Expression) new ColumnReference(nicknamesReference, nickname)),
           new Join
           (Join.Type.Inner,
            namesReference,
            nicknamesReference),
           new BinaryOperation
           (BinaryOperation.Type.Equal,
            new ColumnReference(namesReference, id),
            new ColumnReference(nicknamesReference, id)));
        Object[] parameters = {};
        
        QueryResult result = tail.diff(first, namesInnerNicknames, parameters);

        assertEquals(result.nextRow(), QueryResult.Type.Inserted);
        assertEquals(result.nextItem(), "tom");
        assertEquals(result.nextItem(), "moneybags");
        assertEquals(result.nextRow(), QueryResult.Type.Inserted);
        assertEquals(result.nextItem(), "tim");
        assertEquals(result.nextItem(), "eight ball");
        assertEquals(result.nextRow(), QueryResult.Type.Inserted);
        assertEquals(result.nextItem(), "tod");
        assertEquals(result.nextItem(), "baldy");
        assertEquals(result.nextRow(), QueryResult.Type.Inserted);
        assertEquals(result.nextItem(), "tes");
        assertEquals(result.nextItem(), "knuckles");
        assertEquals(result.nextRow(), QueryResult.Type.End);

        QueryTemplate namesLeftNicknames = new QueryTemplate
          (list((Expression) new ColumnReference(namesReference, name),
                (Expression) new ColumnReference(nicknamesReference, nickname)),
           new Join
           (Join.Type.LeftOuter,
            namesReference,
            nicknamesReference),
           new BinaryOperation
           (BinaryOperation.Type.Equal,
            new ColumnReference(namesReference, id),
            new ColumnReference(nicknamesReference, id)));
        Object[] parameters1 = {};
        
        result = tail.diff(first, namesLeftNicknames, parameters1);

        assertEquals(result.nextRow(), QueryResult.Type.Inserted);
        assertEquals(result.nextItem(), "tom");
        assertEquals(result.nextItem(), "moneybags");
        assertEquals(result.nextRow(), QueryResult.Type.Inserted);
        assertEquals(result.nextItem(), "ted");
        assertEquals(result.nextItem(), null);
        assertEquals(result.nextRow(), QueryResult.Type.Inserted);
        assertEquals(result.nextItem(), "tim");
        assertEquals(result.nextItem(), "eight ball");
        assertEquals(result.nextRow(), QueryResult.Type.Inserted);
        assertEquals(result.nextItem(), "tod");
        assertEquals(result.nextItem(), "baldy");
        assertEquals(result.nextRow(), QueryResult.Type.Inserted);
        assertEquals(result.nextItem(), "tes");
        assertEquals(result.nextItem(), "knuckles");
        assertEquals(result.nextRow(), QueryResult.Type.End);

        builder = first.builder();

        builder.apply(nameInsert, 6, "rapunzel");
        builder.apply(nameInsert, 7, "carlos");
        builder.apply(nameInsert, 8, "benjamin");

        builder.apply(nicknameInsert, 1, "big bucks");
        builder.apply(nicknameInsert, 8, "jellybean");

        Revision second = builder.commit();
        Object[] parameters2 = {};

        result = first.diff(second, namesLeftNicknames, parameters2);

        assertEquals(result.nextRow(), QueryResult.Type.Inserted);
        assertEquals(result.nextItem(), "tom");
        assertEquals(result.nextItem(), "big bucks");
        assertEquals(result.nextRow(), QueryResult.Type.Inserted);
        assertEquals(result.nextItem(), "rapunzel");
        assertEquals(result.nextItem(), "no name");
        assertEquals(result.nextRow(), QueryResult.Type.Inserted);
        assertEquals(result.nextItem(), "carlos");
        assertEquals(result.nextItem(), null);
        assertEquals(result.nextRow(), QueryResult.Type.Inserted);
        assertEquals(result.nextItem(), "benjamin");
        assertEquals(result.nextItem(), "jellybean");
        assertEquals(result.nextRow(), QueryResult.Type.End);
        Object[] parameters3 = {};

        result = tail.diff(second, namesLeftNicknames, parameters3);

        assertEquals(result.nextRow(), QueryResult.Type.Inserted);
        assertEquals(result.nextItem(), "tom");
        assertEquals(result.nextItem(), "big bucks");
        assertEquals(result.nextRow(), QueryResult.Type.Inserted);
        assertEquals(result.nextItem(), "tom");
        assertEquals(result.nextItem(), "moneybags");
        assertEquals(result.nextRow(), QueryResult.Type.Inserted);
        assertEquals(result.nextItem(), "ted");
        assertEquals(result.nextItem(), null);
        assertEquals(result.nextRow(), QueryResult.Type.Inserted);
        assertEquals(result.nextItem(), "tim");
        assertEquals(result.nextItem(), "eight ball");
        assertEquals(result.nextRow(), QueryResult.Type.Inserted);
        assertEquals(result.nextItem(), "tod");
        assertEquals(result.nextItem(), "baldy");
        assertEquals(result.nextRow(), QueryResult.Type.Inserted);
        assertEquals(result.nextItem(), "tes");
        assertEquals(result.nextItem(), "knuckles");
        assertEquals(result.nextRow(), QueryResult.Type.Inserted);
        assertEquals(result.nextItem(), "rapunzel");
        assertEquals(result.nextItem(), "no name");
        assertEquals(result.nextRow(), QueryResult.Type.Inserted);
        assertEquals(result.nextItem(), "carlos");
        assertEquals(result.nextItem(), null);
        assertEquals(result.nextRow(), QueryResult.Type.Inserted);
        assertEquals(result.nextItem(), "benjamin");
        assertEquals(result.nextItem(), "jellybean");
        assertEquals(result.nextRow(), QueryResult.Type.End);
    	
    }
    
    @Test
    public void testCompoundJoins(){
    	
        Column id = new Column(Integer.class);
        Column name = new Column(String.class);
        Table names = new Table(list(id));

        Column nickname = new Column(String.class);
        Table nicknames = new Table(list(id, nickname));

        Column lastname = new Column(String.class);
        Table lastnames = new Table(list(name));

        Column string = new Column(String.class);
        Column color = new Column(String.class);
        Table colors = new Table(list(string));

        Revision tail = Revisions.Empty;

        PatchTemplate nameInsert = new InsertTemplate
          (names,
           list(id, name),
           list((Expression) new Parameter(), new Parameter()),
           DuplicateKeyResolution.Throw);

        PatchTemplate nicknameInsert = new InsertTemplate
          (nicknames,
           list(id, nickname),
           list((Expression) new Parameter(), new Parameter()),
           DuplicateKeyResolution.Throw);

        PatchTemplate lastnameInsert = new InsertTemplate
          (lastnames,
           list(name, lastname),
           list((Expression) new Parameter(), new Parameter()),
           DuplicateKeyResolution.Throw);

        PatchTemplate colorInsert = new InsertTemplate
          (colors,
           list(string, color),
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
          (list((Expression) new ColumnReference(namesReference, name),
                (Expression) new ColumnReference(nicknamesReference, nickname),
                (Expression) new ColumnReference(colorsReference, color)),
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
             new ColumnReference(namesReference, id),
             new ColumnReference(nicknamesReference, id)),
            new BinaryOperation
            (BinaryOperation.Type.Equal,
             new ColumnReference(colorsReference, string),
             new ColumnReference(nicknamesReference, nickname))));
        Object[] parameters = {};
        
        QueryResult result = tail.diff(first, namesInnerNicknamesInnerColors, parameters);

        assertEquals(result.nextRow(), QueryResult.Type.Inserted);
        assertEquals(result.nextItem(), "tom");
        assertEquals(result.nextItem(), "big bucks");
        assertEquals(result.nextItem(), "red");
        assertEquals(result.nextRow(), QueryResult.Type.Inserted);
        assertEquals(result.nextItem(), "tim");
        assertEquals(result.nextItem(), "eight ball");
        assertEquals(result.nextItem(), "sky blue");
        assertEquals(result.nextRow(), QueryResult.Type.Inserted);
        assertEquals(result.nextItem(), "tod");
        assertEquals(result.nextItem(), "baldy");
        assertEquals(result.nextItem(), "green");
        assertEquals(result.nextRow(), QueryResult.Type.End);

        QueryTemplate namesLeftNicknamesInnerColors = new QueryTemplate
          (list((Expression) new ColumnReference(namesReference, name),
                (Expression) new ColumnReference(nicknamesReference, nickname),
                (Expression) new ColumnReference(colorsReference, color)),
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
             new ColumnReference(namesReference, id),
             new ColumnReference(nicknamesReference, id)),
            new BinaryOperation
            (BinaryOperation.Type.Equal,
             new ColumnReference(colorsReference, string),
             new ColumnReference(nicknamesReference, nickname))));
        Object[] parameters1 = {};
        
        result = tail.diff(first, namesLeftNicknamesInnerColors, parameters1);

        assertEquals(result.nextRow(), QueryResult.Type.Inserted);
        assertEquals(result.nextItem(), "tom");
        assertEquals(result.nextItem(), "big bucks");
        assertEquals(result.nextItem(), "red");
        assertEquals(result.nextRow(), QueryResult.Type.Inserted);
        assertEquals(result.nextItem(), "tim");
        assertEquals(result.nextItem(), "eight ball");
        assertEquals(result.nextItem(), "sky blue");
        assertEquals(result.nextRow(), QueryResult.Type.Inserted);
        assertEquals(result.nextItem(), "tod");
        assertEquals(result.nextItem(), "baldy");
        assertEquals(result.nextItem(), "green");
        assertEquals(result.nextRow(), QueryResult.Type.End);

        QueryTemplate namesInnerNicknamesLeftColors = new QueryTemplate
          (list((Expression) new ColumnReference(namesReference, name),
                (Expression) new ColumnReference(nicknamesReference, nickname),
                (Expression) new ColumnReference(colorsReference, color)),
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
             new ColumnReference(namesReference, id),
             new ColumnReference(nicknamesReference, id)),
            new BinaryOperation
            (BinaryOperation.Type.Equal,
             new ColumnReference(colorsReference, string),
             new ColumnReference(nicknamesReference, nickname))));
        Object[] parameters2 = {};
        
        result = tail.diff(first, namesInnerNicknamesLeftColors, parameters2);

        assertEquals(result.nextRow(), QueryResult.Type.Inserted);
        assertEquals(result.nextItem(), "tom");
        assertEquals(result.nextItem(), "big bucks");
        assertEquals(result.nextItem(), "red");
        assertEquals(result.nextRow(), QueryResult.Type.Inserted);
        assertEquals(result.nextItem(), "tom");
        assertEquals(result.nextItem(), "moneybags");
        assertEquals(result.nextItem(), null);
        assertEquals(result.nextRow(), QueryResult.Type.Inserted);
        assertEquals(result.nextItem(), "tim");
        assertEquals(result.nextItem(), "eight ball");
        assertEquals(result.nextItem(), "sky blue");
        assertEquals(result.nextRow(), QueryResult.Type.Inserted);
        assertEquals(result.nextItem(), "tod");
        assertEquals(result.nextItem(), "baldy");
        assertEquals(result.nextItem(), "green");
        assertEquals(result.nextRow(), QueryResult.Type.Inserted);
        assertEquals(result.nextItem(), "tes");
        assertEquals(result.nextItem(), "knuckles");
        assertEquals(result.nextItem(), null);
        assertEquals(result.nextRow(), QueryResult.Type.End);

        QueryTemplate namesInnerLastnamesLeftNicknamesLeftColors
          = new QueryTemplate
          (list((Expression) new ColumnReference(namesReference, name),
                (Expression) new ColumnReference(lastnamesReference, lastname),
                (Expression) new ColumnReference(nicknamesReference, nickname),
                (Expression) new ColumnReference(colorsReference, color)),
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
             new ColumnReference(namesReference, name),
             new ColumnReference(lastnamesReference, name)),
            new BinaryOperation
            (BinaryOperation.Type.And,
             new BinaryOperation
             (BinaryOperation.Type.Equal,
              new ColumnReference(namesReference, id),
              new ColumnReference(nicknamesReference, id)),
             new BinaryOperation
             (BinaryOperation.Type.Equal,
              new ColumnReference(colorsReference, string),
              new ColumnReference(nicknamesReference, nickname)))));
        Object[] parameters3 = {};
        
        result = tail.diff(first, namesInnerLastnamesLeftNicknamesLeftColors, parameters3);

        assertEquals(result.nextRow(), QueryResult.Type.Inserted);
        assertEquals(result.nextItem(), "tom");
        assertEquals(result.nextItem(), "thumb");
        assertEquals(result.nextItem(), "big bucks");
        assertEquals(result.nextItem(), "red");
        assertEquals(result.nextRow(), QueryResult.Type.Inserted);
        assertEquals(result.nextItem(), "tom");
        assertEquals(result.nextItem(), "thumb");
        assertEquals(result.nextItem(), "moneybags");
        assertEquals(result.nextItem(), null);
        assertEquals(result.nextRow(), QueryResult.Type.Inserted);
        assertEquals(result.nextItem(), "ted");
        assertEquals(result.nextItem(), "thomson");
        assertEquals(result.nextItem(), null);
        assertEquals(result.nextItem(), null);
        assertEquals(result.nextRow(), QueryResult.Type.Inserted);
        assertEquals(result.nextItem(), "tes");
        assertEquals(result.nextItem(), "teasdale");
        assertEquals(result.nextItem(), "knuckles");
        assertEquals(result.nextItem(), null);
        assertEquals(result.nextRow(), QueryResult.Type.End);	
    }
}

package unittests;

import junit.framework.TestCase;

import org.junit.Test;

import static com.readytalk.oss.dbms.imp.Util.list;
import static org.junit.Assert.*;

import com.readytalk.oss.dbms.DBMS;
import com.readytalk.oss.dbms.DBMS.BinaryOperationType;
import com.readytalk.oss.dbms.DBMS.Column;
import com.readytalk.oss.dbms.DBMS.JoinType;
import com.readytalk.oss.dbms.DBMS.Table;
import com.readytalk.oss.dbms.DBMS.Expression;
import com.readytalk.oss.dbms.DBMS.Revision;
import com.readytalk.oss.dbms.DBMS.PatchContext;
import com.readytalk.oss.dbms.DBMS.PatchTemplate;
import com.readytalk.oss.dbms.DBMS.TableReference;
import com.readytalk.oss.dbms.DBMS.QueryTemplate;
import com.readytalk.oss.dbms.DBMS.QueryResult;
import com.readytalk.oss.dbms.DBMS.ResultType;
import com.readytalk.oss.dbms.DBMS.DuplicateKeyResolution;
import com.readytalk.oss.dbms.imp.MyDBMS;

public class JoinTest extends TestCase{
    
    @Test
    public void testSimpleJoins(){
    	
        DBMS dbms = new MyDBMS();

        Column id = dbms.column(Integer.class);
        Column name = dbms.column(String.class);
        Table names = dbms.table(list(id));

        Column nickname = dbms.column(String.class);
        Table nicknames = dbms.table(list(id, nickname));

        Revision tail = dbms.revision();

        PatchTemplate nameInsert = dbms.insertTemplate
          (names,
           list(id, name),
           list(dbms.parameter(),
                dbms.parameter()), DuplicateKeyResolution.Throw);

        PatchTemplate nicknameInsert = dbms.insertTemplate
          (nicknames,
           list(id, nickname),
           list(dbms.parameter(),
                dbms.parameter()), DuplicateKeyResolution.Throw);

        PatchContext context = dbms.patchContext(tail);

        dbms.apply(context, nameInsert, 1, "tom");
        dbms.apply(context, nameInsert, 2, "ted");
        dbms.apply(context, nameInsert, 3, "tim");
        dbms.apply(context, nameInsert, 4, "tod");
        dbms.apply(context, nameInsert, 5, "tes");

        dbms.apply(context, nicknameInsert, 1, "moneybags");
        dbms.apply(context, nicknameInsert, 3, "eight ball");
        dbms.apply(context, nicknameInsert, 4, "baldy");
        dbms.apply(context, nicknameInsert, 5, "knuckles");
        dbms.apply(context, nicknameInsert, 6, "no name");

        Revision first = dbms.commit(context);
       
        TableReference namesReference = dbms.tableReference(names);
        TableReference nicknamesReference = dbms.tableReference(nicknames);

        QueryTemplate namesInnerNicknames = dbms.queryTemplate
          (list((Expression) dbms.columnReference(namesReference, name),
                (Expression) dbms.columnReference(nicknamesReference, nickname)),
           dbms.join
           (JoinType.Inner,
            namesReference,
            nicknamesReference),
           dbms.operation
           (BinaryOperationType.Equal,
            dbms.columnReference(namesReference, id),
            dbms.columnReference(nicknamesReference, id)));
        
        QueryResult result = dbms.diff(tail, first, namesInnerNicknames);

        assertEquals(result.nextRow(), ResultType.Inserted);
        assertEquals(result.nextItem(), "tom");
        assertEquals(result.nextItem(), "moneybags");
        assertEquals(result.nextRow(), ResultType.Inserted);
        assertEquals(result.nextItem(), "tim");
        assertEquals(result.nextItem(), "eight ball");
        assertEquals(result.nextRow(), ResultType.Inserted);
        assertEquals(result.nextItem(), "tod");
        assertEquals(result.nextItem(), "baldy");
        assertEquals(result.nextRow(), ResultType.Inserted);
        assertEquals(result.nextItem(), "tes");
        assertEquals(result.nextItem(), "knuckles");
        assertEquals(result.nextRow(), ResultType.End);

        QueryTemplate namesLeftNicknames = dbms.queryTemplate
          (list((Expression) dbms.columnReference(namesReference, name),
                (Expression) dbms.columnReference(nicknamesReference, nickname)),
           dbms.join
           (JoinType.LeftOuter,
            namesReference,
            nicknamesReference),
           dbms.operation
           (BinaryOperationType.Equal,
            dbms.columnReference(namesReference, id),
            dbms.columnReference(nicknamesReference, id)));
        
        result = dbms.diff(tail, first, namesLeftNicknames);

        assertEquals(result.nextRow(), ResultType.Inserted);
        assertEquals(result.nextItem(), "tom");
        assertEquals(result.nextItem(), "moneybags");
        assertEquals(result.nextRow(), ResultType.Inserted);
        assertEquals(result.nextItem(), "ted");
        assertEquals(result.nextItem(), null);
        assertEquals(result.nextRow(), ResultType.Inserted);
        assertEquals(result.nextItem(), "tim");
        assertEquals(result.nextItem(), "eight ball");
        assertEquals(result.nextRow(), ResultType.Inserted);
        assertEquals(result.nextItem(), "tod");
        assertEquals(result.nextItem(), "baldy");
        assertEquals(result.nextRow(), ResultType.Inserted);
        assertEquals(result.nextItem(), "tes");
        assertEquals(result.nextItem(), "knuckles");
        assertEquals(result.nextRow(), ResultType.End);

        context = dbms.patchContext(first);

        dbms.apply(context, nameInsert, 6, "rapunzel");
        dbms.apply(context, nameInsert, 7, "carlos");
        dbms.apply(context, nameInsert, 8, "benjamin");

        dbms.apply(context, nicknameInsert, 1, "big bucks");
        dbms.apply(context, nicknameInsert, 8, "jellybean");

        Revision second = dbms.commit(context);

        result = dbms.diff(first, second, namesLeftNicknames);

        assertEquals(result.nextRow(), ResultType.Inserted);
        assertEquals(result.nextItem(), "tom");
        assertEquals(result.nextItem(), "big bucks");
        assertEquals(result.nextRow(), ResultType.Inserted);
        assertEquals(result.nextItem(), "rapunzel");
        assertEquals(result.nextItem(), "no name");
        assertEquals(result.nextRow(), ResultType.Inserted);
        assertEquals(result.nextItem(), "carlos");
        assertEquals(result.nextItem(), null);
        assertEquals(result.nextRow(), ResultType.Inserted);
        assertEquals(result.nextItem(), "benjamin");
        assertEquals(result.nextItem(), "jellybean");
        assertEquals(result.nextRow(), ResultType.End);

        result = dbms.diff(tail, second, namesLeftNicknames);

        assertEquals(result.nextRow(), ResultType.Inserted);
        assertEquals(result.nextItem(), "tom");
        assertEquals(result.nextItem(), "big bucks");
        assertEquals(result.nextRow(), ResultType.Inserted);
        assertEquals(result.nextItem(), "tom");
        assertEquals(result.nextItem(), "moneybags");
        assertEquals(result.nextRow(), ResultType.Inserted);
        assertEquals(result.nextItem(), "ted");
        assertEquals(result.nextItem(), null);
        assertEquals(result.nextRow(), ResultType.Inserted);
        assertEquals(result.nextItem(), "tim");
        assertEquals(result.nextItem(), "eight ball");
        assertEquals(result.nextRow(), ResultType.Inserted);
        assertEquals(result.nextItem(), "tod");
        assertEquals(result.nextItem(), "baldy");
        assertEquals(result.nextRow(), ResultType.Inserted);
        assertEquals(result.nextItem(), "tes");
        assertEquals(result.nextItem(), "knuckles");
        assertEquals(result.nextRow(), ResultType.Inserted);
        assertEquals(result.nextItem(), "rapunzel");
        assertEquals(result.nextItem(), "no name");
        assertEquals(result.nextRow(), ResultType.Inserted);
        assertEquals(result.nextItem(), "carlos");
        assertEquals(result.nextItem(), null);
        assertEquals(result.nextRow(), ResultType.Inserted);
        assertEquals(result.nextItem(), "benjamin");
        assertEquals(result.nextItem(), "jellybean");
        assertEquals(result.nextRow(), ResultType.End);
    	
    }
    
    @Test
    public void testCompoundJoins(){
    	
        DBMS dbms = new MyDBMS();

        Column id = dbms.column(Integer.class);
        Column name = dbms.column(String.class);
        Table names = dbms.table(list(id));

        Column nickname = dbms.column(String.class);
        Table nicknames = dbms.table(list(id, nickname));

        Column lastname = dbms.column(String.class);
        Table lastnames = dbms.table(list(name));

        Column string = dbms.column(String.class);
        Column color = dbms.column(String.class);
        Table colors = dbms.table(list(string));

        Revision tail = dbms.revision();

        PatchTemplate nameInsert = dbms.insertTemplate
          (names,
           list(id, name),
           list(dbms.parameter(),
                dbms.parameter()), DuplicateKeyResolution.Throw);

        PatchTemplate nicknameInsert = dbms.insertTemplate
          (nicknames,
           list(id, nickname),
           list(dbms.parameter(),
                dbms.parameter()), DuplicateKeyResolution.Throw);

        PatchTemplate lastnameInsert = dbms.insertTemplate
          (lastnames,
           list(name, lastname),
           list(dbms.parameter(),
                dbms.parameter()), DuplicateKeyResolution.Throw);

        PatchTemplate colorInsert = dbms.insertTemplate
          (colors,
           list(string, color),
           list(dbms.parameter(),
                dbms.parameter()), DuplicateKeyResolution.Throw);

        PatchContext context = dbms.patchContext(tail);

        dbms.apply(context, nameInsert, 1, "tom");
        dbms.apply(context, nameInsert, 2, "ted");
        dbms.apply(context, nameInsert, 3, "tim");
        dbms.apply(context, nameInsert, 4, "tod");
        dbms.apply(context, nameInsert, 5, "tes");

        dbms.apply(context, nicknameInsert, 1, "moneybags");
        dbms.apply(context, nicknameInsert, 1, "big bucks");
        dbms.apply(context, nicknameInsert, 3, "eight ball");
        dbms.apply(context, nicknameInsert, 4, "baldy");
        dbms.apply(context, nicknameInsert, 5, "knuckles");
        dbms.apply(context, nicknameInsert, 6, "no name");

        dbms.apply(context, lastnameInsert, "tom", "thumb");
        dbms.apply(context, lastnameInsert, "ted", "thomson");
        dbms.apply(context, lastnameInsert, "tes", "teasdale");

        dbms.apply(context, colorInsert, "big bucks", "red");
        dbms.apply(context, colorInsert, "baldy", "green");
        dbms.apply(context, colorInsert, "no name", "pink");
        dbms.apply(context, colorInsert, "eight ball", "sky blue");

        Revision first = dbms.commit(context);
       
        TableReference namesReference = dbms.tableReference(names);
        TableReference nicknamesReference = dbms.tableReference(nicknames);
        TableReference lastnamesReference = dbms.tableReference(lastnames);
        TableReference colorsReference = dbms.tableReference(colors);

        QueryTemplate namesInnerNicknamesInnerColors = dbms.queryTemplate
          (list((Expression) dbms.columnReference(namesReference, name),
                (Expression) dbms.columnReference(nicknamesReference, nickname),
                (Expression) dbms.columnReference(colorsReference, color)),
           dbms.join
           (JoinType.Inner,
            dbms.join
            (JoinType.Inner,
             namesReference,
             nicknamesReference),
            colorsReference),
           dbms.operation
           (BinaryOperationType.And,
            dbms.operation
            (BinaryOperationType.Equal,
             dbms.columnReference(namesReference, id),
             dbms.columnReference(nicknamesReference, id)),
            dbms.operation
            (BinaryOperationType.Equal,
             dbms.columnReference(colorsReference, string),
             dbms.columnReference(nicknamesReference, nickname))));
        
        QueryResult result = dbms.diff
          (tail, first, namesInnerNicknamesInnerColors);

        assertEquals(result.nextRow(), ResultType.Inserted);
        assertEquals(result.nextItem(), "tom");
        assertEquals(result.nextItem(), "big bucks");
        assertEquals(result.nextItem(), "red");
        assertEquals(result.nextRow(), ResultType.Inserted);
        assertEquals(result.nextItem(), "tim");
        assertEquals(result.nextItem(), "eight ball");
        assertEquals(result.nextItem(), "sky blue");
        assertEquals(result.nextRow(), ResultType.Inserted);
        assertEquals(result.nextItem(), "tod");
        assertEquals(result.nextItem(), "baldy");
        assertEquals(result.nextItem(), "green");
        assertEquals(result.nextRow(), ResultType.End);

        QueryTemplate namesLeftNicknamesInnerColors = dbms.queryTemplate
          (list((Expression) dbms.columnReference(namesReference, name),
                (Expression) dbms.columnReference(nicknamesReference, nickname),
                (Expression) dbms.columnReference(colorsReference, color)),
           dbms.join
           (JoinType.Inner,
            dbms.join
            (JoinType.LeftOuter,
             namesReference,
             nicknamesReference),
            colorsReference),
           dbms.operation
           (BinaryOperationType.And,
            dbms.operation
            (BinaryOperationType.Equal,
             dbms.columnReference(namesReference, id),
             dbms.columnReference(nicknamesReference, id)),
            dbms.operation
            (BinaryOperationType.Equal,
             dbms.columnReference(colorsReference, string),
             dbms.columnReference(nicknamesReference, nickname))));
        
        result = dbms.diff(tail, first, namesLeftNicknamesInnerColors);

        assertEquals(result.nextRow(), ResultType.Inserted);
        assertEquals(result.nextItem(), "tom");
        assertEquals(result.nextItem(), "big bucks");
        assertEquals(result.nextItem(), "red");
        assertEquals(result.nextRow(), ResultType.Inserted);
        assertEquals(result.nextItem(), "tim");
        assertEquals(result.nextItem(), "eight ball");
        assertEquals(result.nextItem(), "sky blue");
        assertEquals(result.nextRow(), ResultType.Inserted);
        assertEquals(result.nextItem(), "tod");
        assertEquals(result.nextItem(), "baldy");
        assertEquals(result.nextItem(), "green");
        assertEquals(result.nextRow(), ResultType.End);

        QueryTemplate namesInnerNicknamesLeftColors = dbms.queryTemplate
          (list((Expression) dbms.columnReference(namesReference, name),
                (Expression) dbms.columnReference(nicknamesReference, nickname),
                (Expression) dbms.columnReference(colorsReference, color)),
           dbms.join
           (JoinType.LeftOuter,
            dbms.join
            (JoinType.Inner,
             namesReference,
             nicknamesReference),
            colorsReference),
           dbms.operation
           (BinaryOperationType.And,
            dbms.operation
            (BinaryOperationType.Equal,
             dbms.columnReference(namesReference, id),
             dbms.columnReference(nicknamesReference, id)),
            dbms.operation
            (BinaryOperationType.Equal,
             dbms.columnReference(colorsReference, string),
             dbms.columnReference(nicknamesReference, nickname))));
        
        result = dbms.diff(tail, first, namesInnerNicknamesLeftColors);

        assertEquals(result.nextRow(), ResultType.Inserted);
        assertEquals(result.nextItem(), "tom");
        assertEquals(result.nextItem(), "big bucks");
        assertEquals(result.nextItem(), "red");
        assertEquals(result.nextRow(), ResultType.Inserted);
        assertEquals(result.nextItem(), "tom");
        assertEquals(result.nextItem(), "moneybags");
        assertEquals(result.nextItem(), null);
        assertEquals(result.nextRow(), ResultType.Inserted);
        assertEquals(result.nextItem(), "tim");
        assertEquals(result.nextItem(), "eight ball");
        assertEquals(result.nextItem(), "sky blue");
        assertEquals(result.nextRow(), ResultType.Inserted);
        assertEquals(result.nextItem(), "tod");
        assertEquals(result.nextItem(), "baldy");
        assertEquals(result.nextItem(), "green");
        assertEquals(result.nextRow(), ResultType.Inserted);
        assertEquals(result.nextItem(), "tes");
        assertEquals(result.nextItem(), "knuckles");
        assertEquals(result.nextItem(), null);
        assertEquals(result.nextRow(), ResultType.End);

        QueryTemplate namesInnerLastnamesLeftNicknamesLeftColors
          = dbms.queryTemplate
          (list((Expression) dbms.columnReference(namesReference, name),
                (Expression) dbms.columnReference(lastnamesReference, lastname),
                (Expression) dbms.columnReference(nicknamesReference, nickname),
                (Expression) dbms.columnReference(colorsReference, color)),
           dbms.join
           (JoinType.LeftOuter,
            dbms.join
            (JoinType.Inner,
             namesReference,
             lastnamesReference),
            dbms.join
            (JoinType.LeftOuter,
             nicknamesReference,
             colorsReference)),
           dbms.operation
           (BinaryOperationType.And,
            dbms.operation
            (BinaryOperationType.Equal,
             dbms.columnReference(namesReference, name),
             dbms.columnReference(lastnamesReference, name)),
            dbms.operation
            (BinaryOperationType.And,
             dbms.operation
             (BinaryOperationType.Equal,
              dbms.columnReference(namesReference, id),
              dbms.columnReference(nicknamesReference, id)),
             dbms.operation
             (BinaryOperationType.Equal,
              dbms.columnReference(colorsReference, string),
              dbms.columnReference(nicknamesReference, nickname)))));
        
        result = dbms.diff
          (tail, first, namesInnerLastnamesLeftNicknamesLeftColors);

        assertEquals(result.nextRow(), ResultType.Inserted);
        assertEquals(result.nextItem(), "tom");
        assertEquals(result.nextItem(), "thumb");
        assertEquals(result.nextItem(), "big bucks");
        assertEquals(result.nextItem(), "red");
        assertEquals(result.nextRow(), ResultType.Inserted);
        assertEquals(result.nextItem(), "tom");
        assertEquals(result.nextItem(), "thumb");
        assertEquals(result.nextItem(), "moneybags");
        assertEquals(result.nextItem(), null);
        assertEquals(result.nextRow(), ResultType.Inserted);
        assertEquals(result.nextItem(), "ted");
        assertEquals(result.nextItem(), "thomson");
        assertEquals(result.nextItem(), null);
        assertEquals(result.nextItem(), null);
        assertEquals(result.nextRow(), ResultType.Inserted);
        assertEquals(result.nextItem(), "tes");
        assertEquals(result.nextItem(), "teasdale");
        assertEquals(result.nextItem(), "knuckles");
        assertEquals(result.nextItem(), null);
        assertEquals(result.nextRow(), ResultType.End);	
    }
}

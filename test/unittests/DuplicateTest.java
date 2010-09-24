package unittests;
import org.junit.Test;

import static com.readytalk.oss.dbms.imp.Util.list;
import static org.junit.Assert.*;

import com.readytalk.oss.dbms.DBMS;
import com.readytalk.oss.dbms.DBMS.BinaryOperationType;
import com.readytalk.oss.dbms.DBMS.Column;
import com.readytalk.oss.dbms.DBMS.DuplicateKeyException;
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

public class DuplicateTest{
    
    @Test
    public void testDuplicateInserts(){
    	DBMS dbms = new MyDBMS();

        Column number = dbms.column(Integer.class);
        Column name = dbms.column(String.class);
        Table numbers = dbms.table(list(number));

        Revision tail = dbms.revision();

        PatchTemplate insert = dbms.insertTemplate
          (numbers,
           list(number, name),
           list(dbms.parameter(),
                dbms.parameter()), DuplicateKeyResolution.Throw);

        PatchContext context = dbms.patchContext(tail);

        dbms.apply(context, insert, 1, "one");
        dbms.apply(context, insert, 2, "two");
        dbms.apply(context, insert, 3, "three");

        Revision first = dbms.commit(context);

        try {
          dbms.apply(dbms.patchContext(first), insert, 1, "uno");
          throw new RuntimeException();
        } catch (DuplicateKeyException e) { }

        try {
          dbms.apply(dbms.patchContext(first), insert, 2, "dos");
          throw new RuntimeException();
        } catch (DuplicateKeyException e) { }

        try {
          dbms.apply(dbms.patchContext(first), insert, 3, "tres");
          throw new RuntimeException();
        } catch (DuplicateKeyException e) { }

        context = dbms.patchContext(first);

        dbms.apply(context, insert, 4, "quatro");

        PatchTemplate insertOrUpdate = dbms.insertTemplate
          (numbers,
           list(number, name),
           list(dbms.parameter(),
                dbms.parameter()), DuplicateKeyResolution.Overwrite);

        dbms.apply(context, insertOrUpdate, 1, "uno");

        Revision second = dbms.commit(context);

        TableReference numbersReference = dbms.tableReference(numbers);

        QueryTemplate any = dbms.queryTemplate
          (list((Expression) dbms.columnReference(numbersReference, name)),
           numbersReference,
           dbms.constant(true));

        QueryResult result = dbms.diff(tail, second, any);

        assertEquals(result.nextRow(), ResultType.Inserted);
        assertEquals(result.nextItem(), "uno");
        assertEquals(result.nextRow(), ResultType.Inserted);
        assertEquals(result.nextItem(), "two");
        assertEquals(result.nextRow(), ResultType.Inserted);
        assertEquals(result.nextItem(), "three");
        assertEquals(result.nextRow(), ResultType.Inserted);
        assertEquals(result.nextItem(), "quatro");
        assertEquals(result.nextRow(), ResultType.End);
    	
    }
    
    @Test
    public void testDuplicateUpdates(){
    	DBMS dbms = new MyDBMS();

        Column number = dbms.column(Integer.class);
        Column name = dbms.column(String.class);
        Table numbers = dbms.table(list(number));

        Revision tail = dbms.revision();

        PatchTemplate insert = dbms.insertTemplate
          (numbers,
           list(number, name),
           list(dbms.parameter(),
                dbms.parameter()), DuplicateKeyResolution.Throw);

        PatchContext context = dbms.patchContext(tail);

        dbms.apply(context, insert, 1, "one");
        dbms.apply(context, insert, 2, "two");
        dbms.apply(context, insert, 3, "three");

        Revision first = dbms.commit(context);

        TableReference numbersReference = dbms.tableReference(numbers);

        PatchTemplate updateNumberWhereNumberEqual = dbms.updateTemplate
          (numbersReference,
           dbms.operation
           (BinaryOperationType.Equal,
            dbms.columnReference(numbersReference, number),
            dbms.parameter()),
           list(number),
           list(dbms.parameter()));

        try {
          dbms.apply(dbms.patchContext(first), updateNumberWhereNumberEqual, 1, 2);
          throw new RuntimeException();
        } catch (DuplicateKeyException e) { }

        try {
          dbms.apply(dbms.patchContext(first), updateNumberWhereNumberEqual, 2, 3);
          throw new RuntimeException();
        } catch (DuplicateKeyException e) { }

        context = dbms.patchContext(first);

        dbms.apply(context, updateNumberWhereNumberEqual, 3, 3);
        dbms.apply(context, updateNumberWhereNumberEqual, 4, 2);
        dbms.apply(context, updateNumberWhereNumberEqual, 3, 4);

        Revision second = dbms.commit(context);

        QueryTemplate any = dbms.queryTemplate
          (list((Expression) dbms.columnReference(numbersReference, number),
                (Expression) dbms.columnReference(numbersReference, name)),
           numbersReference,
           dbms.constant(true));

        QueryResult result = dbms.diff(tail, second, any);

        assertEquals(result.nextRow(), ResultType.Inserted);
        assertEquals(result.nextItem(), 1);
        assertEquals(result.nextItem(), "one");
        assertEquals(result.nextRow(), ResultType.Inserted);
        assertEquals(result.nextItem(), 2);
        assertEquals(result.nextItem(), "two");
        assertEquals(result.nextRow(), ResultType.Inserted);
        assertEquals(result.nextItem(), 4);
        assertEquals(result.nextItem(), "three");
        assertEquals(result.nextRow(), ResultType.End);
    	
    }

}
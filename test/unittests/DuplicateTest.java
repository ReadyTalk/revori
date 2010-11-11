package unittests;
import junit.framework.TestCase;

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

public class DuplicateTest extends TestCase{
    
    @Test
    public void testDuplicateInsertsThrowAndOverwrite(){
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
          fail("Inside testDuplicateInserts: Expected Duplicate Key Exception... ");
        } catch (DuplicateKeyException e) { }

        try {
          dbms.apply(dbms.patchContext(first), insert, 2, "dos");
          fail("Inside testDuplicateInserts: Expected Duplicate Key Exception... ");
        } catch (DuplicateKeyException e) { }

        try {
          dbms.apply(dbms.patchContext(first), insert, 3, "tres");
          fail("Inside testDuplicateInserts: Expected Duplicate Key Exception... ");
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
    public void testDuplicateInsertsSkip(){
    	DBMS dbms = new MyDBMS();

        Column number = dbms.column(Integer.class);
        Column name = dbms.column(String.class);
        Table numbers = dbms.table(list(number));

        Revision tail = dbms.revision();

        PatchTemplate insert = dbms.insertTemplate
          (numbers,
           list(number, name),
           list(dbms.parameter(),
                dbms.parameter()), DuplicateKeyResolution.Skip);

        PatchContext context = dbms.patchContext(tail);

        dbms.apply(context, insert, 1, "one");
        dbms.apply(context, insert, 2, "two");
        dbms.apply(context, insert, 3, "three");

        Revision first = dbms.commit(context);
        
        TableReference numbersReference = dbms.tableReference(numbers);
        QueryTemplate q1 = dbms.queryTemplate
        (list((Expression) dbms.columnReference(numbersReference, number), 
        		dbms.columnReference(numbersReference, name)),
         numbersReference,
         dbms.constant(true));
        
        QueryResult result1 = dbms.diff(tail, first, q1);
        
        assertEquals(result1.nextRow(), ResultType.Inserted);
        assertEquals(result1.nextItem(), 1);
        assertEquals(result1.nextItem(), "one");
        assertEquals(result1.nextRow(), ResultType.Inserted);
        assertEquals(result1.nextItem(), 2);
        assertEquals(result1.nextItem(), "two");
        assertEquals(result1.nextRow(), ResultType.Inserted);
        assertEquals(result1.nextItem(), 3);
        assertEquals(result1.nextItem(), "three");
        assertEquals(result1.nextRow(), ResultType.End);        
        
        context = dbms.patchContext(first);

        dbms.apply(context, insert, 1, "uno");
        dbms.apply(context, insert, 2, "dos");
        dbms.apply(context, insert, 3, "tres");
        dbms.apply(context, insert, 4, "quatro");

        Revision second = dbms.commit(context);

        numbersReference = dbms.tableReference(numbers);

        QueryTemplate any = dbms.queryTemplate
          (list((Expression) dbms.columnReference(numbersReference, number), 
        		  (Expression) dbms.columnReference(numbersReference, name)),
           numbersReference,
           dbms.constant(true));

        QueryResult result2 = dbms.diff(tail, second, any);

        assertEquals(result2.nextRow(), ResultType.Inserted);
        assertEquals(result2.nextItem(), 1);
        assertEquals(result2.nextItem(), "one");
        assertEquals(result2.nextRow(), ResultType.Inserted);
        assertEquals(result2.nextItem(), 2);
        assertEquals(result2.nextItem(), "two");
        assertEquals(result2.nextRow(), ResultType.Inserted);
        assertEquals(result2.nextItem(), 3);
        assertEquals(result2.nextItem(), "three");
        assertEquals(result2.nextRow(), ResultType.Inserted);
        assertEquals(result2.nextItem(), 4);
        assertEquals(result2.nextItem(), "quatro");
        assertEquals(result2.nextRow(), ResultType.End);
        
        QueryResult result3 = dbms.diff(first, second, any);
        
        assertEquals(result3.nextRow(), ResultType.Inserted);
        assertEquals(result3.nextItem(), 4);
        assertEquals(result3.nextItem(), "quatro");
        assertEquals(result3.nextRow(), ResultType.End);
    }
    
    @Test
    public void testDuplicateInsertsOverwrite(){
    	DBMS dbms = new MyDBMS();

        Column number = dbms.column(Integer.class);
        Column name = dbms.column(String.class);
        Table numbers = dbms.table(list(number));

        Revision tail = dbms.revision();

        PatchTemplate insert = dbms.insertTemplate
          (numbers,
           list(number, name),
           list(dbms.parameter(),
                dbms.parameter()), DuplicateKeyResolution.Overwrite);

        PatchContext context = dbms.patchContext(tail);

        dbms.apply(context, insert, 1, "one");
        dbms.apply(context, insert, 2, "two");
        dbms.apply(context, insert, 3, "three");

        Revision first = dbms.commit(context);
        
        TableReference numbersReference = dbms.tableReference(numbers);
        QueryTemplate q1 = dbms.queryTemplate
        (list((Expression) dbms.columnReference(numbersReference, number),
        		dbms.columnReference(numbersReference, name)),
         numbersReference,
         dbms.constant(true));
        
        QueryResult result1 = dbms.diff(tail, first, q1);
        
        assertEquals(result1.nextRow(), ResultType.Inserted);
        assertEquals(result1.nextItem(), 1);
        assertEquals(result1.nextItem(), "one");
        assertEquals(result1.nextRow(), ResultType.Inserted);
        assertEquals(result1.nextItem(), 2);
        assertEquals(result1.nextItem(), "two");
        assertEquals(result1.nextRow(), ResultType.Inserted);
        assertEquals(result1.nextItem(), 3);
        assertEquals(result1.nextItem(), "three");
        assertEquals(result1.nextRow(), ResultType.End);        
        
        context = dbms.patchContext(first);

        dbms.apply(context, insert, 1, "uno");
        dbms.apply(context, insert, 2, "dos");
        dbms.apply(context, insert, 3, "tres");
        dbms.apply(context, insert, 4, "quatro");

        Revision second = dbms.commit(context);

        numbersReference = dbms.tableReference(numbers);

        QueryTemplate any = dbms.queryTemplate
          (list((Expression) dbms.columnReference(numbersReference, number), 
        		  (Expression) dbms.columnReference(numbersReference, name)),
           numbersReference,
           dbms.constant(true));

        QueryResult result2 = dbms.diff(tail, second, any);

        assertEquals(result2.nextRow(), ResultType.Inserted);
        assertEquals(result2.nextItem(), 1);
        assertEquals(result2.nextItem(), "uno");
        assertEquals(result2.nextRow(), ResultType.Inserted);
        assertEquals(result2.nextItem(), 2);
        assertEquals(result2.nextItem(), "dos");
        assertEquals(result2.nextRow(), ResultType.Inserted);
        assertEquals(result2.nextItem(), 3);
        assertEquals(result2.nextItem(), "tres");
        assertEquals(result2.nextRow(), ResultType.Inserted);
        assertEquals(result2.nextItem(), 4);
        assertEquals(result2.nextItem(), "quatro");
        assertEquals(result2.nextRow(), ResultType.End);
        
        QueryResult result3 = dbms.diff(first, second, any);
        
        assertEquals(result3.nextRow(), ResultType.Deleted);
        assertEquals(result3.nextItem(), 1);
        assertEquals(result3.nextItem(), "one");
        assertEquals(result3.nextRow(), ResultType.Inserted);
        assertEquals(result3.nextItem(), 1);
        assertEquals(result3.nextItem(), "uno");
        assertEquals(result3.nextRow(), ResultType.Deleted);
        assertEquals(result3.nextItem(), 2);
        assertEquals(result3.nextItem(), "two");
        assertEquals(result3.nextRow(), ResultType.Inserted);
        assertEquals(result3.nextItem(), 2);
        assertEquals(result3.nextItem(), "dos");
        assertEquals(result3.nextRow(), ResultType.Deleted);
        assertEquals(result3.nextItem(), 3);
        assertEquals(result3.nextItem(), "three");
        assertEquals(result3.nextRow(), ResultType.Inserted);
        assertEquals(result3.nextItem(), 3);
        assertEquals(result3.nextItem(), "tres");
        assertEquals(result3.nextRow(), ResultType.Inserted);
        assertEquals(result3.nextItem(), 4);
        assertEquals(result3.nextItem(), "quatro");
        assertEquals(result3.nextRow(), ResultType.End);
    }
    
    @Test
    public void testDuplicateInsertsMultiKeyThrow(){
    	DBMS dbms = new MyDBMS();

        Column number = dbms.column(Integer.class);
        Column key = dbms.column(Integer.class);
        Column name = dbms.column(String.class);
        Table numbers = dbms.table(list(number, key));

        Revision tail = dbms.revision();

        PatchTemplate insert = dbms.insertTemplate
          (numbers,
           list(number, name, key),
           list(dbms.parameter(), dbms.parameter(),
                dbms.parameter()), DuplicateKeyResolution.Throw);

        PatchContext context = dbms.patchContext(tail);

        dbms.apply(context, insert, 1, "one", 1);
        dbms.apply(context, insert, 2, "two", 2);
        dbms.apply(context, insert, 3, "three", 3);

        Revision first = dbms.commit(context);
        
        TableReference numbersReference = dbms.tableReference(numbers);
        QueryTemplate q1 = dbms.queryTemplate
        (list((Expression) dbms.columnReference(numbersReference, number),
        		dbms.columnReference(numbersReference, name),
        		dbms.columnReference(numbersReference, key)),
         numbersReference,
         dbms.constant(true));
        
        QueryResult result1 = dbms.diff(tail, first, q1);
        
        assertEquals(result1.nextRow(), ResultType.Inserted);
        assertEquals(result1.nextItem(), 1);
        assertEquals(result1.nextItem(), "one");
        assertEquals(result1.nextItem(), 1);
        assertEquals(result1.nextRow(), ResultType.Inserted);
        assertEquals(result1.nextItem(), 2);
        assertEquals(result1.nextItem(), "two");
        assertEquals(result1.nextItem(), 2);
        assertEquals(result1.nextRow(), ResultType.Inserted);
        assertEquals(result1.nextItem(), 3);
        assertEquals(result1.nextItem(), "three");
        assertEquals(result1.nextItem(), 3);
        assertEquals(result1.nextRow(), ResultType.End);        
        
        context = dbms.patchContext(first);

        try{
            dbms.apply(context, insert, 1, "uno", 1);
            fail("Inside testDuplicateInsertsMultiKeyThrow: Expected Duplicate Key Exception...");
        }catch(DuplicateKeyException expected){}
        context = dbms.patchContext(first);
        try{
            dbms.apply(context, insert, 2, "dos", 2);
            fail("Inside testDuplicateInsertsMultiKeyThrow: Expected Duplicate Key Exception...");
        }catch(DuplicateKeyException expected){}
        context = dbms.patchContext(first);
        try{
            dbms.apply(context, insert, 3, "tres", 3);
            fail("Inside testDuplicateInsertsMultiKeyThrow: Expected Duplicate Key Exception...");
        }catch(DuplicateKeyException expected){}
        context = dbms.patchContext(first);
        
        dbms.apply(context, insert, 4, "quatro", 4);

        Revision second = dbms.commit(context);

        numbersReference = dbms.tableReference(numbers);

        QueryTemplate any = dbms.queryTemplate
          (list((Expression) dbms.columnReference(numbersReference, number), 
        		  (Expression) dbms.columnReference(numbersReference, name),
        		  (Expression) dbms.columnReference(numbersReference, key)),
           numbersReference,
           dbms.constant(true));

        QueryResult result2 = dbms.diff(tail, second, any);

        assertEquals(result2.nextRow(), ResultType.Inserted);
        assertEquals(result2.nextItem(), 1);
        assertEquals(result2.nextItem(), "one");
        assertEquals(result2.nextItem(), 1);
        assertEquals(result2.nextRow(), ResultType.Inserted);
        assertEquals(result2.nextItem(), 2);
        assertEquals(result2.nextItem(), "two");
        assertEquals(result2.nextItem(), 2);
        assertEquals(result2.nextRow(), ResultType.Inserted);
        assertEquals(result2.nextItem(), 3);
        assertEquals(result2.nextItem(), "three");
        assertEquals(result2.nextItem(), 3);
        assertEquals(result2.nextRow(), ResultType.Inserted);
        assertEquals(result2.nextItem(), 4);
        assertEquals(result2.nextItem(), "quatro");
        assertEquals(result2.nextItem(), 4);
        assertEquals(result2.nextRow(), ResultType.End);
        
        QueryResult result3 = dbms.diff(first, second, any);
        
        assertEquals(result3.nextRow(), ResultType.Inserted);
        assertEquals(result3.nextItem(), 4);
        assertEquals(result3.nextItem(), "quatro");
        assertEquals(result3.nextItem(), 4);
        assertEquals(result3.nextRow(), ResultType.End);
    }
    
    @Test
    public void testDuplicateInsertsMultiKeySkip(){
    	DBMS dbms = new MyDBMS();

        Column number = dbms.column(Integer.class);
        Column key = dbms.column(Integer.class);
        Column name = dbms.column(String.class);
        Table numbers = dbms.table(list(number, key));

        Revision tail = dbms.revision();

        PatchTemplate insert = dbms.insertTemplate
          (numbers,
           list(number, name, key),
           list(dbms.parameter(), dbms.parameter(),
                dbms.parameter()), DuplicateKeyResolution.Skip);

        PatchContext context = dbms.patchContext(tail);

        dbms.apply(context, insert, 1, "one", 1);
        dbms.apply(context, insert, 2, "two", 2);
        dbms.apply(context, insert, 3, "three", 3);

        Revision first = dbms.commit(context);
        
        TableReference numbersReference = dbms.tableReference(numbers);
        QueryTemplate q1 = dbms.queryTemplate
        (list((Expression) dbms.columnReference(numbersReference, number),
        		dbms.columnReference(numbersReference, name),
        		dbms.columnReference(numbersReference, key)),
         numbersReference,
         dbms.constant(true));
        
        QueryResult result1 = dbms.diff(tail, first, q1);
        
        assertEquals(result1.nextRow(), ResultType.Inserted);
        assertEquals(result1.nextItem(), 1);
        assertEquals(result1.nextItem(), "one");
        assertEquals(result1.nextItem(), 1);
        assertEquals(result1.nextRow(), ResultType.Inserted);
        assertEquals(result1.nextItem(), 2);
        assertEquals(result1.nextItem(), "two");
        assertEquals(result1.nextItem(), 2);
        assertEquals(result1.nextRow(), ResultType.Inserted);
        assertEquals(result1.nextItem(), 3);
        assertEquals(result1.nextItem(), "three");
        assertEquals(result1.nextItem(), 3);
        assertEquals(result1.nextRow(), ResultType.End);        
        
        context = dbms.patchContext(first);

        dbms.apply(context, insert, 1, "uno", 1);
        dbms.apply(context, insert, 2, "dos", 2);
        dbms.apply(context, insert, 3, "tres", 3);
        dbms.apply(context, insert, 4, "quatro", 4);

        Revision second = dbms.commit(context);

        numbersReference = dbms.tableReference(numbers);

        QueryTemplate any = dbms.queryTemplate
          (list((Expression) dbms.columnReference(numbersReference, number), 
        		  (Expression) dbms.columnReference(numbersReference, name),
        		  (Expression) dbms.columnReference(numbersReference, key)),
           numbersReference,
           dbms.constant(true));

        QueryResult result2 = dbms.diff(tail, second, any);

        assertEquals(result2.nextRow(), ResultType.Inserted);
        assertEquals(result2.nextItem(), 1);
        assertEquals(result2.nextItem(), "one");
        assertEquals(result2.nextItem(), 1);
        assertEquals(result2.nextRow(), ResultType.Inserted);
        assertEquals(result2.nextItem(), 2);
        assertEquals(result2.nextItem(), "two");
        assertEquals(result2.nextItem(), 2);
        assertEquals(result2.nextRow(), ResultType.Inserted);
        assertEquals(result2.nextItem(), 3);
        assertEquals(result2.nextItem(), "three");
        assertEquals(result2.nextItem(), 3);
        assertEquals(result2.nextRow(), ResultType.Inserted);
        assertEquals(result2.nextItem(), 4);
        assertEquals(result2.nextItem(), "quatro");
        assertEquals(result2.nextItem(), 4);
        assertEquals(result2.nextRow(), ResultType.End);
        
        QueryResult result3 = dbms.diff(first, second, any);
        
        assertEquals(result3.nextRow(), ResultType.Inserted);
        assertEquals(result3.nextItem(), 4);
        assertEquals(result3.nextItem(), "quatro");
        assertEquals(result3.nextItem(), 4);
        assertEquals(result3.nextRow(), ResultType.End);
    }
    
    @Test
    public void testDuplicateInsertsMultiKeyOverwrite(){
    	DBMS dbms = new MyDBMS();

        Column number = dbms.column(Integer.class);
        Column key = dbms.column(Integer.class);
        Column name = dbms.column(String.class);
        Table numbers = dbms.table(list(number, key));

        Revision tail = dbms.revision();

        PatchTemplate insert = dbms.insertTemplate
          (numbers,
           list(number, name, key),
           list(dbms.parameter(), dbms.parameter(),
                dbms.parameter()), DuplicateKeyResolution.Overwrite);

        PatchContext context = dbms.patchContext(tail);

        dbms.apply(context, insert, 1, "one", 1);
        dbms.apply(context, insert, 2, "two", 2);
        dbms.apply(context, insert, 3, "three", 3);

        Revision first = dbms.commit(context);
        
        TableReference numbersReference = dbms.tableReference(numbers);
        QueryTemplate q1 = dbms.queryTemplate
        (list((Expression) dbms.columnReference(numbersReference, number),
        		dbms.columnReference(numbersReference, name),
        		dbms.columnReference(numbersReference, key)),
         numbersReference,
         dbms.constant(true));
        
        QueryResult result1 = dbms.diff(tail, first, q1);
        
        assertEquals(result1.nextRow(), ResultType.Inserted);
        assertEquals(result1.nextItem(), 1);
        assertEquals(result1.nextItem(), "one");
        assertEquals(result1.nextItem(), 1);
        assertEquals(result1.nextRow(), ResultType.Inserted);
        assertEquals(result1.nextItem(), 2);
        assertEquals(result1.nextItem(), "two");
        assertEquals(result1.nextItem(), 2);
        assertEquals(result1.nextRow(), ResultType.Inserted);
        assertEquals(result1.nextItem(), 3);
        assertEquals(result1.nextItem(), "three");
        assertEquals(result1.nextItem(), 3);
        assertEquals(result1.nextRow(), ResultType.End);        
        
        context = dbms.patchContext(first);

        dbms.apply(context, insert, 1, "uno", 1);
        dbms.apply(context, insert, 2, "dos", 2);
        dbms.apply(context, insert, 3, "tres", 3);
        dbms.apply(context, insert, 4, "quatro", 4);

        Revision second = dbms.commit(context);

        numbersReference = dbms.tableReference(numbers);

        QueryTemplate any = dbms.queryTemplate
          (list((Expression) dbms.columnReference(numbersReference, number), 
        		  (Expression) dbms.columnReference(numbersReference, name),
        		  (Expression) dbms.columnReference(numbersReference, key)),
           numbersReference,
           dbms.constant(true));

        QueryResult result2 = dbms.diff(tail, second, any);

        assertEquals(result2.nextRow(), ResultType.Inserted);
        assertEquals(result2.nextItem(), 1);
        assertEquals(result2.nextItem(), "uno");
        assertEquals(result2.nextItem(), 1);
        assertEquals(result2.nextRow(), ResultType.Inserted);
        assertEquals(result2.nextItem(), 2);
        assertEquals(result2.nextItem(), "dos");
        assertEquals(result2.nextItem(), 2);
        assertEquals(result2.nextRow(), ResultType.Inserted);
        assertEquals(result2.nextItem(), 3);
        assertEquals(result2.nextItem(), "tres");
        assertEquals(result2.nextItem(), 3);
        assertEquals(result2.nextRow(), ResultType.Inserted);
        assertEquals(result2.nextItem(), 4);
        assertEquals(result2.nextItem(), "quatro");
        assertEquals(result2.nextItem(), 4);
        assertEquals(result2.nextRow(), ResultType.End);
        
        QueryResult result3 = dbms.diff(first, second, any);
        
        assertEquals(result3.nextRow(), ResultType.Deleted);
        assertEquals(result3.nextItem(), 1);
        assertEquals(result3.nextItem(), "one");
        assertEquals(result3.nextItem(), 1);
        assertEquals(result3.nextRow(), ResultType.Inserted);
        assertEquals(result3.nextItem(), 1);
        assertEquals(result3.nextItem(), "uno");
        assertEquals(result3.nextItem(), 1);
        assertEquals(result3.nextRow(), ResultType.Deleted);
        assertEquals(result3.nextItem(), 2);
        assertEquals(result3.nextItem(), "two");
        assertEquals(result3.nextItem(), 2);
        assertEquals(result3.nextRow(), ResultType.Inserted);
        assertEquals(result3.nextItem(), 2);
        assertEquals(result3.nextItem(), "dos");
        assertEquals(result3.nextItem(), 2);
        assertEquals(result3.nextRow(), ResultType.Deleted);
        assertEquals(result3.nextItem(), 3);
        assertEquals(result3.nextItem(), "three");
        assertEquals(result3.nextItem(), 3);
        assertEquals(result3.nextRow(), ResultType.Inserted);
        assertEquals(result3.nextItem(), 3);
        assertEquals(result3.nextItem(), "tres");
        assertEquals(result3.nextItem(), 3);
        assertEquals(result3.nextRow(), ResultType.Inserted);
        assertEquals(result3.nextItem(), 4);
        assertEquals(result3.nextItem(), "quatro");
        assertEquals(result3.nextItem(), 4);
        assertEquals(result3.nextRow(), ResultType.End);
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
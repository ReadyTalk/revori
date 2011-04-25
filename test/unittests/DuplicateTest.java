package unittests;
import junit.framework.TestCase;

import org.junit.Test;

import static com.readytalk.oss.dbms.util.Util.list;
import static org.junit.Assert.*;

import com.readytalk.oss.dbms.DBMS;
import com.readytalk.oss.dbms.BinaryOperation;
import com.readytalk.oss.dbms.Column;
import com.readytalk.oss.dbms.DuplicateKeyException;
import com.readytalk.oss.dbms.Table;
import com.readytalk.oss.dbms.Expression;
import com.readytalk.oss.dbms.Revision;
import com.readytalk.oss.dbms.RevisionBuilder;
import com.readytalk.oss.dbms.PatchTemplate;
import com.readytalk.oss.dbms.InsertTemplate;
import com.readytalk.oss.dbms.UpdateTemplate;
import com.readytalk.oss.dbms.TableReference;
import com.readytalk.oss.dbms.ColumnReference;
import com.readytalk.oss.dbms.QueryTemplate;
import com.readytalk.oss.dbms.QueryResult;
import com.readytalk.oss.dbms.Parameter;
import com.readytalk.oss.dbms.Constant;
import com.readytalk.oss.dbms.DuplicateKeyResolution;
import com.readytalk.oss.dbms.imp.MyDBMS;

public class DuplicateTest extends TestCase{
    
    @Test
    public void testDuplicateInsertsThrowAndOverwrite(){
    	DBMS dbms = new MyDBMS();

        Column number = new Column(Integer.class);
        Column name = new Column(String.class);
        Table numbers = new Table(list(number));

        Revision tail = dbms.revision();

        PatchTemplate insert = new InsertTemplate
          (numbers,
           list(number, name),
           list((Expression) new Parameter(), new Parameter()),
           DuplicateKeyResolution.Throw);

        RevisionBuilder builder = dbms.builder(tail);

        builder.apply(insert, 1, "one");
        builder.apply(insert, 2, "two");
        builder.apply(insert, 3, "three");

        Revision first = builder.commit();

        try {
          dbms.builder(first).apply(insert, 1, "uno");
          fail("Inside testDuplicateInserts: Expected Duplicate Key Exception... ");
        } catch (DuplicateKeyException e) { }

        try {
          dbms.builder(first).apply(insert, 2, "dos");
          fail("Inside testDuplicateInserts: Expected Duplicate Key Exception... ");
        } catch (DuplicateKeyException e) { }

        try {
          dbms.builder(first).apply(insert, 3, "tres");
          fail("Inside testDuplicateInserts: Expected Duplicate Key Exception... ");
        } catch (DuplicateKeyException e) { }

        builder = dbms.builder(first);

        builder.apply(insert, 4, "quatro");

        PatchTemplate insertOrUpdate = new InsertTemplate
          (numbers,
           list(number, name),
           list((Expression) new Parameter(), new Parameter()),
           DuplicateKeyResolution.Overwrite);

        builder.apply(insertOrUpdate, 1, "uno");

        Revision second = builder.commit();

        TableReference numbersReference = new TableReference(numbers);

        QueryTemplate any = new QueryTemplate
          (list((Expression) new ColumnReference(numbersReference, name)),
           numbersReference,
           new Constant(true));

        QueryResult result = dbms.diff(tail, second, any);

        assertEquals(result.nextRow(), QueryResult.Type.Inserted);
        assertEquals(result.nextItem(), "uno");
        assertEquals(result.nextRow(), QueryResult.Type.Inserted);
        assertEquals(result.nextItem(), "two");
        assertEquals(result.nextRow(), QueryResult.Type.Inserted);
        assertEquals(result.nextItem(), "three");
        assertEquals(result.nextRow(), QueryResult.Type.Inserted);
        assertEquals(result.nextItem(), "quatro");
        assertEquals(result.nextRow(), QueryResult.Type.End);	
    }
    
    @Test
    public void testDuplicateInsertsSkip(){
    	DBMS dbms = new MyDBMS();

        Column number = new Column(Integer.class);
        Column name = new Column(String.class);
        Table numbers = new Table(list(number));

        Revision tail = dbms.revision();

        PatchTemplate insert = new InsertTemplate
          (numbers,
           list(number, name),
           list((Expression) new Parameter(), new Parameter()),
           DuplicateKeyResolution.Skip);

        RevisionBuilder builder = dbms.builder(tail);

        builder.apply(insert, 1, "one");
        builder.apply(insert, 2, "two");
        builder.apply(insert, 3, "three");

        Revision first = builder.commit();
        
        TableReference numbersReference = new TableReference(numbers);
        QueryTemplate q1 = new QueryTemplate
        (list((Expression) new ColumnReference(numbersReference, number), 
        		new ColumnReference(numbersReference, name)),
         numbersReference,
         new Constant(true));
        
        QueryResult result1 = dbms.diff(tail, first, q1);
        
        assertEquals(result1.nextRow(), QueryResult.Type.Inserted);
        assertEquals(result1.nextItem(), 1);
        assertEquals(result1.nextItem(), "one");
        assertEquals(result1.nextRow(), QueryResult.Type.Inserted);
        assertEquals(result1.nextItem(), 2);
        assertEquals(result1.nextItem(), "two");
        assertEquals(result1.nextRow(), QueryResult.Type.Inserted);
        assertEquals(result1.nextItem(), 3);
        assertEquals(result1.nextItem(), "three");
        assertEquals(result1.nextRow(), QueryResult.Type.End);        
        
        builder = dbms.builder(first);

        builder.apply(insert, 1, "uno");
        builder.apply(insert, 2, "dos");
        builder.apply(insert, 3, "tres");
        builder.apply(insert, 4, "quatro");

        Revision second = builder.commit();

        numbersReference = new TableReference(numbers);

        QueryTemplate any = new QueryTemplate
          (list((Expression) new ColumnReference(numbersReference, number), 
        		  (Expression) new ColumnReference(numbersReference, name)),
           numbersReference,
           new Constant(true));

        QueryResult result2 = dbms.diff(tail, second, any);

        assertEquals(result2.nextRow(), QueryResult.Type.Inserted);
        assertEquals(result2.nextItem(), 1);
        assertEquals(result2.nextItem(), "one");
        assertEquals(result2.nextRow(), QueryResult.Type.Inserted);
        assertEquals(result2.nextItem(), 2);
        assertEquals(result2.nextItem(), "two");
        assertEquals(result2.nextRow(), QueryResult.Type.Inserted);
        assertEquals(result2.nextItem(), 3);
        assertEquals(result2.nextItem(), "three");
        assertEquals(result2.nextRow(), QueryResult.Type.Inserted);
        assertEquals(result2.nextItem(), 4);
        assertEquals(result2.nextItem(), "quatro");
        assertEquals(result2.nextRow(), QueryResult.Type.End);
        
        QueryResult result3 = dbms.diff(first, second, any);
        
        assertEquals(result3.nextRow(), QueryResult.Type.Inserted);
        assertEquals(result3.nextItem(), 4);
        assertEquals(result3.nextItem(), "quatro");
        assertEquals(result3.nextRow(), QueryResult.Type.End);
    }
    
    @Test
    public void testDuplicateInsertsOverwrite(){
    	DBMS dbms = new MyDBMS();

        Column number = new Column(Integer.class);
        Column name = new Column(String.class);
        Table numbers = new Table(list(number));

        Revision tail = dbms.revision();

        PatchTemplate insert = new InsertTemplate
          (numbers,
           list(number, name),
           list((Expression) new Parameter(),
                new Parameter()), DuplicateKeyResolution.Overwrite);

        RevisionBuilder builder = dbms.builder(tail);

        builder.apply(insert, 1, "one");
        builder.apply(insert, 2, "two");
        builder.apply(insert, 3, "three");

        Revision first = builder.commit();
        
        TableReference numbersReference = new TableReference(numbers);
        QueryTemplate q1 = new QueryTemplate
        (list((Expression) new ColumnReference(numbersReference, number),
        		new ColumnReference(numbersReference, name)),
         numbersReference,
         new Constant(true));
        
        QueryResult result1 = dbms.diff(tail, first, q1);
        
        assertEquals(result1.nextRow(), QueryResult.Type.Inserted);
        assertEquals(result1.nextItem(), 1);
        assertEquals(result1.nextItem(), "one");
        assertEquals(result1.nextRow(), QueryResult.Type.Inserted);
        assertEquals(result1.nextItem(), 2);
        assertEquals(result1.nextItem(), "two");
        assertEquals(result1.nextRow(), QueryResult.Type.Inserted);
        assertEquals(result1.nextItem(), 3);
        assertEquals(result1.nextItem(), "three");
        assertEquals(result1.nextRow(), QueryResult.Type.End);        
        
        builder = dbms.builder(first);

        builder.apply(insert, 1, "uno");
        builder.apply(insert, 2, "dos");
        builder.apply(insert, 3, "tres");
        builder.apply(insert, 4, "quatro");

        Revision second = builder.commit();

        numbersReference = new TableReference(numbers);

        QueryTemplate any = new QueryTemplate
          (list((Expression) new ColumnReference(numbersReference, number), 
        		  (Expression) new ColumnReference(numbersReference, name)),
           numbersReference,
           new Constant(true));

        QueryResult result2 = dbms.diff(tail, second, any);

        assertEquals(result2.nextRow(), QueryResult.Type.Inserted);
        assertEquals(result2.nextItem(), 1);
        assertEquals(result2.nextItem(), "uno");
        assertEquals(result2.nextRow(), QueryResult.Type.Inserted);
        assertEquals(result2.nextItem(), 2);
        assertEquals(result2.nextItem(), "dos");
        assertEquals(result2.nextRow(), QueryResult.Type.Inserted);
        assertEquals(result2.nextItem(), 3);
        assertEquals(result2.nextItem(), "tres");
        assertEquals(result2.nextRow(), QueryResult.Type.Inserted);
        assertEquals(result2.nextItem(), 4);
        assertEquals(result2.nextItem(), "quatro");
        assertEquals(result2.nextRow(), QueryResult.Type.End);
        
        QueryResult result3 = dbms.diff(first, second, any);
        
        assertEquals(result3.nextRow(), QueryResult.Type.Deleted);
        assertEquals(result3.nextItem(), 1);
        assertEquals(result3.nextItem(), "one");
        assertEquals(result3.nextRow(), QueryResult.Type.Inserted);
        assertEquals(result3.nextItem(), 1);
        assertEquals(result3.nextItem(), "uno");
        assertEquals(result3.nextRow(), QueryResult.Type.Deleted);
        assertEquals(result3.nextItem(), 2);
        assertEquals(result3.nextItem(), "two");
        assertEquals(result3.nextRow(), QueryResult.Type.Inserted);
        assertEquals(result3.nextItem(), 2);
        assertEquals(result3.nextItem(), "dos");
        assertEquals(result3.nextRow(), QueryResult.Type.Deleted);
        assertEquals(result3.nextItem(), 3);
        assertEquals(result3.nextItem(), "three");
        assertEquals(result3.nextRow(), QueryResult.Type.Inserted);
        assertEquals(result3.nextItem(), 3);
        assertEquals(result3.nextItem(), "tres");
        assertEquals(result3.nextRow(), QueryResult.Type.Inserted);
        assertEquals(result3.nextItem(), 4);
        assertEquals(result3.nextItem(), "quatro");
        assertEquals(result3.nextRow(), QueryResult.Type.End);
    }
    
    @Test
    public void testDuplicateInsertsMultiKeyThrow(){
    	DBMS dbms = new MyDBMS();

        Column number = new Column(Integer.class);
        Column key = new Column(Integer.class);
        Column name = new Column(String.class);
        Table numbers = new Table(list(number, key));

        Revision tail = dbms.revision();

        PatchTemplate insert = new InsertTemplate
          (numbers,
           list(number, name, key),
           list((Expression) new Parameter(), new Parameter(),
                new Parameter()), DuplicateKeyResolution.Throw);

        RevisionBuilder builder = dbms.builder(tail);

        builder.apply(insert, 1, "one", 1);
        builder.apply(insert, 2, "two", 2);
        builder.apply(insert, 3, "three", 3);

        Revision first = builder.commit();
        
        TableReference numbersReference = new TableReference(numbers);
        QueryTemplate q1 = new QueryTemplate
        (list((Expression) new ColumnReference(numbersReference, number),
        		new ColumnReference(numbersReference, name),
        		new ColumnReference(numbersReference, key)),
         numbersReference,
         new Constant(true));
        
        QueryResult result1 = dbms.diff(tail, first, q1);
        
        assertEquals(result1.nextRow(), QueryResult.Type.Inserted);
        assertEquals(result1.nextItem(), 1);
        assertEquals(result1.nextItem(), "one");
        assertEquals(result1.nextItem(), 1);
        assertEquals(result1.nextRow(), QueryResult.Type.Inserted);
        assertEquals(result1.nextItem(), 2);
        assertEquals(result1.nextItem(), "two");
        assertEquals(result1.nextItem(), 2);
        assertEquals(result1.nextRow(), QueryResult.Type.Inserted);
        assertEquals(result1.nextItem(), 3);
        assertEquals(result1.nextItem(), "three");
        assertEquals(result1.nextItem(), 3);
        assertEquals(result1.nextRow(), QueryResult.Type.End);        
        
        builder = dbms.builder(first);

        try{
            builder.apply(insert, 1, "uno", 1);
            fail("Inside testDuplicateInsertsMultiKeyThrow: Expected Duplicate Key Exception...");
        }catch(DuplicateKeyException expected){}
        builder = dbms.builder(first);
        try{
            builder.apply(insert, 2, "dos", 2);
            fail("Inside testDuplicateInsertsMultiKeyThrow: Expected Duplicate Key Exception...");
        }catch(DuplicateKeyException expected){}
        builder = dbms.builder(first);
        try{
            builder.apply(insert, 3, "tres", 3);
            fail("Inside testDuplicateInsertsMultiKeyThrow: Expected Duplicate Key Exception...");
        }catch(DuplicateKeyException expected){}
        builder = dbms.builder(first);
        
        builder.apply(insert, 4, "quatro", 4);

        Revision second = builder.commit();

        numbersReference = new TableReference(numbers);

        QueryTemplate any = new QueryTemplate
          (list((Expression) new ColumnReference(numbersReference, number), 
        		  (Expression) new ColumnReference(numbersReference, name),
        		  (Expression) new ColumnReference(numbersReference, key)),
           numbersReference,
           new Constant(true));

        QueryResult result2 = dbms.diff(tail, second, any);

        assertEquals(result2.nextRow(), QueryResult.Type.Inserted);
        assertEquals(result2.nextItem(), 1);
        assertEquals(result2.nextItem(), "one");
        assertEquals(result2.nextItem(), 1);
        assertEquals(result2.nextRow(), QueryResult.Type.Inserted);
        assertEquals(result2.nextItem(), 2);
        assertEquals(result2.nextItem(), "two");
        assertEquals(result2.nextItem(), 2);
        assertEquals(result2.nextRow(), QueryResult.Type.Inserted);
        assertEquals(result2.nextItem(), 3);
        assertEquals(result2.nextItem(), "three");
        assertEquals(result2.nextItem(), 3);
        assertEquals(result2.nextRow(), QueryResult.Type.Inserted);
        assertEquals(result2.nextItem(), 4);
        assertEquals(result2.nextItem(), "quatro");
        assertEquals(result2.nextItem(), 4);
        assertEquals(result2.nextRow(), QueryResult.Type.End);
        
        QueryResult result3 = dbms.diff(first, second, any);
        
        assertEquals(result3.nextRow(), QueryResult.Type.Inserted);
        assertEquals(result3.nextItem(), 4);
        assertEquals(result3.nextItem(), "quatro");
        assertEquals(result3.nextItem(), 4);
        assertEquals(result3.nextRow(), QueryResult.Type.End);
    }
    
    @Test
    public void testDuplicateInsertsMultiKeySkip(){
    	DBMS dbms = new MyDBMS();

        Column number = new Column(Integer.class);
        Column key = new Column(Integer.class);
        Column name = new Column(String.class);
        Table numbers = new Table(list(number, key));

        Revision tail = dbms.revision();

        PatchTemplate insert = new InsertTemplate
          (numbers,
           list(number, name, key),
           list((Expression) new Parameter(), new Parameter(),
                new Parameter()), DuplicateKeyResolution.Skip);

        RevisionBuilder builder = dbms.builder(tail);

        builder.apply(insert, 1, "one", 1);
        builder.apply(insert, 2, "two", 2);
        builder.apply(insert, 3, "three", 3);

        Revision first = builder.commit();
        
        TableReference numbersReference = new TableReference(numbers);
        QueryTemplate q1 = new QueryTemplate
        (list((Expression) new ColumnReference(numbersReference, number),
        		new ColumnReference(numbersReference, name),
        		new ColumnReference(numbersReference, key)),
         numbersReference,
         new Constant(true));
        
        QueryResult result1 = dbms.diff(tail, first, q1);
        
        assertEquals(result1.nextRow(), QueryResult.Type.Inserted);
        assertEquals(result1.nextItem(), 1);
        assertEquals(result1.nextItem(), "one");
        assertEquals(result1.nextItem(), 1);
        assertEquals(result1.nextRow(), QueryResult.Type.Inserted);
        assertEquals(result1.nextItem(), 2);
        assertEquals(result1.nextItem(), "two");
        assertEquals(result1.nextItem(), 2);
        assertEquals(result1.nextRow(), QueryResult.Type.Inserted);
        assertEquals(result1.nextItem(), 3);
        assertEquals(result1.nextItem(), "three");
        assertEquals(result1.nextItem(), 3);
        assertEquals(result1.nextRow(), QueryResult.Type.End);        
        
        builder = dbms.builder(first);

        builder.apply(insert, 1, "uno", 1);
        builder.apply(insert, 2, "dos", 2);
        builder.apply(insert, 3, "tres", 3);
        builder.apply(insert, 4, "quatro", 4);

        Revision second = builder.commit();

        numbersReference = new TableReference(numbers);

        QueryTemplate any = new QueryTemplate
          (list((Expression) new ColumnReference(numbersReference, number), 
        		  (Expression) new ColumnReference(numbersReference, name),
        		  (Expression) new ColumnReference(numbersReference, key)),
           numbersReference,
           new Constant(true));

        QueryResult result2 = dbms.diff(tail, second, any);

        assertEquals(result2.nextRow(), QueryResult.Type.Inserted);
        assertEquals(result2.nextItem(), 1);
        assertEquals(result2.nextItem(), "one");
        assertEquals(result2.nextItem(), 1);
        assertEquals(result2.nextRow(), QueryResult.Type.Inserted);
        assertEquals(result2.nextItem(), 2);
        assertEquals(result2.nextItem(), "two");
        assertEquals(result2.nextItem(), 2);
        assertEquals(result2.nextRow(), QueryResult.Type.Inserted);
        assertEquals(result2.nextItem(), 3);
        assertEquals(result2.nextItem(), "three");
        assertEquals(result2.nextItem(), 3);
        assertEquals(result2.nextRow(), QueryResult.Type.Inserted);
        assertEquals(result2.nextItem(), 4);
        assertEquals(result2.nextItem(), "quatro");
        assertEquals(result2.nextItem(), 4);
        assertEquals(result2.nextRow(), QueryResult.Type.End);
        
        QueryResult result3 = dbms.diff(first, second, any);
        
        assertEquals(result3.nextRow(), QueryResult.Type.Inserted);
        assertEquals(result3.nextItem(), 4);
        assertEquals(result3.nextItem(), "quatro");
        assertEquals(result3.nextItem(), 4);
        assertEquals(result3.nextRow(), QueryResult.Type.End);
    }
    
    @Test
    public void testDuplicateInsertsMultiKeyOverwrite(){
    	DBMS dbms = new MyDBMS();

        Column number = new Column(Integer.class);
        Column key = new Column(Integer.class);
        Column name = new Column(String.class);
        Table numbers = new Table(list(number, key));

        Revision tail = dbms.revision();

        PatchTemplate insert = new InsertTemplate
          (numbers,
           list(number, name, key),
           list((Expression) new Parameter(), new Parameter(),
                new Parameter()), DuplicateKeyResolution.Overwrite);

        RevisionBuilder builder = dbms.builder(tail);

        builder.apply(insert, 1, "one", 1);
        builder.apply(insert, 2, "two", 2);
        builder.apply(insert, 3, "three", 3);

        Revision first = builder.commit();
        
        TableReference numbersReference = new TableReference(numbers);
        QueryTemplate q1 = new QueryTemplate
        (list((Expression) new ColumnReference(numbersReference, number),
        		new ColumnReference(numbersReference, name),
        		new ColumnReference(numbersReference, key)),
         numbersReference,
         new Constant(true));
        
        QueryResult result1 = dbms.diff(tail, first, q1);
        
        assertEquals(result1.nextRow(), QueryResult.Type.Inserted);
        assertEquals(result1.nextItem(), 1);
        assertEquals(result1.nextItem(), "one");
        assertEquals(result1.nextItem(), 1);
        assertEquals(result1.nextRow(), QueryResult.Type.Inserted);
        assertEquals(result1.nextItem(), 2);
        assertEquals(result1.nextItem(), "two");
        assertEquals(result1.nextItem(), 2);
        assertEquals(result1.nextRow(), QueryResult.Type.Inserted);
        assertEquals(result1.nextItem(), 3);
        assertEquals(result1.nextItem(), "three");
        assertEquals(result1.nextItem(), 3);
        assertEquals(result1.nextRow(), QueryResult.Type.End);        
        
        builder = dbms.builder(first);

        builder.apply(insert, 1, "uno", 1);
        builder.apply(insert, 2, "dos", 2);
        builder.apply(insert, 3, "tres", 3);
        builder.apply(insert, 4, "quatro", 4);

        Revision second = builder.commit();

        numbersReference = new TableReference(numbers);

        QueryTemplate any = new QueryTemplate
          (list((Expression) new ColumnReference(numbersReference, number), 
        		  (Expression) new ColumnReference(numbersReference, name),
        		  (Expression) new ColumnReference(numbersReference, key)),
           numbersReference,
           new Constant(true));

        QueryResult result2 = dbms.diff(tail, second, any);

        assertEquals(result2.nextRow(), QueryResult.Type.Inserted);
        assertEquals(result2.nextItem(), 1);
        assertEquals(result2.nextItem(), "uno");
        assertEquals(result2.nextItem(), 1);
        assertEquals(result2.nextRow(), QueryResult.Type.Inserted);
        assertEquals(result2.nextItem(), 2);
        assertEquals(result2.nextItem(), "dos");
        assertEquals(result2.nextItem(), 2);
        assertEquals(result2.nextRow(), QueryResult.Type.Inserted);
        assertEquals(result2.nextItem(), 3);
        assertEquals(result2.nextItem(), "tres");
        assertEquals(result2.nextItem(), 3);
        assertEquals(result2.nextRow(), QueryResult.Type.Inserted);
        assertEquals(result2.nextItem(), 4);
        assertEquals(result2.nextItem(), "quatro");
        assertEquals(result2.nextItem(), 4);
        assertEquals(result2.nextRow(), QueryResult.Type.End);
        
        QueryResult result3 = dbms.diff(first, second, any);
        
        assertEquals(result3.nextRow(), QueryResult.Type.Deleted);
        assertEquals(result3.nextItem(), 1);
        assertEquals(result3.nextItem(), "one");
        assertEquals(result3.nextItem(), 1);
        assertEquals(result3.nextRow(), QueryResult.Type.Inserted);
        assertEquals(result3.nextItem(), 1);
        assertEquals(result3.nextItem(), "uno");
        assertEquals(result3.nextItem(), 1);
        assertEquals(result3.nextRow(), QueryResult.Type.Deleted);
        assertEquals(result3.nextItem(), 2);
        assertEquals(result3.nextItem(), "two");
        assertEquals(result3.nextItem(), 2);
        assertEquals(result3.nextRow(), QueryResult.Type.Inserted);
        assertEquals(result3.nextItem(), 2);
        assertEquals(result3.nextItem(), "dos");
        assertEquals(result3.nextItem(), 2);
        assertEquals(result3.nextRow(), QueryResult.Type.Deleted);
        assertEquals(result3.nextItem(), 3);
        assertEquals(result3.nextItem(), "three");
        assertEquals(result3.nextItem(), 3);
        assertEquals(result3.nextRow(), QueryResult.Type.Inserted);
        assertEquals(result3.nextItem(), 3);
        assertEquals(result3.nextItem(), "tres");
        assertEquals(result3.nextItem(), 3);
        assertEquals(result3.nextRow(), QueryResult.Type.Inserted);
        assertEquals(result3.nextItem(), 4);
        assertEquals(result3.nextItem(), "quatro");
        assertEquals(result3.nextItem(), 4);
        assertEquals(result3.nextRow(), QueryResult.Type.End);
    }
    

    @Test
    public void testDuplicateUpdates(){
    	DBMS dbms = new MyDBMS();

        Column number = new Column(Integer.class);
        Column name = new Column(String.class);
        Table numbers = new Table(list(number));

        Revision tail = dbms.revision();

        PatchTemplate insert = new InsertTemplate
          (numbers,
           list(number, name),
           list((Expression) new Parameter(), new Parameter()),
           DuplicateKeyResolution.Throw);

        RevisionBuilder builder = dbms.builder(tail);

        builder.apply(insert, 1, "one");
        builder.apply(insert, 2, "two");
        builder.apply(insert, 3, "three");

        Revision first = builder.commit();

        TableReference numbersReference = new TableReference(numbers);

        PatchTemplate updateNumberWhereNumberEqual = new UpdateTemplate
          (numbersReference,
           new BinaryOperation
           (BinaryOperation.Type.Equal,
            new ColumnReference(numbersReference, number),
            new Parameter()),
           list(number),
           list((Expression) new Parameter()));

        try {
          dbms.builder(first).apply(updateNumberWhereNumberEqual, 1, 2);
          throw new RuntimeException();
        } catch (DuplicateKeyException e) { }

        try {
          dbms.builder(first).apply(updateNumberWhereNumberEqual, 2, 3);
          throw new RuntimeException();
        } catch (DuplicateKeyException e) { }

        builder = dbms.builder(first);

        builder.apply(updateNumberWhereNumberEqual, 3, 3);
        builder.apply(updateNumberWhereNumberEqual, 4, 2);
        builder.apply(updateNumberWhereNumberEqual, 3, 4);

        Revision second = builder.commit();

        QueryTemplate any = new QueryTemplate
          (list((Expression) new ColumnReference(numbersReference, number),
                (Expression) new ColumnReference(numbersReference, name)),
           numbersReference,
           new Constant(true));

        QueryResult result = dbms.diff(tail, second, any);

        assertEquals(result.nextRow(), QueryResult.Type.Inserted);
        assertEquals(result.nextItem(), 1);
        assertEquals(result.nextItem(), "one");
        assertEquals(result.nextRow(), QueryResult.Type.Inserted);
        assertEquals(result.nextItem(), 2);
        assertEquals(result.nextItem(), "two");
        assertEquals(result.nextRow(), QueryResult.Type.Inserted);
        assertEquals(result.nextItem(), 4);
        assertEquals(result.nextItem(), "three");
        assertEquals(result.nextRow(), QueryResult.Type.End);
    	
    }

}

package unittests;

import junit.framework.TestCase;

import org.junit.Test;

import static com.readytalk.oss.dbms.util.Util.list;
import static org.junit.Assert.*;

import com.readytalk.oss.dbms.DBMS;
import com.readytalk.oss.dbms.BinaryOperation;
import com.readytalk.oss.dbms.Column;
import com.readytalk.oss.dbms.Table;
import com.readytalk.oss.dbms.Expression;
import com.readytalk.oss.dbms.Revision;
import com.readytalk.oss.dbms.RevisionBuilder;
import com.readytalk.oss.dbms.PatchTemplate;
import com.readytalk.oss.dbms.InsertTemplate;
import com.readytalk.oss.dbms.UpdateTemplate;
import com.readytalk.oss.dbms.DeleteTemplate;
import com.readytalk.oss.dbms.TableReference;
import com.readytalk.oss.dbms.QueryTemplate;
import com.readytalk.oss.dbms.QueryResult;
import com.readytalk.oss.dbms.Parameter;
import com.readytalk.oss.dbms.Constant;
import com.readytalk.oss.dbms.ColumnReference;
import com.readytalk.oss.dbms.DuplicateKeyResolution;
import com.readytalk.oss.dbms.imp.MyDBMS;


public class GeneralTests extends TestCase{
    @Test
    public void testSimpleInsertDiffs(){
        DBMS dbms = new MyDBMS();
        
        Column number = new Column(Integer.class);
        Column name = new Column(String.class);
        Table numbers = new Table(list(number));
        
        Revision tail = dbms.revision();
        
        RevisionBuilder builder = dbms.builder(tail);
        
        PatchTemplate insert = new InsertTemplate
        (numbers,
         list(number, name),
         list((Expression) new Parameter(), new Parameter()),
         DuplicateKeyResolution.Throw);
        
        builder.apply(insert, 42, "forty two");
        
        Revision first = builder.commit();
        
        TableReference numbersReference = new TableReference(numbers);
        QueryTemplate equal = new QueryTemplate
        (list((Expression) new ColumnReference(numbersReference, name)),
                numbersReference,
                new BinaryOperation
                (BinaryOperation.Type.Equal,
                        new ColumnReference(numbersReference, number),
                        new Parameter()));
        
        QueryResult result = dbms.diff(tail, first, equal, 42);
        
        assertEquals(result.nextRow(), QueryResult.Type.Inserted);
        assertEquals(result.nextItem(), "forty two");
        assertEquals(result.nextRow(), QueryResult.Type.End);
        
        result = dbms.diff(first, tail, equal, 42);
        
        assertEquals(result.nextRow(), QueryResult.Type.Deleted);
        assertEquals(result.nextItem(), "forty two");
        assertEquals(result.nextRow(), QueryResult.Type.End);
        
        result = dbms.diff(tail, first, equal, 43);
        
        assertEquals(result.nextRow(), QueryResult.Type.End);
        
        result = dbms.diff(tail, tail, equal, 42);
        
        assertEquals(result.nextRow(), QueryResult.Type.End);
        
        result = dbms.diff(first, first, equal, 42);
        
        assertEquals(result.nextRow(), QueryResult.Type.End);
        }
    @Test
    public void testLargerInsertDiffs(){
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
        
        builder.apply(insert, 42, "forty two");
        builder.apply(insert, 43, "forty three");
        builder.apply(insert, 44, "forty four");
        builder.apply(insert,  2, "two");
        builder.apply(insert, 65, "sixty five");
        builder.apply(insert,  8, "eight");
        
        Revision first = builder.commit();
        
        TableReference numbersReference = new TableReference(numbers);

        QueryTemplate equal = new QueryTemplate
        (list((Expression) new ColumnReference(numbersReference, name)),
                numbersReference,
                new BinaryOperation
                (BinaryOperation.Type.Equal,
                        new ColumnReference(numbersReference, number),
                        new Parameter()));
        
        QueryResult result = dbms.diff(tail, first, equal, 42);
        
        assertEquals(result.nextRow(), QueryResult.Type.Inserted);
        assertEquals(result.nextItem(), "forty two");
        assertEquals(result.nextRow(), QueryResult.Type.End);
        result = dbms.diff(first, tail, equal, 42);
        
        assertEquals(result.nextRow(), QueryResult.Type.Deleted);
        assertEquals(result.nextItem(), "forty two");
        assertEquals(result.nextRow(), QueryResult.Type.End);
        result = dbms.diff(tail, first, equal, 43);
        
        assertEquals(result.nextRow(), QueryResult.Type.Inserted);
        assertEquals(result.nextItem(), "forty three");
        assertEquals(result.nextRow(), QueryResult.Type.End);
        result = dbms.diff(tail, tail, equal, 42);
        
        assertEquals(result.nextRow(), QueryResult.Type.End);
        
        result = dbms.diff(first, first, equal, 42);
        
        assertEquals(result.nextRow(), QueryResult.Type.End);
        
        builder = dbms.builder(first);
        
        builder.apply(insert, 1, "one");
        builder.apply(insert, 3, "three");
        builder.apply(insert, 5, "five");
        builder.apply(insert, 6, "six");
        
        Revision second = builder.commit();
        
        result = dbms.diff(tail, second, equal, 43);
        
        assertEquals(result.nextRow(), QueryResult.Type.Inserted);
        assertEquals(result.nextItem(), "forty three");
        assertEquals(result.nextRow(), QueryResult.Type.End);
        
        result = dbms.diff(first, second, equal, 43);
        
        assertEquals(result.nextRow(), QueryResult.Type.End);
        
        result = dbms.diff(first, second, equal, 5);
        
        assertEquals(result.nextRow(), QueryResult.Type.Inserted);
        assertEquals(result.nextItem(), "five");
        assertEquals(result.nextRow(), QueryResult.Type.End);
        
        result = dbms.diff(tail, first, equal, 5);
        
        assertEquals(result.nextRow(), QueryResult.Type.End);
        
        result = dbms.diff(second, first, equal, 5);
        
        assertEquals(result.nextRow(), QueryResult.Type.Deleted);
        assertEquals(result.nextItem(), "five");
        assertEquals(result.nextRow(), QueryResult.Type.End);
        }

    @Test
    public void testDeleteDiffs(){
    
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

    builder.apply(insert, 42, "forty two");
    builder.apply(insert, 43, "forty three");
    builder.apply(insert, 44, "forty four");
    builder.apply(insert,  2, "two");
    builder.apply(insert, 65, "sixty five");
    builder.apply(insert,  8, "eight");

    Revision first = builder.commit();

    TableReference numbersReference = new TableReference(numbers);
    QueryTemplate equal = new QueryTemplate
      (list((Expression) new ColumnReference(numbersReference, name)),
       numbersReference,
       new BinaryOperation
       (BinaryOperation.Type.Equal,
        new ColumnReference(numbersReference, number),
        new Parameter()));

    PatchTemplate delete = new DeleteTemplate
      (numbersReference,
       new BinaryOperation
       (BinaryOperation.Type.Equal,
        new ColumnReference(numbersReference, number),
        new Parameter()));

    builder = dbms.builder(first);

    builder.apply(delete, 43);

    Revision second = builder.commit();

    QueryResult result = dbms.diff(first, second, equal, 43);

    assertEquals(result.nextRow(), QueryResult.Type.Deleted);
    assertEquals(result.nextItem(), "forty three");
    assertEquals(result.nextRow(), QueryResult.Type.End);

    result = dbms.diff(second, first, equal, 43);

    assertEquals(result.nextRow(), QueryResult.Type.Inserted);
    assertEquals(result.nextItem(), "forty three");
    assertEquals(result.nextRow(), QueryResult.Type.End);

    result = dbms.diff(tail, second, equal, 43);

    assertEquals(result.nextRow(), QueryResult.Type.End);
    
    result = dbms.diff(first, second, equal, 42);

    assertEquals(result.nextRow(), QueryResult.Type.End);
    
    result = dbms.diff(tail, second, equal, 42);

    assertEquals(result.nextRow(), QueryResult.Type.Inserted);
    assertEquals(result.nextItem(), "forty two");
    assertEquals(result.nextRow(), QueryResult.Type.End);
    
    result = dbms.diff(second, tail, equal, 42);

    assertEquals(result.nextRow(), QueryResult.Type.Deleted);
    assertEquals(result.nextItem(), "forty two");
    assertEquals(result.nextRow(), QueryResult.Type.End);

    builder = dbms.builder(first);

    builder.apply(delete, 43);
    builder.apply(delete, 42);
    builder.apply(delete, 65);

    second = builder.commit();

    result = dbms.diff(first, second, equal, 43);

    assertEquals(result.nextRow(), QueryResult.Type.Deleted);
    assertEquals(result.nextItem(), "forty three");
    assertEquals(result.nextRow(), QueryResult.Type.End);

    result = dbms.diff(second, first, equal, 43);

    assertEquals(result.nextRow(), QueryResult.Type.Inserted);
    assertEquals(result.nextItem(), "forty three");
    assertEquals(result.nextRow(), QueryResult.Type.End);

    result = dbms.diff(first, second, equal, 42);

    assertEquals(result.nextRow(), QueryResult.Type.Deleted);
    assertEquals(result.nextItem(), "forty two");
    assertEquals(result.nextRow(), QueryResult.Type.End);

    result = dbms.diff(second, first, equal, 42);

    assertEquals(result.nextRow(), QueryResult.Type.Inserted);
    assertEquals(result.nextItem(), "forty two");
    assertEquals(result.nextRow(), QueryResult.Type.End);

    result = dbms.diff(first, second, equal, 65);

    assertEquals(result.nextRow(), QueryResult.Type.Deleted);
    assertEquals(result.nextItem(), "sixty five");
    assertEquals(result.nextRow(), QueryResult.Type.End);

    result = dbms.diff(second, first, equal, 65);

    assertEquals(result.nextRow(), QueryResult.Type.Inserted);
    assertEquals(result.nextItem(), "sixty five");
    assertEquals(result.nextRow(), QueryResult.Type.End);
    
    result = dbms.diff(first, second, equal, 2);

    assertEquals(result.nextRow(), QueryResult.Type.End);
    
    result = dbms.diff(second, first, equal, 2);

    assertEquals(result.nextRow(), QueryResult.Type.End);

    builder = dbms.builder(second);

    builder.apply(delete, 44);
    builder.apply(delete,  2);
    builder.apply(delete,  8);

    Revision third = builder.commit();

    result = dbms.diff(second, third, equal, 44);

    assertEquals(result.nextRow(), QueryResult.Type.Deleted);
    assertEquals(result.nextItem(), "forty four");
    assertEquals(result.nextRow(), QueryResult.Type.End);

    result = dbms.diff(third, second, equal, 44);

    assertEquals(result.nextRow(), QueryResult.Type.Inserted);
    assertEquals(result.nextItem(), "forty four");
    assertEquals(result.nextRow(), QueryResult.Type.End);

    result = dbms.diff(tail, third, equal, 44);

    assertEquals(result.nextRow(), QueryResult.Type.End);

    result = dbms.diff(tail, third, equal, 42);

    assertEquals(result.nextRow(), QueryResult.Type.End);
    	
    }
    
    @Test
    public void testUpdateDiffs(){
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

        builder.apply(insert,  1, "one");
        builder.apply(insert,  2, "two");
        builder.apply(insert,  3, "three");
        builder.apply(insert,  4, "four");
        builder.apply(insert,  5, "five");
        builder.apply(insert,  6, "six");
        builder.apply(insert,  7, "seven");
        builder.apply(insert,  8, "eight");
        builder.apply(insert,  9, "nine");
        builder.apply(insert, 10, "ten");
        builder.apply(insert, 11, "eleven");
        builder.apply(insert, 12, "twelve");
        builder.apply(insert, 13, "thirteen");

        Revision first = builder.commit();

        TableReference numbersReference = new TableReference(numbers);

        PatchTemplate update = new UpdateTemplate
        (numbersReference,
         new BinaryOperation
         (BinaryOperation.Type.Equal,
          new ColumnReference(numbersReference, number),
          new Parameter()),
         list(name),
         list((Expression) new Parameter()));

        builder = dbms.builder(first);

        builder.apply(update, 1, "ichi");

        Revision second = builder.commit();

        QueryTemplate equal = new QueryTemplate
        (list((Expression) new ColumnReference(numbersReference, name)),
                numbersReference,
                new BinaryOperation
                (BinaryOperation.Type.Equal,
                        new ColumnReference(numbersReference, number),
                        new Parameter()));

        QueryResult result = dbms.diff(first, second, equal, 1);

        assertEquals(result.nextRow(), QueryResult.Type.Deleted);
        assertEquals(result.nextItem(), "one");
        assertEquals(result.nextRow(), QueryResult.Type.Inserted);
        assertEquals(result.nextItem(), "ichi");
        assertEquals(result.nextRow(), QueryResult.Type.End);

        result = dbms.diff(second, first, equal, 1);

        assertEquals(result.nextRow(), QueryResult.Type.Deleted);
        assertEquals(result.nextItem(), "ichi");
        assertEquals(result.nextRow(), QueryResult.Type.Inserted);
        assertEquals(result.nextItem(), "one");
        assertEquals(result.nextRow(), QueryResult.Type.End);

        result = dbms.diff(first, second, equal, 2);

        assertEquals(result.nextRow(), QueryResult.Type.End);

        result = dbms.diff(tail, first, equal, 1);

        assertEquals(result.nextRow(), QueryResult.Type.Inserted);
        assertEquals(result.nextItem(), "one");
        assertEquals(result.nextRow(), QueryResult.Type.End);

        result = dbms.diff(tail, second, equal, 1);

        assertEquals(result.nextRow(), QueryResult.Type.Inserted);
        assertEquals(result.nextItem(), "ichi");
        assertEquals(result.nextRow(), QueryResult.Type.End);

        builder = dbms.builder(second);

        builder.apply(update, 11, "ju ichi");
        builder.apply(update,  6, "roku");
        builder.apply(update,  7, "shichi");

        Revision third = builder.commit();

        QueryTemplate any = new QueryTemplate
        (list((Expression) new ColumnReference(numbersReference, name)),
                numbersReference,
                new Constant(true));

        result = dbms.diff(second, third, any);

        assertEquals(result.nextRow(), QueryResult.Type.Deleted);
        assertEquals(result.nextItem(), "six");
        assertEquals(result.nextRow(), QueryResult.Type.Inserted);
        assertEquals(result.nextItem(), "roku");
        assertEquals(result.nextRow(), QueryResult.Type.Deleted);
        assertEquals(result.nextItem(), "seven");
        assertEquals(result.nextRow(), QueryResult.Type.Inserted);
        assertEquals(result.nextItem(), "shichi");
        assertEquals(result.nextRow(), QueryResult.Type.Deleted);
        assertEquals(result.nextItem(), "eleven");
        assertEquals(result.nextRow(), QueryResult.Type.Inserted);
        assertEquals(result.nextItem(), "ju ichi");
        assertEquals(result.nextRow(), QueryResult.Type.End);
    }
    
    @Test
    public void testNonIndexedQueries(){
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

        builder.apply(insert,  1, "one");
        builder.apply(insert,  2, "two");
        builder.apply(insert,  3, "three");
        builder.apply(insert,  4, "four");
        builder.apply(insert,  5, "five");
        builder.apply(insert,  6, "six");
        builder.apply(insert,  7, "seven");
        builder.apply(insert,  8, "eight");
        builder.apply(insert,  9, "nine");
        builder.apply(insert, 10, "ten");
        builder.apply(insert, 11, "eleven");
        builder.apply(insert, 12, "twelve");
        builder.apply(insert, 13, "thirteen");

        Revision first = builder.commit();

        TableReference numbersReference = new TableReference(numbers);

        QueryTemplate nameEqual = new QueryTemplate
          (list((Expression) new ColumnReference(numbersReference, number),
                (Expression) new ColumnReference(numbersReference, name)),
           numbersReference,
           new BinaryOperation
           (BinaryOperation.Type.Equal,
            new ColumnReference(numbersReference, name),
            new Parameter()));

        QueryResult result = dbms.diff(tail, first, nameEqual, "nine");

        assertEquals(result.nextRow(), QueryResult.Type.Inserted);
        assertEquals(result.nextItem(), 9);
        assertEquals(result.nextItem(), "nine");
        assertEquals(result.nextRow(), QueryResult.Type.End);

        result = dbms.diff(first, tail, nameEqual, "nine");

        assertEquals(result.nextRow(), QueryResult.Type.Deleted);
        assertEquals(result.nextItem(), 9);
        assertEquals(result.nextItem(), "nine");
        assertEquals(result.nextRow(), QueryResult.Type.End);

        QueryTemplate nameLessThan = new QueryTemplate
          (list((Expression) new ColumnReference(numbersReference, number),
                (Expression) new ColumnReference(numbersReference, name)),
           numbersReference,
           new BinaryOperation
           (BinaryOperation.Type.LessThan,
            new ColumnReference(numbersReference, name),
            new Parameter()));

        result = dbms.diff(tail, first, nameLessThan, "nine");

        assertEquals(result.nextRow(), QueryResult.Type.Inserted);
        assertEquals(result.nextItem(), 4);
        assertEquals(result.nextItem(), "four");
        assertEquals(result.nextRow(), QueryResult.Type.Inserted);
        assertEquals(result.nextItem(), 5);
        assertEquals(result.nextItem(), "five");
        assertEquals(result.nextRow(), QueryResult.Type.Inserted);
        assertEquals(result.nextItem(), 8);
        assertEquals(result.nextItem(), "eight");
        assertEquals(result.nextRow(), QueryResult.Type.Inserted);
        assertEquals(result.nextItem(), 11);
        assertEquals(result.nextItem(), "eleven");
        assertEquals(result.nextRow(), QueryResult.Type.End);
    }
    
    @Test
    public void testIndexedColumnUpdates(){
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

        builder = dbms.builder(first);

        builder.apply(updateNumberWhereNumberEqual, 3, 4);

        Revision second = builder.commit();

        QueryTemplate numberEqual = new QueryTemplate
          (list((Expression) new ColumnReference(numbersReference, name)),
           numbersReference,
           new BinaryOperation
           (BinaryOperation.Type.Equal,
            new ColumnReference(numbersReference, number),
            new Parameter()));

        QueryResult result = dbms.diff(tail, second, numberEqual, 4);

        assertEquals(result.nextRow(), QueryResult.Type.Inserted);
        assertEquals(result.nextItem(), "three");
        assertEquals(result.nextRow(), QueryResult.Type.End);

        result = dbms.diff(tail, second, numberEqual, 3);

        assertEquals(result.nextRow(), QueryResult.Type.End);
    }
}

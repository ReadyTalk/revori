package unittests;

import junit.framework.TestCase;

import org.junit.Test;

import static com.readytalk.oss.dbms.imp.Util.list;
import static org.junit.Assert.*;

import com.readytalk.oss.dbms.DBMS;
import com.readytalk.oss.dbms.DBMS.BinaryOperationType;
import com.readytalk.oss.dbms.DBMS.Column;
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


public class GeneralTests extends TestCase{
    @Test
    public void testSimpleInsertDiffs(){
        DBMS dbms = new MyDBMS();
        
        Column number = dbms.column(Integer.class);
        Column name = dbms.column(String.class);
        Table numbers = dbms.table(list(number));
        
        Revision tail = dbms.revision();
        
        PatchContext context = dbms.patchContext(tail);
        
        PatchTemplate insert = dbms.insertTemplate
        (numbers,
                list(number, name),
                list(dbms.parameter(),
                        dbms.parameter()), DuplicateKeyResolution.Throw);
        
        dbms.apply(context, insert, 42, "forty two");
        
        Revision first = dbms.commit(context);
        
        TableReference numbersReference = dbms.tableReference(numbers);
        QueryTemplate equal = dbms.queryTemplate
        (list((Expression) dbms.columnReference(numbersReference, name)),
                numbersReference,
                dbms.operation
                (BinaryOperationType.Equal,
                        dbms.columnReference(numbersReference, number),
                        dbms.parameter()));
        
        QueryResult result = dbms.diff(tail, first, equal, 42);
        
        assertEquals(result.nextRow(), ResultType.Inserted);
        assertEquals(result.nextItem(), "forty two");
        assertEquals(result.nextRow(), ResultType.End);
        
        result = dbms.diff(first, tail, equal, 42);
        
        assertEquals(result.nextRow(), ResultType.Deleted);
        assertEquals(result.nextItem(), "forty two");
        assertEquals(result.nextRow(), ResultType.End);
        
        result = dbms.diff(tail, first, equal, 43);
        
        assertEquals(result.nextRow(), ResultType.End);
        
        result = dbms.diff(tail, tail, equal, 42);
        
        assertEquals(result.nextRow(), ResultType.End);
        
        result = dbms.diff(first, first, equal, 42);
        
        assertEquals(result.nextRow(), ResultType.End);
        }
    @Test
    public void testLargerInsertDiffs(){
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
        
        dbms.apply(context, insert, 42, "forty two");
        dbms.apply(context, insert, 43, "forty three");
        dbms.apply(context, insert, 44, "forty four");
        dbms.apply(context, insert,  2, "two");
        dbms.apply(context, insert, 65, "sixty five");
        dbms.apply(context, insert,  8, "eight");
        
        Revision first = dbms.commit(context);
        
        TableReference numbersReference = dbms.tableReference(numbers);

        QueryTemplate equal = dbms.queryTemplate
        (list((Expression) dbms.columnReference(numbersReference, name)),
                numbersReference,
                dbms.operation
                (BinaryOperationType.Equal,
                        dbms.columnReference(numbersReference, number),
                        dbms.parameter()));
        
        QueryResult result = dbms.diff(tail, first, equal, 42);
        
        assertEquals(result.nextRow(), ResultType.Inserted);
        assertEquals(result.nextItem(), "forty two");
        assertEquals(result.nextRow(), ResultType.End);
        result = dbms.diff(first, tail, equal, 42);
        
        assertEquals(result.nextRow(), ResultType.Deleted);
        assertEquals(result.nextItem(), "forty two");
        assertEquals(result.nextRow(), ResultType.End);
        result = dbms.diff(tail, first, equal, 43);
        
        assertEquals(result.nextRow(), ResultType.Inserted);
        assertEquals(result.nextItem(), "forty three");
        assertEquals(result.nextRow(), ResultType.End);
        result = dbms.diff(tail, tail, equal, 42);
        
        assertEquals(result.nextRow(), ResultType.End);
        
        result = dbms.diff(first, first, equal, 42);
        
        assertEquals(result.nextRow(), ResultType.End);
        
        context = dbms.patchContext(first);
        
        dbms.apply(context, insert, 1, "one");
        dbms.apply(context, insert, 3, "three");
        dbms.apply(context, insert, 5, "five");
        dbms.apply(context, insert, 6, "six");
        
        Revision second = dbms.commit(context);
        
        result = dbms.diff(tail, second, equal, 43);
        
        assertEquals(result.nextRow(), ResultType.Inserted);
        assertEquals(result.nextItem(), "forty three");
        assertEquals(result.nextRow(), ResultType.End);
        
        result = dbms.diff(first, second, equal, 43);
        
        assertEquals(result.nextRow(), ResultType.End);
        
        result = dbms.diff(first, second, equal, 5);
        
        assertEquals(result.nextRow(), ResultType.Inserted);
        assertEquals(result.nextItem(), "five");
        assertEquals(result.nextRow(), ResultType.End);
        
        result = dbms.diff(tail, first, equal, 5);
        
        assertEquals(result.nextRow(), ResultType.End);
        
        result = dbms.diff(second, first, equal, 5);
        
        assertEquals(result.nextRow(), ResultType.Deleted);
        assertEquals(result.nextItem(), "five");
        assertEquals(result.nextRow(), ResultType.End);
        }

    @Test
    public void testDeleteDiffs(){
    
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

    dbms.apply(context, insert, 42, "forty two");
    dbms.apply(context, insert, 43, "forty three");
    dbms.apply(context, insert, 44, "forty four");
    dbms.apply(context, insert,  2, "two");
    dbms.apply(context, insert, 65, "sixty five");
    dbms.apply(context, insert,  8, "eight");

    Revision first = dbms.commit(context);

    TableReference numbersReference = dbms.tableReference(numbers);
    QueryTemplate equal = dbms.queryTemplate
      (list((Expression) dbms.columnReference(numbersReference, name)),
       numbersReference,
       dbms.operation
       (BinaryOperationType.Equal,
        dbms.columnReference(numbersReference, number),
        dbms.parameter()));

    PatchTemplate delete = dbms.deleteTemplate
      (numbersReference,
       dbms.operation
       (BinaryOperationType.Equal,
        dbms.columnReference(numbersReference, number),
        dbms.parameter()));

    context = dbms.patchContext(first);

    dbms.apply(context, delete, 43);

    Revision second = dbms.commit(context);

    QueryResult result = dbms.diff(first, second, equal, 43);

    assertEquals(result.nextRow(), ResultType.Deleted);
    assertEquals(result.nextItem(), "forty three");
    assertEquals(result.nextRow(), ResultType.End);

    result = dbms.diff(second, first, equal, 43);

    assertEquals(result.nextRow(), ResultType.Inserted);
    assertEquals(result.nextItem(), "forty three");
    assertEquals(result.nextRow(), ResultType.End);

    result = dbms.diff(tail, second, equal, 43);

    assertEquals(result.nextRow(), ResultType.End);
    
    result = dbms.diff(first, second, equal, 42);

    assertEquals(result.nextRow(), ResultType.End);
    
    result = dbms.diff(tail, second, equal, 42);

    assertEquals(result.nextRow(), ResultType.Inserted);
    assertEquals(result.nextItem(), "forty two");
    assertEquals(result.nextRow(), ResultType.End);
    
    result = dbms.diff(second, tail, equal, 42);

    assertEquals(result.nextRow(), ResultType.Deleted);
    assertEquals(result.nextItem(), "forty two");
    assertEquals(result.nextRow(), ResultType.End);

    context = dbms.patchContext(first);

    dbms.apply(context, delete, 43);
    dbms.apply(context, delete, 42);
    dbms.apply(context, delete, 65);

    second = dbms.commit(context);

    result = dbms.diff(first, second, equal, 43);

    assertEquals(result.nextRow(), ResultType.Deleted);
    assertEquals(result.nextItem(), "forty three");
    assertEquals(result.nextRow(), ResultType.End);

    result = dbms.diff(second, first, equal, 43);

    assertEquals(result.nextRow(), ResultType.Inserted);
    assertEquals(result.nextItem(), "forty three");
    assertEquals(result.nextRow(), ResultType.End);

    result = dbms.diff(first, second, equal, 42);

    assertEquals(result.nextRow(), ResultType.Deleted);
    assertEquals(result.nextItem(), "forty two");
    assertEquals(result.nextRow(), ResultType.End);

    result = dbms.diff(second, first, equal, 42);

    assertEquals(result.nextRow(), ResultType.Inserted);
    assertEquals(result.nextItem(), "forty two");
    assertEquals(result.nextRow(), ResultType.End);

    result = dbms.diff(first, second, equal, 65);

    assertEquals(result.nextRow(), ResultType.Deleted);
    assertEquals(result.nextItem(), "sixty five");
    assertEquals(result.nextRow(), ResultType.End);

    result = dbms.diff(second, first, equal, 65);

    assertEquals(result.nextRow(), ResultType.Inserted);
    assertEquals(result.nextItem(), "sixty five");
    assertEquals(result.nextRow(), ResultType.End);
    
    result = dbms.diff(first, second, equal, 2);

    assertEquals(result.nextRow(), ResultType.End);
    
    result = dbms.diff(second, first, equal, 2);

    assertEquals(result.nextRow(), ResultType.End);

    context = dbms.patchContext(second);

    dbms.apply(context, delete, 44);
    dbms.apply(context, delete,  2);
    dbms.apply(context, delete,  8);

    Revision third = dbms.commit(context);

    result = dbms.diff(second, third, equal, 44);

    assertEquals(result.nextRow(), ResultType.Deleted);
    assertEquals(result.nextItem(), "forty four");
    assertEquals(result.nextRow(), ResultType.End);

    result = dbms.diff(third, second, equal, 44);

    assertEquals(result.nextRow(), ResultType.Inserted);
    assertEquals(result.nextItem(), "forty four");
    assertEquals(result.nextRow(), ResultType.End);

    result = dbms.diff(tail, third, equal, 44);

    assertEquals(result.nextRow(), ResultType.End);

    result = dbms.diff(tail, third, equal, 42);

    assertEquals(result.nextRow(), ResultType.End);
    	
    }
    
    @Test
    public void testUpdateDiffs(){
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

        dbms.apply(context, insert,  1, "one");
        dbms.apply(context, insert,  2, "two");
        dbms.apply(context, insert,  3, "three");
        dbms.apply(context, insert,  4, "four");
        dbms.apply(context, insert,  5, "five");
        dbms.apply(context, insert,  6, "six");
        dbms.apply(context, insert,  7, "seven");
        dbms.apply(context, insert,  8, "eight");
        dbms.apply(context, insert,  9, "nine");
        dbms.apply(context, insert, 10, "ten");
        dbms.apply(context, insert, 11, "eleven");
        dbms.apply(context, insert, 12, "twelve");
        dbms.apply(context, insert, 13, "thirteen");

        Revision first = dbms.commit(context);

        TableReference numbersReference = dbms.tableReference(numbers);

        PatchTemplate update = dbms.updateTemplate
        (numbersReference,
                dbms.operation
                (BinaryOperationType.Equal,
                        dbms.columnReference(numbersReference, number),
                        dbms.parameter()),
                        list(name),
                        list(dbms.parameter()));

        context = dbms.patchContext(first);

        dbms.apply(context, update, 1, "ichi");

        Revision second = dbms.commit(context);

        QueryTemplate equal = dbms.queryTemplate
        (list((Expression) dbms.columnReference(numbersReference, name)),
                numbersReference,
                dbms.operation
                (BinaryOperationType.Equal,
                        dbms.columnReference(numbersReference, number),
                        dbms.parameter()));

        QueryResult result = dbms.diff(first, second, equal, 1);

        assertEquals(result.nextRow(), ResultType.Deleted);
        assertEquals(result.nextItem(), "one");
        assertEquals(result.nextRow(), ResultType.Inserted);
        assertEquals(result.nextItem(), "ichi");
        assertEquals(result.nextRow(), ResultType.End);

        result = dbms.diff(second, first, equal, 1);

        assertEquals(result.nextRow(), ResultType.Deleted);
        assertEquals(result.nextItem(), "ichi");
        assertEquals(result.nextRow(), ResultType.Inserted);
        assertEquals(result.nextItem(), "one");
        assertEquals(result.nextRow(), ResultType.End);

        result = dbms.diff(first, second, equal, 2);

        assertEquals(result.nextRow(), ResultType.End);

        result = dbms.diff(tail, first, equal, 1);

        assertEquals(result.nextRow(), ResultType.Inserted);
        assertEquals(result.nextItem(), "one");
        assertEquals(result.nextRow(), ResultType.End);

        result = dbms.diff(tail, second, equal, 1);

        assertEquals(result.nextRow(), ResultType.Inserted);
        assertEquals(result.nextItem(), "ichi");
        assertEquals(result.nextRow(), ResultType.End);

        context = dbms.patchContext(second);

        dbms.apply(context, update, 11, "ju ichi");
        dbms.apply(context, update,  6, "roku");
        dbms.apply(context, update,  7, "shichi");

        Revision third = dbms.commit(context);

        QueryTemplate any = dbms.queryTemplate
        (list((Expression) dbms.columnReference(numbersReference, name)),
                numbersReference,
                dbms.constant(true));

        result = dbms.diff(second, third, any);

        assertEquals(result.nextRow(), ResultType.Deleted);
        assertEquals(result.nextItem(), "six");
        assertEquals(result.nextRow(), ResultType.Inserted);
        assertEquals(result.nextItem(), "roku");
        assertEquals(result.nextRow(), ResultType.Deleted);
        assertEquals(result.nextItem(), "seven");
        assertEquals(result.nextRow(), ResultType.Inserted);
        assertEquals(result.nextItem(), "shichi");
        assertEquals(result.nextRow(), ResultType.Deleted);
        assertEquals(result.nextItem(), "eleven");
        assertEquals(result.nextRow(), ResultType.Inserted);
        assertEquals(result.nextItem(), "ju ichi");
        assertEquals(result.nextRow(), ResultType.End);
    }
    
    @Test
    public void testNonIndexedQueries(){
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

        dbms.apply(context, insert,  1, "one");
        dbms.apply(context, insert,  2, "two");
        dbms.apply(context, insert,  3, "three");
        dbms.apply(context, insert,  4, "four");
        dbms.apply(context, insert,  5, "five");
        dbms.apply(context, insert,  6, "six");
        dbms.apply(context, insert,  7, "seven");
        dbms.apply(context, insert,  8, "eight");
        dbms.apply(context, insert,  9, "nine");
        dbms.apply(context, insert, 10, "ten");
        dbms.apply(context, insert, 11, "eleven");
        dbms.apply(context, insert, 12, "twelve");
        dbms.apply(context, insert, 13, "thirteen");

        Revision first = dbms.commit(context);

        TableReference numbersReference = dbms.tableReference(numbers);

        QueryTemplate nameEqual = dbms.queryTemplate
          (list((Expression) dbms.columnReference(numbersReference, number),
                (Expression) dbms.columnReference(numbersReference, name)),
           numbersReference,
           dbms.operation
           (BinaryOperationType.Equal,
            dbms.columnReference(numbersReference, name),
            dbms.parameter()));

        QueryResult result = dbms.diff(tail, first, nameEqual, "nine");

        assertEquals(result.nextRow(), ResultType.Inserted);
        assertEquals(result.nextItem(), 9);
        assertEquals(result.nextItem(), "nine");
        assertEquals(result.nextRow(), ResultType.End);

        result = dbms.diff(first, tail, nameEqual, "nine");

        assertEquals(result.nextRow(), ResultType.Deleted);
        assertEquals(result.nextItem(), 9);
        assertEquals(result.nextItem(), "nine");
        assertEquals(result.nextRow(), ResultType.End);

        QueryTemplate nameLessThan = dbms.queryTemplate
          (list((Expression) dbms.columnReference(numbersReference, number),
                (Expression) dbms.columnReference(numbersReference, name)),
           numbersReference,
           dbms.operation
           (BinaryOperationType.LessThan,
            dbms.columnReference(numbersReference, name),
            dbms.parameter()));

        result = dbms.diff(tail, first, nameLessThan, "nine");

        assertEquals(result.nextRow(), ResultType.Inserted);
        assertEquals(result.nextItem(), 4);
        assertEquals(result.nextItem(), "four");
        assertEquals(result.nextRow(), ResultType.Inserted);
        assertEquals(result.nextItem(), 5);
        assertEquals(result.nextItem(), "five");
        assertEquals(result.nextRow(), ResultType.Inserted);
        assertEquals(result.nextItem(), 8);
        assertEquals(result.nextItem(), "eight");
        assertEquals(result.nextRow(), ResultType.Inserted);
        assertEquals(result.nextItem(), 11);
        assertEquals(result.nextItem(), "eleven");
        assertEquals(result.nextRow(), ResultType.End);
    }
    
    @Test
    public void testIndexedColumnUpdates(){
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

        context = dbms.patchContext(first);

        dbms.apply(context, updateNumberWhereNumberEqual, 3, 4);

        Revision second = dbms.commit(context);

        QueryTemplate numberEqual = dbms.queryTemplate
          (list((Expression) dbms.columnReference(numbersReference, name)),
           numbersReference,
           dbms.operation
           (BinaryOperationType.Equal,
            dbms.columnReference(numbersReference, number),
            dbms.parameter()));

        QueryResult result = dbms.diff(tail, second, numberEqual, 4);

        assertEquals(result.nextRow(), ResultType.Inserted);
        assertEquals(result.nextItem(), "three");
        assertEquals(result.nextRow(), ResultType.End);

        result = dbms.diff(tail, second, numberEqual, 3);

        assertEquals(result.nextRow(), ResultType.End);
    }
}
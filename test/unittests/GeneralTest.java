package unittests;

import junit.framework.TestCase;

import org.junit.Test;

import static com.readytalk.oss.dbms.util.Util.list;
import static org.junit.Assert.*;

import com.readytalk.oss.dbms.BinaryOperation;
import com.readytalk.oss.dbms.Column;
import com.readytalk.oss.dbms.Revisions;
import com.readytalk.oss.dbms.Table;
import com.readytalk.oss.dbms.Expression;
import com.readytalk.oss.dbms.Revision;
import com.readytalk.oss.dbms.RevisionBuilder;
import com.readytalk.oss.dbms.PatchTemplate;
import com.readytalk.oss.dbms.InsertTemplate;
import com.readytalk.oss.dbms.DeleteTemplate;
import com.readytalk.oss.dbms.UpdateTemplate;
import com.readytalk.oss.dbms.Parameter;
import com.readytalk.oss.dbms.Constant;
import com.readytalk.oss.dbms.ColumnReference;
import com.readytalk.oss.dbms.TableReference;
import com.readytalk.oss.dbms.QueryTemplate;
import com.readytalk.oss.dbms.QueryResult;
import com.readytalk.oss.dbms.DuplicateKeyResolution;
import com.readytalk.oss.dbms.Expression;


public class GeneralTest extends TestCase{
    @Test
    public void testSimpleInsertDiffs(){
        Column number = new Column(Integer.class);
        Column name = new Column(String.class);
        Table numbers = new Table(list(number));
        
        Revision tail = Revisions.Empty;
        
        RevisionBuilder builder = tail.builder();
        
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
        
        QueryResult result = tail.diff(first, equal, 42);
        
        assertEquals(result.nextRow(), QueryResult.Type.Inserted);
        assertEquals(result.nextItem(), "forty two");
        assertEquals(result.nextRow(), QueryResult.Type.End);
        
        Object[] parameters = { 42 };
        result = first.diff(tail, equal, parameters);
        
        assertEquals(result.nextRow(), QueryResult.Type.Deleted);
        assertEquals(result.nextItem(), "forty two");
        assertEquals(result.nextRow(), QueryResult.Type.End);
        
        result = tail.diff(first, equal, 43);
        
        assertEquals(result.nextRow(), QueryResult.Type.End);
        Object[] parameters1 = { 42 };
        
        result = tail.diff(tail, equal, parameters1);
        
        assertEquals(result.nextRow(), QueryResult.Type.End);
        Object[] parameters2 = { 42 };
        
        result = first.diff(first, equal, parameters2);
        
        assertEquals(result.nextRow(), QueryResult.Type.End);
        }
    @Test
    public void testLargerInsertDiffs(){
        Column number = new Column(Integer.class);
        Column name = new Column(String.class);
        Table numbers = new Table(list(number));
        
        Revision tail = Revisions.Empty;
        
        PatchTemplate insert = new InsertTemplate
        (numbers,
         list(number, name),
         list((Expression) new Parameter(), new Parameter()),
         DuplicateKeyResolution.Throw);
        
        RevisionBuilder builder = tail.builder();
        
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
        
        QueryResult result = tail.diff(first, equal, 42);
        
        assertEquals(result.nextRow(), QueryResult.Type.Inserted);
        assertEquals(result.nextItem(), "forty two");
        assertEquals(result.nextRow(), QueryResult.Type.End);
        result = first.diff(tail, equal, 42);
        
        assertEquals(result.nextRow(), QueryResult.Type.Deleted);
        assertEquals(result.nextItem(), "forty two");
        assertEquals(result.nextRow(), QueryResult.Type.End);
        result = tail.diff(first, equal, 43);
        
        assertEquals(result.nextRow(), QueryResult.Type.Inserted);
        assertEquals(result.nextItem(), "forty three");
        assertEquals(result.nextRow(), QueryResult.Type.End);
        Object[] parameters = { 42 };
        result = tail.diff(tail, equal, parameters);
        
        assertEquals(result.nextRow(), QueryResult.Type.End);
        Object[] parameters1 = { 42 };
        
        result = first.diff(first, equal, parameters1);
        
        assertEquals(result.nextRow(), QueryResult.Type.End);
        
        builder = first.builder();
        
        builder.apply(insert, 1, "one");
        builder.apply(insert, 3, "three");
        builder.apply(insert, 5, "five");
        builder.apply(insert, 6, "six");
        
        Revision second = builder.commit();
        Object[] parameters2 = { 43 };
        
        result = tail.diff(second, equal, parameters2);
        
        assertEquals(result.nextRow(), QueryResult.Type.Inserted);
        assertEquals(result.nextItem(), "forty three");
        assertEquals(result.nextRow(), QueryResult.Type.End);
        Object[] parameters3 = { 43 };
        
        result = first.diff(second, equal, parameters3);
        
        assertEquals(result.nextRow(), QueryResult.Type.End);
        Object[] parameters4 = { 5 };
        
        result = first.diff(second, equal, parameters4);
        
        assertEquals(result.nextRow(), QueryResult.Type.Inserted);
        assertEquals(result.nextItem(), "five");
        assertEquals(result.nextRow(), QueryResult.Type.End);
        Object[] parameters5 = { 5 };
        
        result = tail.diff(first, equal, parameters5);
        
        assertEquals(result.nextRow(), QueryResult.Type.End);
        Object[] parameters6 = { 5 };
        
        result = second.diff(first, equal, parameters6);
        
        assertEquals(result.nextRow(), QueryResult.Type.Deleted);
        assertEquals(result.nextItem(), "five");
        assertEquals(result.nextRow(), QueryResult.Type.End);
        }

    @Test
    public void testDeleteDiffs(){
    
    Column number = new Column(Integer.class);
    Column name = new Column(String.class);
    Table numbers = new Table(list(number));

    Revision tail = Revisions.Empty;

    PatchTemplate insert = new InsertTemplate
      (numbers,
       list(number, name),
       list((Expression) new Parameter(), new Parameter()),
       DuplicateKeyResolution.Throw);

    RevisionBuilder builder = tail.builder();

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

    builder = first.builder();

    builder.apply(delete, 43);

    Revision second = builder.commit();

    Object[] parameters = { 43 };
    QueryResult result = first.diff(second, equal, parameters);

    assertEquals(result.nextRow(), QueryResult.Type.Deleted);
    assertEquals(result.nextItem(), "forty three");
    assertEquals(result.nextRow(), QueryResult.Type.End);
    Object[] parameters1 = { 43 };

    result = second.diff(first, equal, parameters1);

    assertEquals(result.nextRow(), QueryResult.Type.Inserted);
    assertEquals(result.nextItem(), "forty three");
    assertEquals(result.nextRow(), QueryResult.Type.End);
    Object[] parameters2 = { 43 };

    result = tail.diff(second, equal, parameters2);

    assertEquals(result.nextRow(), QueryResult.Type.End);
    Object[] parameters3 = { 42 };
    
    result = first.diff(second, equal, parameters3);

    assertEquals(result.nextRow(), QueryResult.Type.End);
    Object[] parameters4 = { 42 };
    
    result = tail.diff(second, equal, parameters4);

    assertEquals(result.nextRow(), QueryResult.Type.Inserted);
    assertEquals(result.nextItem(), "forty two");
    assertEquals(result.nextRow(), QueryResult.Type.End);
    Object[] parameters5 = { 42 };
    
    result = second.diff(tail, equal, parameters5);

    assertEquals(result.nextRow(), QueryResult.Type.Deleted);
    assertEquals(result.nextItem(), "forty two");
    assertEquals(result.nextRow(), QueryResult.Type.End);

    builder = first.builder();

    builder.apply(delete, 43);
    builder.apply(delete, 42);
    builder.apply(delete, 65);

    second = builder.commit();
    Object[] parameters6 = { 43 };

    result = first.diff(second, equal, parameters6);

    assertEquals(result.nextRow(), QueryResult.Type.Deleted);
    assertEquals(result.nextItem(), "forty three");
    assertEquals(result.nextRow(), QueryResult.Type.End);
    Object[] parameters7 = { 43 };

    result = second.diff(first, equal, parameters7);

    assertEquals(result.nextRow(), QueryResult.Type.Inserted);
    assertEquals(result.nextItem(), "forty three");
    assertEquals(result.nextRow(), QueryResult.Type.End);
    Object[] parameters8 = { 42 };

    result = first.diff(second, equal, parameters8);

    assertEquals(result.nextRow(), QueryResult.Type.Deleted);
    assertEquals(result.nextItem(), "forty two");
    assertEquals(result.nextRow(), QueryResult.Type.End);
    Object[] parameters9 = { 42 };

    result = second.diff(first, equal, parameters9);

    assertEquals(result.nextRow(), QueryResult.Type.Inserted);
    assertEquals(result.nextItem(), "forty two");
    assertEquals(result.nextRow(), QueryResult.Type.End);
    Object[] parameters10 = { 65 };

    result = first.diff(second, equal, parameters10);

    assertEquals(result.nextRow(), QueryResult.Type.Deleted);
    assertEquals(result.nextItem(), "sixty five");
    assertEquals(result.nextRow(), QueryResult.Type.End);
    Object[] parameters11 = { 65 };

    result = second.diff(first, equal, parameters11);

    assertEquals(result.nextRow(), QueryResult.Type.Inserted);
    assertEquals(result.nextItem(), "sixty five");
    assertEquals(result.nextRow(), QueryResult.Type.End);
    Object[] parameters12 = { 2 };
    
    result = first.diff(second, equal, parameters12);

    assertEquals(result.nextRow(), QueryResult.Type.End);
    Object[] parameters13 = { 2 };
    
    result = second.diff(first, equal, parameters13);

    assertEquals(result.nextRow(), QueryResult.Type.End);

    builder = second.builder();

    builder.apply(delete, 44);
    builder.apply(delete,  2);
    builder.apply(delete,  8);

    Revision third = builder.commit();
    Object[] parameters14 = { 44 };

    result = second.diff(third, equal, parameters14);

    assertEquals(result.nextRow(), QueryResult.Type.Deleted);
    assertEquals(result.nextItem(), "forty four");
    assertEquals(result.nextRow(), QueryResult.Type.End);
    Object[] parameters15 = { 44 };

    result = third.diff(second, equal, parameters15);

    assertEquals(result.nextRow(), QueryResult.Type.Inserted);
    assertEquals(result.nextItem(), "forty four");
    assertEquals(result.nextRow(), QueryResult.Type.End);
    Object[] parameters16 = { 44 };

    result = tail.diff(third, equal, parameters16);

    assertEquals(result.nextRow(), QueryResult.Type.End);
    Object[] parameters17 = { 42 };

    result = tail.diff(third, equal, parameters17);

    assertEquals(result.nextRow(), QueryResult.Type.End);
    	
    }
    
    @Test
    public void testUpdateDiffs(){
        Column number = new Column(Integer.class);
        Column name = new Column(String.class);
        Table numbers = new Table(list(number));

        Revision tail = Revisions.Empty;

        PatchTemplate insert = new InsertTemplate
        (numbers,
         list(number, name),
         list((Expression) new Parameter(), new Parameter()),
         DuplicateKeyResolution.Throw);
        
        RevisionBuilder builder = tail.builder();

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

        builder = first.builder();

        builder.apply(update, 1, "ichi");

        Revision second = builder.commit();

        QueryTemplate equal = new QueryTemplate
        (list((Expression) new ColumnReference(numbersReference, name)),
                numbersReference,
                new BinaryOperation
                (BinaryOperation.Type.Equal,
                        new ColumnReference(numbersReference, number),
                        new Parameter()));

        Object[] parameters = { 1 };
        QueryResult result = first.diff(second, equal, parameters);

        assertEquals(result.nextRow(), QueryResult.Type.Deleted);
        assertEquals(result.nextItem(), "one");
        assertEquals(result.nextRow(), QueryResult.Type.Inserted);
        assertEquals(result.nextItem(), "ichi");
        assertEquals(result.nextRow(), QueryResult.Type.End);
        Object[] parameters1 = { 1 };

        result = second.diff(first, equal, parameters1);

        assertEquals(result.nextRow(), QueryResult.Type.Deleted);
        assertEquals(result.nextItem(), "ichi");
        assertEquals(result.nextRow(), QueryResult.Type.Inserted);
        assertEquals(result.nextItem(), "one");
        assertEquals(result.nextRow(), QueryResult.Type.End);
        Object[] parameters2 = { 2 };

        result = first.diff(second, equal, parameters2);

        assertEquals(result.nextRow(), QueryResult.Type.End);
        Object[] parameters3 = { 1 };

        result = tail.diff(first, equal, parameters3);

        assertEquals(result.nextRow(), QueryResult.Type.Inserted);
        assertEquals(result.nextItem(), "one");
        assertEquals(result.nextRow(), QueryResult.Type.End);
        Object[] parameters4 = { 1 };

        result = tail.diff(second, equal, parameters4);

        assertEquals(result.nextRow(), QueryResult.Type.Inserted);
        assertEquals(result.nextItem(), "ichi");
        assertEquals(result.nextRow(), QueryResult.Type.End);

        builder = second.builder();

        builder.apply(update, 11, "ju ichi");
        builder.apply(update,  6, "roku");
        builder.apply(update,  7, "shichi");

        Revision third = builder.commit();

        QueryTemplate any = new QueryTemplate
        (list((Expression) new ColumnReference(numbersReference, name)),
                numbersReference,
                new Constant(true));
        Object[] parameters5 = {};

        result = second.diff(third, any, parameters5);

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
        Column number = new Column(Integer.class);
        Column name = new Column(String.class);
        Table numbers = new Table(list(number));

        Revision tail = Revisions.Empty;

        PatchTemplate insert = new InsertTemplate
          (numbers,
           list(number, name),
           list((Expression) new Parameter(), new Parameter()),
           DuplicateKeyResolution.Throw);

        RevisionBuilder builder = tail.builder();

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

        Object[] parameters = { "nine" };
        QueryResult result = tail.diff(first, nameEqual, parameters);

        assertEquals(result.nextRow(), QueryResult.Type.Inserted);
        assertEquals(result.nextItem(), 9);
        assertEquals(result.nextItem(), "nine");
        assertEquals(result.nextRow(), QueryResult.Type.End);
        Object[] parameters1 = { "nine" };

        result = first.diff(tail, nameEqual, parameters1);

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
        Object[] parameters2 = { "nine" };

        result = tail.diff(first, nameLessThan, parameters2);

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
        Column number = new Column(Integer.class);
        Column name = new Column(String.class);
        Table numbers = new Table(list(number));

        Revision tail = Revisions.Empty;

        PatchTemplate insert = new InsertTemplate
          (numbers,
           list(number, name),
           list((Expression) new Parameter(), new Parameter()),
           DuplicateKeyResolution.Throw);

        RevisionBuilder builder = tail.builder();

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

        builder = first.builder();

        builder.apply(updateNumberWhereNumberEqual, 3, 4);

        Revision second = builder.commit();

        QueryTemplate numberEqual = new QueryTemplate
          (list((Expression) new ColumnReference(numbersReference, name)),
           numbersReference,
           new BinaryOperation
           (BinaryOperation.Type.Equal,
            new ColumnReference(numbersReference, number),
            new Parameter()));

        Object[] parameters = { 4 };
        QueryResult result = tail.diff(second, numberEqual, parameters);

        assertEquals(result.nextRow(), QueryResult.Type.Inserted);
        assertEquals(result.nextItem(), "three");
        assertEquals(result.nextRow(), QueryResult.Type.End);
        Object[] parameters1 = { 3 };

        result = tail.diff(second, numberEqual, parameters1);

        assertEquals(result.nextRow(), QueryResult.Type.End);
    }
}

package unittests;

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
import com.readytalk.oss.dbms.DBMS.UnaryOperationType;
import com.readytalk.oss.dbms.imp.MyDBMS;

public class OperationTest{
    
    @Test
    public void testComparisons(){
    	
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

        QueryTemplate lessThan = dbms.queryTemplate
          (list((Expression) dbms.columnReference(numbersReference, name)),
           numbersReference,
           dbms.operation
           (BinaryOperationType.LessThan,
            dbms.columnReference(numbersReference, number),
            dbms.parameter()));

        QueryResult result = dbms.diff(tail, first, lessThan, 1);

        assertEquals(result.nextRow(), ResultType.End);

        result = dbms.diff(tail, first, lessThan, 2);

        assertEquals(result.nextRow(), ResultType.Inserted);
        assertEquals(result.nextItem(), "one");
        assertEquals(result.nextRow(), ResultType.End);

        result = dbms.diff(tail, first, lessThan, 6);

        assertEquals(result.nextRow(), ResultType.Inserted);
        assertEquals(result.nextItem(), "one");
        assertEquals(result.nextRow(), ResultType.Inserted);
        assertEquals(result.nextItem(), "two");
        assertEquals(result.nextRow(), ResultType.Inserted);
        assertEquals(result.nextItem(), "three");
        assertEquals(result.nextRow(), ResultType.Inserted);
        assertEquals(result.nextItem(), "four");
        assertEquals(result.nextRow(), ResultType.Inserted);
        assertEquals(result.nextItem(), "five");
        assertEquals(result.nextRow(), ResultType.End);

        result = dbms.diff(tail, first, lessThan, 42);

        assertEquals(result.nextRow(), ResultType.Inserted);
        assertEquals(result.nextItem(), "one");
        assertEquals(result.nextRow(), ResultType.Inserted);
        assertEquals(result.nextItem(), "two");
        assertEquals(result.nextRow(), ResultType.Inserted);
        assertEquals(result.nextItem(), "three");
        assertEquals(result.nextRow(), ResultType.Inserted);
        assertEquals(result.nextItem(), "four");
        assertEquals(result.nextRow(), ResultType.Inserted);
        assertEquals(result.nextItem(), "five");
        assertEquals(result.nextRow(), ResultType.Inserted);
        assertEquals(result.nextItem(), "six");
        assertEquals(result.nextRow(), ResultType.Inserted);
        assertEquals(result.nextItem(), "seven");
        assertEquals(result.nextRow(), ResultType.Inserted);
        assertEquals(result.nextItem(), "eight");
        assertEquals(result.nextRow(), ResultType.Inserted);
        assertEquals(result.nextItem(), "nine");
        assertEquals(result.nextRow(), ResultType.Inserted);
        assertEquals(result.nextItem(), "ten");
        assertEquals(result.nextRow(), ResultType.Inserted);
        assertEquals(result.nextItem(), "eleven");
        assertEquals(result.nextRow(), ResultType.Inserted);
        assertEquals(result.nextItem(), "twelve");
        assertEquals(result.nextRow(), ResultType.Inserted);
        assertEquals(result.nextItem(), "thirteen");
        assertEquals(result.nextRow(), ResultType.End);

        QueryTemplate greaterThan = dbms.queryTemplate
          (list((Expression) dbms.columnReference(numbersReference, name)),
           numbersReference,
           dbms.operation
           (BinaryOperationType.GreaterThan,
            dbms.columnReference(numbersReference, number),
            dbms.parameter()));

        result = dbms.diff(tail, first, greaterThan, 13);

        assertEquals(result.nextRow(), ResultType.End);

        result = dbms.diff(tail, first, greaterThan, 12);

        assertEquals(result.nextRow(), ResultType.Inserted);
        assertEquals(result.nextItem(), "thirteen");
        assertEquals(result.nextRow(), ResultType.End);

        result = dbms.diff(tail, first, greaterThan, 11);

        assertEquals(result.nextRow(), ResultType.Inserted);
        assertEquals(result.nextItem(), "twelve");
        assertEquals(result.nextRow(), ResultType.Inserted);
        assertEquals(result.nextItem(), "thirteen");
        assertEquals(result.nextRow(), ResultType.End);

        QueryTemplate lessThanOrEqual = dbms.queryTemplate
          (list((Expression) dbms.columnReference(numbersReference, name)),
           numbersReference,
           dbms.operation
           (BinaryOperationType.LessThanOrEqual,
            dbms.columnReference(numbersReference, number),
            dbms.parameter()));

        result = dbms.diff(tail, first, lessThanOrEqual, 0);

        assertEquals(result.nextRow(), ResultType.End);

        result = dbms.diff(tail, first, lessThanOrEqual, 1);

        assertEquals(result.nextRow(), ResultType.Inserted);
        assertEquals(result.nextItem(), "one");
        assertEquals(result.nextRow(), ResultType.End);

        result = dbms.diff(tail, first, lessThanOrEqual, 2);

        assertEquals(result.nextRow(), ResultType.Inserted);
        assertEquals(result.nextItem(), "one");
        assertEquals(result.nextRow(), ResultType.Inserted);
        assertEquals(result.nextItem(), "two");
        assertEquals(result.nextRow(), ResultType.End);

        QueryTemplate greaterThanOrEqual = dbms.queryTemplate
          (list((Expression) dbms.columnReference(numbersReference, name)),
           numbersReference,
           dbms.operation
           (BinaryOperationType.GreaterThanOrEqual,
            dbms.columnReference(numbersReference, number),
            dbms.parameter()));

        result = dbms.diff(tail, first, greaterThanOrEqual, 14);

        assertEquals(result.nextRow(), ResultType.End);

        result = dbms.diff(tail, first, greaterThanOrEqual, 13);

        assertEquals(result.nextRow(), ResultType.Inserted);
        assertEquals(result.nextItem(), "thirteen");
        assertEquals(result.nextRow(), ResultType.End);

        result = dbms.diff(tail, first, greaterThanOrEqual, 12);

        assertEquals(result.nextRow(), ResultType.Inserted);
        assertEquals(result.nextItem(), "twelve");
        assertEquals(result.nextRow(), ResultType.Inserted);
        assertEquals(result.nextItem(), "thirteen");
        assertEquals(result.nextRow(), ResultType.End);

        QueryTemplate notEqual = dbms.queryTemplate
          (list((Expression) dbms.columnReference(numbersReference, name)),
           numbersReference,
           dbms.operation
           (BinaryOperationType.NotEqual,
            dbms.columnReference(numbersReference, number),
            dbms.parameter()));

        result = dbms.diff(tail, first, notEqual, 4);

        assertEquals(result.nextRow(), ResultType.Inserted);
        assertEquals(result.nextItem(), "one");
        assertEquals(result.nextRow(), ResultType.Inserted);
        assertEquals(result.nextItem(), "two");
        assertEquals(result.nextRow(), ResultType.Inserted);
        assertEquals(result.nextItem(), "three");
        assertEquals(result.nextRow(), ResultType.Inserted);
        assertEquals(result.nextItem(), "five");
        assertEquals(result.nextRow(), ResultType.Inserted);
        assertEquals(result.nextItem(), "six");
        assertEquals(result.nextRow(), ResultType.Inserted);
        assertEquals(result.nextItem(), "seven");
        assertEquals(result.nextRow(), ResultType.Inserted);
        assertEquals(result.nextItem(), "eight");
        assertEquals(result.nextRow(), ResultType.Inserted);
        assertEquals(result.nextItem(), "nine");
        assertEquals(result.nextRow(), ResultType.Inserted);
        assertEquals(result.nextItem(), "ten");
        assertEquals(result.nextRow(), ResultType.Inserted);
        assertEquals(result.nextItem(), "eleven");
        assertEquals(result.nextRow(), ResultType.Inserted);
        assertEquals(result.nextItem(), "twelve");
        assertEquals(result.nextRow(), ResultType.Inserted);
        assertEquals(result.nextItem(), "thirteen");
        assertEquals(result.nextRow(), ResultType.End);
    }
    
    @Test
    public void testBooleanOperators(){
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

        QueryTemplate greaterThanAndLessThan = dbms.queryTemplate
          (list((Expression) dbms.columnReference(numbersReference, name)),
           numbersReference,
           dbms.operation
           (BinaryOperationType.And,
            dbms.operation
            (BinaryOperationType.GreaterThan,
             dbms.columnReference(numbersReference, number),
             dbms.parameter()),
            dbms.operation
            (BinaryOperationType.LessThan,
             dbms.columnReference(numbersReference, number),
             dbms.parameter())));

        QueryResult result = dbms.diff(tail, first, greaterThanAndLessThan, 8, 12);

        assertEquals(result.nextRow(), ResultType.Inserted);
        assertEquals(result.nextItem(), "nine");
        assertEquals(result.nextRow(), ResultType.Inserted);
        assertEquals(result.nextItem(), "ten");
        assertEquals(result.nextRow(), ResultType.Inserted);
        assertEquals(result.nextItem(), "eleven");
        assertEquals(result.nextRow(), ResultType.End);

        result = dbms.diff(tail, first, greaterThanAndLessThan, 8, 8);

        assertEquals(result.nextRow(), ResultType.End);

        result = dbms.diff(tail, first, greaterThanAndLessThan, 12, 8);

        assertEquals(result.nextRow(), ResultType.End);

        QueryTemplate lessThanOrGreaterThan = dbms.queryTemplate
          (list((Expression) dbms.columnReference(numbersReference, name)),
           numbersReference,
           dbms.operation
           (BinaryOperationType.Or,
            dbms.operation
            (BinaryOperationType.LessThan,
             dbms.columnReference(numbersReference, number),
             dbms.parameter()),
            dbms.operation
            (BinaryOperationType.GreaterThan,
             dbms.columnReference(numbersReference, number),
             dbms.parameter())));

        result = dbms.diff(tail, first, lessThanOrGreaterThan, 8, 12);

        assertEquals(result.nextRow(), ResultType.Inserted);
        assertEquals(result.nextItem(), "one");
        assertEquals(result.nextRow(), ResultType.Inserted);
        assertEquals(result.nextItem(), "two");
        assertEquals(result.nextRow(), ResultType.Inserted);
        assertEquals(result.nextItem(), "three");
        assertEquals(result.nextRow(), ResultType.Inserted);
        assertEquals(result.nextItem(), "four");
        assertEquals(result.nextRow(), ResultType.Inserted);
        assertEquals(result.nextItem(), "five");
        assertEquals(result.nextRow(), ResultType.Inserted);
        assertEquals(result.nextItem(), "six");
        assertEquals(result.nextRow(), ResultType.Inserted);
        assertEquals(result.nextItem(), "seven");
        assertEquals(result.nextRow(), ResultType.Inserted);
        assertEquals(result.nextItem(), "thirteen");
        assertEquals(result.nextRow(), ResultType.End);

        result = dbms.diff(tail, first, lessThanOrGreaterThan, 8, 8);

        assertEquals(result.nextRow(), ResultType.Inserted);
        assertEquals(result.nextItem(), "one");
        assertEquals(result.nextRow(), ResultType.Inserted);
        assertEquals(result.nextItem(), "two");
        assertEquals(result.nextRow(), ResultType.Inserted);
        assertEquals(result.nextItem(), "three");
        assertEquals(result.nextRow(), ResultType.Inserted);
        assertEquals(result.nextItem(), "four");
        assertEquals(result.nextRow(), ResultType.Inserted);
        assertEquals(result.nextItem(), "five");
        assertEquals(result.nextRow(), ResultType.Inserted);
        assertEquals(result.nextItem(), "six");
        assertEquals(result.nextRow(), ResultType.Inserted);
        assertEquals(result.nextItem(), "seven");
        assertEquals(result.nextRow(), ResultType.Inserted);
        assertEquals(result.nextItem(), "nine");
        assertEquals(result.nextRow(), ResultType.Inserted);
        assertEquals(result.nextItem(), "ten");
        assertEquals(result.nextRow(), ResultType.Inserted);
        assertEquals(result.nextItem(), "eleven");
        assertEquals(result.nextRow(), ResultType.Inserted);
        assertEquals(result.nextItem(), "twelve");
        assertEquals(result.nextRow(), ResultType.Inserted);
        assertEquals(result.nextItem(), "thirteen");
        assertEquals(result.nextRow(), ResultType.End);

        result = dbms.diff(tail, first, lessThanOrGreaterThan, 12, 8);

        assertEquals(result.nextRow(), ResultType.Inserted);
        assertEquals(result.nextItem(), "one");
        assertEquals(result.nextRow(), ResultType.Inserted);
        assertEquals(result.nextItem(), "two");
        assertEquals(result.nextRow(), ResultType.Inserted);
        assertEquals(result.nextItem(), "three");
        assertEquals(result.nextRow(), ResultType.Inserted);
        assertEquals(result.nextItem(), "four");
        assertEquals(result.nextRow(), ResultType.Inserted);
        assertEquals(result.nextItem(), "five");
        assertEquals(result.nextRow(), ResultType.Inserted);
        assertEquals(result.nextItem(), "six");
        assertEquals(result.nextRow(), ResultType.Inserted);
        assertEquals(result.nextItem(), "seven");
        assertEquals(result.nextRow(), ResultType.Inserted);
        assertEquals(result.nextItem(), "eight");
        assertEquals(result.nextRow(), ResultType.Inserted);
        assertEquals(result.nextItem(), "nine");
        assertEquals(result.nextRow(), ResultType.Inserted);
        assertEquals(result.nextItem(), "ten");
        assertEquals(result.nextRow(), ResultType.Inserted);
        assertEquals(result.nextItem(), "eleven");
        assertEquals(result.nextRow(), ResultType.Inserted);
        assertEquals(result.nextItem(), "twelve");
        assertEquals(result.nextRow(), ResultType.Inserted);
        assertEquals(result.nextItem(), "thirteen");
        assertEquals(result.nextRow(), ResultType.End);

        QueryTemplate notEqual = dbms.queryTemplate
          (list((Expression) dbms.columnReference(numbersReference, name)),
           numbersReference,
           dbms.operation
           (UnaryOperationType.Not,
            dbms.operation
            (BinaryOperationType.Equal,
             dbms.columnReference(numbersReference, number),
             dbms.parameter())));

        result = dbms.diff(tail, first, notEqual, 2);

        assertEquals(result.nextRow(), ResultType.Inserted);
        assertEquals(result.nextItem(), "one");
        assertEquals(result.nextRow(), ResultType.Inserted);
        assertEquals(result.nextItem(), "three");
        assertEquals(result.nextRow(), ResultType.Inserted);
        assertEquals(result.nextItem(), "four");
        assertEquals(result.nextRow(), ResultType.Inserted);
        assertEquals(result.nextItem(), "five");
        assertEquals(result.nextRow(), ResultType.Inserted);
        assertEquals(result.nextItem(), "six");
        assertEquals(result.nextRow(), ResultType.Inserted);
        assertEquals(result.nextItem(), "seven");
        assertEquals(result.nextRow(), ResultType.Inserted);
        assertEquals(result.nextItem(), "eight");
        assertEquals(result.nextRow(), ResultType.Inserted);
        assertEquals(result.nextItem(), "nine");
        assertEquals(result.nextRow(), ResultType.Inserted);
        assertEquals(result.nextItem(), "ten");
        assertEquals(result.nextRow(), ResultType.Inserted);
        assertEquals(result.nextItem(), "eleven");
        assertEquals(result.nextRow(), ResultType.Inserted);
        assertEquals(result.nextItem(), "twelve");
        assertEquals(result.nextRow(), ResultType.Inserted);
        assertEquals(result.nextItem(), "thirteen");
        assertEquals(result.nextRow(), ResultType.End);

        QueryTemplate greaterThanAndLessThanOrNotLessThanOrEqual
          = dbms.queryTemplate
          (list((Expression) dbms.columnReference(numbersReference, name)),
           numbersReference,
           dbms.operation
           (BinaryOperationType.Or,
            dbms.operation
            (BinaryOperationType.And,
             dbms.operation
             (BinaryOperationType.GreaterThan,
              dbms.columnReference(numbersReference, number),
              dbms.parameter()),
             dbms.operation
             (BinaryOperationType.LessThan,
              dbms.columnReference(numbersReference, number),
              dbms.parameter())),
            dbms.operation
            (UnaryOperationType.Not,
             dbms.operation
             (BinaryOperationType.LessThanOrEqual,
              dbms.columnReference(numbersReference, number),
              dbms.parameter()))));

        result = dbms.diff
          (tail, first, greaterThanAndLessThanOrNotLessThanOrEqual, 3, 7, 10);

        assertEquals(result.nextRow(), ResultType.Inserted);
        assertEquals(result.nextItem(), "four");
        assertEquals(result.nextRow(), ResultType.Inserted);
        assertEquals(result.nextItem(), "five");
        assertEquals(result.nextRow(), ResultType.Inserted);
        assertEquals(result.nextItem(), "six");
        assertEquals(result.nextRow(), ResultType.Inserted);
        assertEquals(result.nextItem(), "eleven");
        assertEquals(result.nextRow(), ResultType.Inserted);
        assertEquals(result.nextItem(), "twelve");
        assertEquals(result.nextRow(), ResultType.Inserted);
        assertEquals(result.nextItem(), "thirteen");
        assertEquals(result.nextRow(), ResultType.End);	
    }    
}

package unittests;

import junit.framework.TestCase;

import org.junit.Test;

import static com.readytalk.oss.dbms.util.Util.list;
import static com.readytalk.oss.dbms.ExpressionFactory.parameter;
import static com.readytalk.oss.dbms.ExpressionFactory.reference;
import static com.readytalk.oss.dbms.ExpressionFactory.and;
import static com.readytalk.oss.dbms.ExpressionFactory.or;
import static com.readytalk.oss.dbms.ExpressionFactory.equal;
import static com.readytalk.oss.dbms.ExpressionFactory.notEqual;
import static com.readytalk.oss.dbms.ExpressionFactory.lessThan;
import static com.readytalk.oss.dbms.ExpressionFactory.lessThanOrEqual;
import static com.readytalk.oss.dbms.ExpressionFactory.greaterThan;
import static com.readytalk.oss.dbms.ExpressionFactory.greaterThanOrEqual;
import static com.readytalk.oss.dbms.ExpressionFactory.not;
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
import com.readytalk.oss.dbms.TableReference;
import com.readytalk.oss.dbms.QueryTemplate;
import com.readytalk.oss.dbms.QueryResult;
import com.readytalk.oss.dbms.Parameter;
import com.readytalk.oss.dbms.ColumnReference;
import com.readytalk.oss.dbms.DuplicateKeyResolution;
import com.readytalk.oss.dbms.UnaryOperation;
import com.readytalk.oss.dbms.imp.MyDBMS;

public class OperationTest extends TestCase{
    
    @Test
    public void testComparisons(){
    	
        DBMS dbms = new MyDBMS();

        Column number = new Column(Integer.class);
        Column name = new Column(String.class);
        Table numbers = new Table(list(number));

        Revision tail = dbms.revision();

        PatchTemplate insert = new InsertTemplate
          (numbers,
           list(number, name),
           list(parameter(), parameter()),
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

        QueryTemplate lessThan = new QueryTemplate
          (list(reference(numbersReference, name)),
           numbersReference,
           lessThan(reference(numbersReference, number),
                    parameter()));

        QueryResult result = dbms.diff(tail, first, lessThan, 1);

        assertEquals(result.nextRow(), QueryResult.Type.End);

        result = dbms.diff(tail, first, lessThan, 2);

        assertEquals(result.nextRow(), QueryResult.Type.Inserted);
        assertEquals(result.nextItem(), "one");
        assertEquals(result.nextRow(), QueryResult.Type.End);

        result = dbms.diff(tail, first, lessThan, 6);

        assertEquals(result.nextRow(), QueryResult.Type.Inserted);
        assertEquals(result.nextItem(), "one");
        assertEquals(result.nextRow(), QueryResult.Type.Inserted);
        assertEquals(result.nextItem(), "two");
        assertEquals(result.nextRow(), QueryResult.Type.Inserted);
        assertEquals(result.nextItem(), "three");
        assertEquals(result.nextRow(), QueryResult.Type.Inserted);
        assertEquals(result.nextItem(), "four");
        assertEquals(result.nextRow(), QueryResult.Type.Inserted);
        assertEquals(result.nextItem(), "five");
        assertEquals(result.nextRow(), QueryResult.Type.End);

        result = dbms.diff(tail, first, lessThan, 42);

        assertEquals(result.nextRow(), QueryResult.Type.Inserted);
        assertEquals(result.nextItem(), "one");
        assertEquals(result.nextRow(), QueryResult.Type.Inserted);
        assertEquals(result.nextItem(), "two");
        assertEquals(result.nextRow(), QueryResult.Type.Inserted);
        assertEquals(result.nextItem(), "three");
        assertEquals(result.nextRow(), QueryResult.Type.Inserted);
        assertEquals(result.nextItem(), "four");
        assertEquals(result.nextRow(), QueryResult.Type.Inserted);
        assertEquals(result.nextItem(), "five");
        assertEquals(result.nextRow(), QueryResult.Type.Inserted);
        assertEquals(result.nextItem(), "six");
        assertEquals(result.nextRow(), QueryResult.Type.Inserted);
        assertEquals(result.nextItem(), "seven");
        assertEquals(result.nextRow(), QueryResult.Type.Inserted);
        assertEquals(result.nextItem(), "eight");
        assertEquals(result.nextRow(), QueryResult.Type.Inserted);
        assertEquals(result.nextItem(), "nine");
        assertEquals(result.nextRow(), QueryResult.Type.Inserted);
        assertEquals(result.nextItem(), "ten");
        assertEquals(result.nextRow(), QueryResult.Type.Inserted);
        assertEquals(result.nextItem(), "eleven");
        assertEquals(result.nextRow(), QueryResult.Type.Inserted);
        assertEquals(result.nextItem(), "twelve");
        assertEquals(result.nextRow(), QueryResult.Type.Inserted);
        assertEquals(result.nextItem(), "thirteen");
        assertEquals(result.nextRow(), QueryResult.Type.End);

        QueryTemplate greaterThan = new QueryTemplate
          (list(reference(numbersReference, name)),
           numbersReference,
           greaterThan(reference(numbersReference, number), parameter()));

        result = dbms.diff(tail, first, greaterThan, 13);

        assertEquals(result.nextRow(), QueryResult.Type.End);

        result = dbms.diff(tail, first, greaterThan, 12);

        assertEquals(result.nextRow(), QueryResult.Type.Inserted);
        assertEquals(result.nextItem(), "thirteen");
        assertEquals(result.nextRow(), QueryResult.Type.End);

        result = dbms.diff(tail, first, greaterThan, 11);

        assertEquals(result.nextRow(), QueryResult.Type.Inserted);
        assertEquals(result.nextItem(), "twelve");
        assertEquals(result.nextRow(), QueryResult.Type.Inserted);
        assertEquals(result.nextItem(), "thirteen");
        assertEquals(result.nextRow(), QueryResult.Type.End);

        QueryTemplate lessThanOrEqual = new QueryTemplate
          (list(reference(numbersReference, name)),
           numbersReference,
           lessThanOrEqual(reference(numbersReference, number), parameter()));

        result = dbms.diff(tail, first, lessThanOrEqual, 0);

        assertEquals(result.nextRow(), QueryResult.Type.End);

        result = dbms.diff(tail, first, lessThanOrEqual, 1);

        assertEquals(result.nextRow(), QueryResult.Type.Inserted);
        assertEquals(result.nextItem(), "one");
        assertEquals(result.nextRow(), QueryResult.Type.End);

        result = dbms.diff(tail, first, lessThanOrEqual, 2);

        assertEquals(result.nextRow(), QueryResult.Type.Inserted);
        assertEquals(result.nextItem(), "one");
        assertEquals(result.nextRow(), QueryResult.Type.Inserted);
        assertEquals(result.nextItem(), "two");
        assertEquals(result.nextRow(), QueryResult.Type.End);

        QueryTemplate greaterThanOrEqual = new QueryTemplate
          (list(reference(numbersReference, name)),
           numbersReference,
           greaterThanOrEqual(reference(numbersReference, number),
                              parameter()));

        result = dbms.diff(tail, first, greaterThanOrEqual, 14);

        assertEquals(result.nextRow(), QueryResult.Type.End);

        result = dbms.diff(tail, first, greaterThanOrEqual, 13);

        assertEquals(result.nextRow(), QueryResult.Type.Inserted);
        assertEquals(result.nextItem(), "thirteen");
        assertEquals(result.nextRow(), QueryResult.Type.End);

        result = dbms.diff(tail, first, greaterThanOrEqual, 12);

        assertEquals(result.nextRow(), QueryResult.Type.Inserted);
        assertEquals(result.nextItem(), "twelve");
        assertEquals(result.nextRow(), QueryResult.Type.Inserted);
        assertEquals(result.nextItem(), "thirteen");
        assertEquals(result.nextRow(), QueryResult.Type.End);

        QueryTemplate notEqual = new QueryTemplate
          (list(reference(numbersReference, name)),
           numbersReference,
           notEqual(reference(numbersReference, number), parameter()));

        result = dbms.diff(tail, first, notEqual, 4);

        assertEquals(result.nextRow(), QueryResult.Type.Inserted);
        assertEquals(result.nextItem(), "one");
        assertEquals(result.nextRow(), QueryResult.Type.Inserted);
        assertEquals(result.nextItem(), "two");
        assertEquals(result.nextRow(), QueryResult.Type.Inserted);
        assertEquals(result.nextItem(), "three");
        assertEquals(result.nextRow(), QueryResult.Type.Inserted);
        assertEquals(result.nextItem(), "five");
        assertEquals(result.nextRow(), QueryResult.Type.Inserted);
        assertEquals(result.nextItem(), "six");
        assertEquals(result.nextRow(), QueryResult.Type.Inserted);
        assertEquals(result.nextItem(), "seven");
        assertEquals(result.nextRow(), QueryResult.Type.Inserted);
        assertEquals(result.nextItem(), "eight");
        assertEquals(result.nextRow(), QueryResult.Type.Inserted);
        assertEquals(result.nextItem(), "nine");
        assertEquals(result.nextRow(), QueryResult.Type.Inserted);
        assertEquals(result.nextItem(), "ten");
        assertEquals(result.nextRow(), QueryResult.Type.Inserted);
        assertEquals(result.nextItem(), "eleven");
        assertEquals(result.nextRow(), QueryResult.Type.Inserted);
        assertEquals(result.nextItem(), "twelve");
        assertEquals(result.nextRow(), QueryResult.Type.Inserted);
        assertEquals(result.nextItem(), "thirteen");
        assertEquals(result.nextRow(), QueryResult.Type.End);
    }
    
    @Test
    public void testBooleanOperators(){
    	DBMS dbms = new MyDBMS();

        Column number = new Column(Integer.class);
        Column name = new Column(String.class);
        Table numbers = new Table(list(number));

        Revision tail = dbms.revision();

        PatchTemplate insert = new InsertTemplate
          (numbers,
           list(number, name),
           list(parameter(), parameter()),
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

        QueryTemplate greaterThanAndLessThan = new QueryTemplate
          (list(reference(numbersReference, name)),
           numbersReference,
           and(greaterThan(reference(numbersReference, number), parameter()),
               lessThan(reference(numbersReference, number), parameter())));

        QueryResult result = dbms.diff
          (tail, first, greaterThanAndLessThan, 8, 12);

        assertEquals(result.nextRow(), QueryResult.Type.Inserted);
        assertEquals(result.nextItem(), "nine");
        assertEquals(result.nextRow(), QueryResult.Type.Inserted);
        assertEquals(result.nextItem(), "ten");
        assertEquals(result.nextRow(), QueryResult.Type.Inserted);
        assertEquals(result.nextItem(), "eleven");
        assertEquals(result.nextRow(), QueryResult.Type.End);

        result = dbms.diff(tail, first, greaterThanAndLessThan, 8, 8);

        assertEquals(result.nextRow(), QueryResult.Type.End);

        result = dbms.diff(tail, first, greaterThanAndLessThan, 12, 8);

        assertEquals(result.nextRow(), QueryResult.Type.End);

        QueryTemplate lessThanOrGreaterThan = new QueryTemplate
          (list(reference(numbersReference, name)),
           numbersReference,
           or(lessThan(reference(numbersReference, number), parameter()),
              greaterThan(reference(numbersReference, number), parameter())));

        result = dbms.diff(tail, first, lessThanOrGreaterThan, 8, 12);

        assertEquals(result.nextRow(), QueryResult.Type.Inserted);
        assertEquals(result.nextItem(), "one");
        assertEquals(result.nextRow(), QueryResult.Type.Inserted);
        assertEquals(result.nextItem(), "two");
        assertEquals(result.nextRow(), QueryResult.Type.Inserted);
        assertEquals(result.nextItem(), "three");
        assertEquals(result.nextRow(), QueryResult.Type.Inserted);
        assertEquals(result.nextItem(), "four");
        assertEquals(result.nextRow(), QueryResult.Type.Inserted);
        assertEquals(result.nextItem(), "five");
        assertEquals(result.nextRow(), QueryResult.Type.Inserted);
        assertEquals(result.nextItem(), "six");
        assertEquals(result.nextRow(), QueryResult.Type.Inserted);
        assertEquals(result.nextItem(), "seven");
        assertEquals(result.nextRow(), QueryResult.Type.Inserted);
        assertEquals(result.nextItem(), "thirteen");
        assertEquals(result.nextRow(), QueryResult.Type.End);

        result = dbms.diff(tail, first, lessThanOrGreaterThan, 8, 8);

        assertEquals(result.nextRow(), QueryResult.Type.Inserted);
        assertEquals(result.nextItem(), "one");
        assertEquals(result.nextRow(), QueryResult.Type.Inserted);
        assertEquals(result.nextItem(), "two");
        assertEquals(result.nextRow(), QueryResult.Type.Inserted);
        assertEquals(result.nextItem(), "three");
        assertEquals(result.nextRow(), QueryResult.Type.Inserted);
        assertEquals(result.nextItem(), "four");
        assertEquals(result.nextRow(), QueryResult.Type.Inserted);
        assertEquals(result.nextItem(), "five");
        assertEquals(result.nextRow(), QueryResult.Type.Inserted);
        assertEquals(result.nextItem(), "six");
        assertEquals(result.nextRow(), QueryResult.Type.Inserted);
        assertEquals(result.nextItem(), "seven");
        assertEquals(result.nextRow(), QueryResult.Type.Inserted);
        assertEquals(result.nextItem(), "nine");
        assertEquals(result.nextRow(), QueryResult.Type.Inserted);
        assertEquals(result.nextItem(), "ten");
        assertEquals(result.nextRow(), QueryResult.Type.Inserted);
        assertEquals(result.nextItem(), "eleven");
        assertEquals(result.nextRow(), QueryResult.Type.Inserted);
        assertEquals(result.nextItem(), "twelve");
        assertEquals(result.nextRow(), QueryResult.Type.Inserted);
        assertEquals(result.nextItem(), "thirteen");
        assertEquals(result.nextRow(), QueryResult.Type.End);

        result = dbms.diff(tail, first, lessThanOrGreaterThan, 12, 8);

        assertEquals(result.nextRow(), QueryResult.Type.Inserted);
        assertEquals(result.nextItem(), "one");
        assertEquals(result.nextRow(), QueryResult.Type.Inserted);
        assertEquals(result.nextItem(), "two");
        assertEquals(result.nextRow(), QueryResult.Type.Inserted);
        assertEquals(result.nextItem(), "three");
        assertEquals(result.nextRow(), QueryResult.Type.Inserted);
        assertEquals(result.nextItem(), "four");
        assertEquals(result.nextRow(), QueryResult.Type.Inserted);
        assertEquals(result.nextItem(), "five");
        assertEquals(result.nextRow(), QueryResult.Type.Inserted);
        assertEquals(result.nextItem(), "six");
        assertEquals(result.nextRow(), QueryResult.Type.Inserted);
        assertEquals(result.nextItem(), "seven");
        assertEquals(result.nextRow(), QueryResult.Type.Inserted);
        assertEquals(result.nextItem(), "eight");
        assertEquals(result.nextRow(), QueryResult.Type.Inserted);
        assertEquals(result.nextItem(), "nine");
        assertEquals(result.nextRow(), QueryResult.Type.Inserted);
        assertEquals(result.nextItem(), "ten");
        assertEquals(result.nextRow(), QueryResult.Type.Inserted);
        assertEquals(result.nextItem(), "eleven");
        assertEquals(result.nextRow(), QueryResult.Type.Inserted);
        assertEquals(result.nextItem(), "twelve");
        assertEquals(result.nextRow(), QueryResult.Type.Inserted);
        assertEquals(result.nextItem(), "thirteen");
        assertEquals(result.nextRow(), QueryResult.Type.End);

        QueryTemplate notEqual = new QueryTemplate
          (list(reference(numbersReference, name)),
           numbersReference,
           not(equal(reference(numbersReference, number), parameter())));

        result = dbms.diff(tail, first, notEqual, 2);

        assertEquals(result.nextRow(), QueryResult.Type.Inserted);
        assertEquals(result.nextItem(), "one");
        assertEquals(result.nextRow(), QueryResult.Type.Inserted);
        assertEquals(result.nextItem(), "three");
        assertEquals(result.nextRow(), QueryResult.Type.Inserted);
        assertEquals(result.nextItem(), "four");
        assertEquals(result.nextRow(), QueryResult.Type.Inserted);
        assertEquals(result.nextItem(), "five");
        assertEquals(result.nextRow(), QueryResult.Type.Inserted);
        assertEquals(result.nextItem(), "six");
        assertEquals(result.nextRow(), QueryResult.Type.Inserted);
        assertEquals(result.nextItem(), "seven");
        assertEquals(result.nextRow(), QueryResult.Type.Inserted);
        assertEquals(result.nextItem(), "eight");
        assertEquals(result.nextRow(), QueryResult.Type.Inserted);
        assertEquals(result.nextItem(), "nine");
        assertEquals(result.nextRow(), QueryResult.Type.Inserted);
        assertEquals(result.nextItem(), "ten");
        assertEquals(result.nextRow(), QueryResult.Type.Inserted);
        assertEquals(result.nextItem(), "eleven");
        assertEquals(result.nextRow(), QueryResult.Type.Inserted);
        assertEquals(result.nextItem(), "twelve");
        assertEquals(result.nextRow(), QueryResult.Type.Inserted);
        assertEquals(result.nextItem(), "thirteen");
        assertEquals(result.nextRow(), QueryResult.Type.End);

        QueryTemplate greaterThanAndLessThanOrNotLessThanOrEqual
          = new QueryTemplate
          (list(reference(numbersReference, name)),
           numbersReference,
           or(and(greaterThan(reference(numbersReference, number),
                              parameter()),
                  lessThan(reference(numbersReference, number),
                           parameter())),
              not(lessThanOrEqual(reference(numbersReference, number),
                                  parameter()))));

        result = dbms.diff
          (tail, first, greaterThanAndLessThanOrNotLessThanOrEqual, 3, 7, 10);

        assertEquals(result.nextRow(), QueryResult.Type.Inserted);
        assertEquals(result.nextItem(), "four");
        assertEquals(result.nextRow(), QueryResult.Type.Inserted);
        assertEquals(result.nextItem(), "five");
        assertEquals(result.nextRow(), QueryResult.Type.Inserted);
        assertEquals(result.nextItem(), "six");
        assertEquals(result.nextRow(), QueryResult.Type.Inserted);
        assertEquals(result.nextItem(), "eleven");
        assertEquals(result.nextRow(), QueryResult.Type.Inserted);
        assertEquals(result.nextItem(), "twelve");
        assertEquals(result.nextRow(), QueryResult.Type.Inserted);
        assertEquals(result.nextItem(), "thirteen");
        assertEquals(result.nextRow(), QueryResult.Type.End);	
    }    
}

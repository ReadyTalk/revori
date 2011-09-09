package unittests;

import junit.framework.TestCase;

import org.junit.Test;

import static com.readytalk.oss.dbms.util.Util.list;
import static com.readytalk.oss.dbms.util.Util.cols;
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
import com.readytalk.oss.dbms.Column;
import com.readytalk.oss.dbms.Revisions;
import com.readytalk.oss.dbms.Table;
import com.readytalk.oss.dbms.Revision;
import com.readytalk.oss.dbms.RevisionBuilder;
import com.readytalk.oss.dbms.PatchTemplate;
import com.readytalk.oss.dbms.InsertTemplate;
import com.readytalk.oss.dbms.TableReference;
import com.readytalk.oss.dbms.QueryTemplate;
import com.readytalk.oss.dbms.QueryResult;
import com.readytalk.oss.dbms.DuplicateKeyResolution;

public class OperationTest extends TestCase{
    
    @Test
    public void testComparisons(){
    	
        Column<Integer> number = new Column<Integer>(Integer.class);
        Column<String> name = new Column<String>(String.class);
        Table numbers = new Table(cols(number));

        Revision tail = Revisions.Empty;

        PatchTemplate insert = new InsertTemplate
          (numbers,
           cols(number, name),
           list(parameter(), parameter()),
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

        QueryTemplate lessThan = new QueryTemplate
          (list(reference(numbersReference, name)),
           numbersReference,
           lessThan(reference(numbersReference, number),
                    parameter()));
        Object[] parameters = { 1 };

        QueryResult result = tail.diff(first, lessThan, parameters);

        assertEquals(result.nextRow(), QueryResult.Type.End);
        Object[] parameters1 = { 2 };

        result = tail.diff(first, lessThan, parameters1);

        assertEquals(result.nextRow(), QueryResult.Type.Inserted);
        assertEquals(result.nextItem(), "one");
        assertEquals(result.nextRow(), QueryResult.Type.End);
        Object[] parameters2 = { 6 };

        result = tail.diff(first, lessThan, parameters2);

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
        Object[] parameters3 = { 42 };

        result = tail.diff(first, lessThan, parameters3);

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
        Object[] parameters4 = { 13 };

        result = tail.diff(first, greaterThan, parameters4);

        assertEquals(result.nextRow(), QueryResult.Type.End);
        Object[] parameters5 = { 12 };

        result = tail.diff(first, greaterThan, parameters5);

        assertEquals(result.nextRow(), QueryResult.Type.Inserted);
        assertEquals(result.nextItem(), "thirteen");
        assertEquals(result.nextRow(), QueryResult.Type.End);
        Object[] parameters6 = { 11 };

        result = tail.diff(first, greaterThan, parameters6);

        assertEquals(result.nextRow(), QueryResult.Type.Inserted);
        assertEquals(result.nextItem(), "twelve");
        assertEquals(result.nextRow(), QueryResult.Type.Inserted);
        assertEquals(result.nextItem(), "thirteen");
        assertEquals(result.nextRow(), QueryResult.Type.End);

        QueryTemplate lessThanOrEqual = new QueryTemplate
          (list(reference(numbersReference, name)),
           numbersReference,
           lessThanOrEqual(reference(numbersReference, number), parameter()));
        Object[] parameters7 = { 0 };

        result = tail.diff(first, lessThanOrEqual, parameters7);

        assertEquals(result.nextRow(), QueryResult.Type.End);
        Object[] parameters8 = { 1 };

        result = tail.diff(first, lessThanOrEqual, parameters8);

        assertEquals(result.nextRow(), QueryResult.Type.Inserted);
        assertEquals(result.nextItem(), "one");
        assertEquals(result.nextRow(), QueryResult.Type.End);
        Object[] parameters9 = { 2 };

        result = tail.diff(first, lessThanOrEqual, parameters9);

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
        Object[] parameters10 = { 14 };

        result = tail.diff(first, greaterThanOrEqual, parameters10);

        assertEquals(result.nextRow(), QueryResult.Type.End);
        Object[] parameters11 = { 13 };

        result = tail.diff(first, greaterThanOrEqual, parameters11);

        assertEquals(result.nextRow(), QueryResult.Type.Inserted);
        assertEquals(result.nextItem(), "thirteen");
        assertEquals(result.nextRow(), QueryResult.Type.End);
        Object[] parameters12 = { 12 };

        result = tail.diff(first, greaterThanOrEqual, parameters12);

        assertEquals(result.nextRow(), QueryResult.Type.Inserted);
        assertEquals(result.nextItem(), "twelve");
        assertEquals(result.nextRow(), QueryResult.Type.Inserted);
        assertEquals(result.nextItem(), "thirteen");
        assertEquals(result.nextRow(), QueryResult.Type.End);

        QueryTemplate notEqual = new QueryTemplate
          (list(reference(numbersReference, name)),
           numbersReference,
           notEqual(reference(numbersReference, number), parameter()));
        Object[] parameters13 = { 4 };

        result = tail.diff(first, notEqual, parameters13);

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
        Column<Integer> number = new Column<Integer>(Integer.class);
        Column<String> name = new Column<String>(String.class);
        Table numbers = new Table(cols(number));

        Revision tail = Revisions.Empty;

        PatchTemplate insert = new InsertTemplate
          (numbers,
           cols(number, name),
           list(parameter(), parameter()),
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

        QueryTemplate greaterThanAndLessThan = new QueryTemplate
          (list(reference(numbersReference, name)),
           numbersReference,
           and(greaterThan(reference(numbersReference, number), parameter()),
               lessThan(reference(numbersReference, number), parameter())));
        Object[] parameters = { 8, 12 };

        QueryResult result = tail.diff(first, greaterThanAndLessThan, parameters);

        assertEquals(result.nextRow(), QueryResult.Type.Inserted);
        assertEquals(result.nextItem(), "nine");
        assertEquals(result.nextRow(), QueryResult.Type.Inserted);
        assertEquals(result.nextItem(), "ten");
        assertEquals(result.nextRow(), QueryResult.Type.Inserted);
        assertEquals(result.nextItem(), "eleven");
        assertEquals(result.nextRow(), QueryResult.Type.End);
        Object[] parameters1 = { 8, 8 };

        result = tail.diff(first, greaterThanAndLessThan, parameters1);

        assertEquals(result.nextRow(), QueryResult.Type.End);
        Object[] parameters2 = { 12, 8 };

        result = tail.diff(first, greaterThanAndLessThan, parameters2);

        assertEquals(result.nextRow(), QueryResult.Type.End);

        QueryTemplate lessThanOrGreaterThan = new QueryTemplate
          (list(reference(numbersReference, name)),
           numbersReference,
           or(lessThan(reference(numbersReference, number), parameter()),
              greaterThan(reference(numbersReference, number), parameter())));
        Object[] parameters3 = { 8, 12 };

        result = tail.diff(first, lessThanOrGreaterThan, parameters3);

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
        Object[] parameters4 = { 8, 8 };

        result = tail.diff(first, lessThanOrGreaterThan, parameters4);

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
        Object[] parameters5 = { 12, 8 };

        result = tail.diff(first, lessThanOrGreaterThan, parameters5);

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
        Object[] parameters6 = { 2 };

        result = tail.diff(first, notEqual, parameters6);

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
        Object[] parameters7 = { 3, 7, 10 };

        result = tail.diff(first, greaterThanAndLessThanOrNotLessThanOrEqual, parameters7);

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

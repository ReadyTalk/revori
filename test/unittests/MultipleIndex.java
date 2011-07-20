package unittests;

import junit.framework.TestCase;

import org.junit.Test;

import static com.readytalk.oss.dbms.util.Util.list;
import static org.junit.Assert.*;

import com.readytalk.oss.dbms.DBMS;
import com.readytalk.oss.dbms.BinaryOperation;
import com.readytalk.oss.dbms.Column;
import com.readytalk.oss.dbms.ConflictResolver;
import com.readytalk.oss.dbms.Index;
import com.readytalk.oss.dbms.Table;
import com.readytalk.oss.dbms.Expression;
import com.readytalk.oss.dbms.Revision;
import com.readytalk.oss.dbms.RevisionBuilder;
import com.readytalk.oss.dbms.PatchTemplate;
import com.readytalk.oss.dbms.UpdateTemplate;
import com.readytalk.oss.dbms.InsertTemplate;
import com.readytalk.oss.dbms.DeleteTemplate;
import com.readytalk.oss.dbms.Constant;
import com.readytalk.oss.dbms.TableReference;
import com.readytalk.oss.dbms.ColumnReference;
import com.readytalk.oss.dbms.QueryTemplate;
import com.readytalk.oss.dbms.QueryResult;
import com.readytalk.oss.dbms.Parameter;
import com.readytalk.oss.dbms.DuplicateKeyResolution;
import com.readytalk.oss.dbms.imp.MyDBMS;


public class MultipleIndex extends TestCase{
    @Test
    public void testMultipleIndexInserts(){
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

        Index nameIndex = new Index(numbers, list(name));

        builder.add(nameIndex);

        builder.apply(insert, 1, "one");
        builder.apply(insert, 2, "two");
        builder.apply(insert, 3, "three");
        builder.apply(insert, 4, "four");
        builder.apply(insert, 5, "five");
        builder.apply(insert, 6, "six");
        builder.apply(insert, 7, "seven");
        builder.apply(insert, 8, "eight");
        builder.apply(insert, 9, "nine");

        Revision first = builder.commit();

        TableReference numbersReference = new TableReference(numbers);
        
        QueryTemplate greaterThanAndLessThan = new QueryTemplate
          (list((Expression) new ColumnReference(numbersReference, name)),
           numbersReference,
           new BinaryOperation
           (BinaryOperation.Type.And,
            new BinaryOperation
            (BinaryOperation.Type.GreaterThan,
             new ColumnReference(numbersReference, name),
             new Parameter()),
            new BinaryOperation
            (BinaryOperation.Type.LessThan,
             new ColumnReference(numbersReference, name),
             new Parameter())));

        QueryResult result = dbms.diff
          (tail, first, greaterThanAndLessThan, "four", "two");

        // We assume here that, by defining a query which is implemented
        // most efficiently in terms of the index on numbers.name, the
        // DBMS will actually use that index to execute it, and thus we
        // will visit the results in alphabetical order.

        assertEquals(result.nextRow(), QueryResult.Type.Inserted);
        assertEquals(result.nextItem(), "nine");
        assertEquals(result.nextRow(), QueryResult.Type.Inserted);
        assertEquals(result.nextItem(), "one");
        assertEquals(result.nextRow(), QueryResult.Type.Inserted);
        assertEquals(result.nextItem(), "seven");
        assertEquals(result.nextRow(), QueryResult.Type.Inserted);
        assertEquals(result.nextItem(), "six");
        assertEquals(result.nextRow(), QueryResult.Type.Inserted);
        assertEquals(result.nextItem(), "three");
        assertEquals(result.nextRow(), QueryResult.Type.End);

        builder = dbms.builder(tail);

        builder.apply(insert, 1, "one");
        builder.apply(insert, 2, "two");
        builder.apply(insert, 3, "three");
        builder.apply(insert, 4, "four");

        builder.add(nameIndex);

        builder.apply(insert, 5, "five");
        builder.apply(insert, 6, "six");
        builder.apply(insert, 7, "seven");
        builder.apply(insert, 8, "eight");
        builder.apply(insert, 9, "nine");

        first = builder.commit();

        result = dbms.diff
          (tail, first, greaterThanAndLessThan, "four", "two");

        assertEquals(result.nextRow(), QueryResult.Type.Inserted);
        assertEquals(result.nextItem(), "nine");
        assertEquals(result.nextRow(), QueryResult.Type.Inserted);
        assertEquals(result.nextItem(), "one");
        assertEquals(result.nextRow(), QueryResult.Type.Inserted);
        assertEquals(result.nextItem(), "seven");
        assertEquals(result.nextRow(), QueryResult.Type.Inserted);
        assertEquals(result.nextItem(), "six");
        assertEquals(result.nextRow(), QueryResult.Type.Inserted);
        assertEquals(result.nextItem(), "three");
        assertEquals(result.nextRow(), QueryResult.Type.End);

        builder = dbms.builder(tail);

        builder.apply(insert, 1, "one");
        builder.apply(insert, 2, "two");
        builder.apply(insert, 3, "three");
        builder.apply(insert, 4, "four");
        builder.apply(insert, 5, "five");
        builder.apply(insert, 6, "six");
        builder.apply(insert, 7, "seven");
        builder.apply(insert, 8, "eight");
        builder.apply(insert, 9, "nine");

        builder.add(nameIndex);

        first = builder.commit();

        result = dbms.diff
          (tail, first, greaterThanAndLessThan, "four", "two");

        assertEquals(result.nextRow(), QueryResult.Type.Inserted);
        assertEquals(result.nextItem(), "nine");
        assertEquals(result.nextRow(), QueryResult.Type.Inserted);
        assertEquals(result.nextItem(), "one");
        assertEquals(result.nextRow(), QueryResult.Type.Inserted);
        assertEquals(result.nextItem(), "seven");
        assertEquals(result.nextRow(), QueryResult.Type.Inserted);
        assertEquals(result.nextItem(), "six");
        assertEquals(result.nextRow(), QueryResult.Type.Inserted);
        assertEquals(result.nextItem(), "three");
        assertEquals(result.nextRow(), QueryResult.Type.End);

        builder = dbms.builder(first);

        builder.remove(nameIndex);

        Revision second = builder.commit();

        result = dbms.diff
          (tail, second, greaterThanAndLessThan, "four", "two");

        assertEquals(result.nextRow(), QueryResult.Type.Inserted);
        assertEquals(result.nextItem(), "one");
        assertEquals(result.nextRow(), QueryResult.Type.Inserted);
        assertEquals(result.nextItem(), "three");
        assertEquals(result.nextRow(), QueryResult.Type.Inserted);
        assertEquals(result.nextItem(), "six");
        assertEquals(result.nextRow(), QueryResult.Type.Inserted);
        assertEquals(result.nextItem(), "seven");
        assertEquals(result.nextRow(), QueryResult.Type.Inserted);
        assertEquals(result.nextItem(), "nine");
        assertEquals(result.nextRow(), QueryResult.Type.End);
    }
    
    @Test
    public void testMultipleIndexUpdates(){
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
        builder.apply(insert, 4, "four");
        builder.apply(insert, 5, "five");
        builder.apply(insert, 6, "six");
        builder.apply(insert, 7, "seven");
        builder.apply(insert, 8, "eight");
        builder.apply(insert, 9, "nine");

        Index nameIndex = new Index(numbers, list(name));

        builder.add(nameIndex);

        TableReference numbersReference = new TableReference(numbers);

        PatchTemplate updateNameWhereNumberEqual = new UpdateTemplate
          (numbersReference,
           new BinaryOperation
           (BinaryOperation.Type.Equal,
            new ColumnReference(numbersReference, number),
            new Parameter()),
           list(name),
           list((Expression) new Parameter()));

        builder.apply(updateNameWhereNumberEqual, 1, "uno");
        builder.apply(updateNameWhereNumberEqual, 2, "dos");
        builder.apply(updateNameWhereNumberEqual, 3, "tres");
        builder.apply(updateNameWhereNumberEqual, 8, "ocho");

        Revision first = builder.commit();

        QueryTemplate greaterThanAndLessThan = new QueryTemplate
          (list((Expression) new ColumnReference(numbersReference, name)),
           numbersReference,
           new BinaryOperation
           (BinaryOperation.Type.And,
            new BinaryOperation
            (BinaryOperation.Type.GreaterThan,
             new ColumnReference(numbersReference, name),
             new Parameter()),
            new BinaryOperation
            (BinaryOperation.Type.LessThan,
             new ColumnReference(numbersReference, name),
             new Parameter())));

        QueryResult result = dbms.diff
          (tail, first, greaterThanAndLessThan, "four", "two");

        assertEquals(result.nextRow(), QueryResult.Type.Inserted);
        assertEquals(result.nextItem(), "nine");
        assertEquals(result.nextRow(), QueryResult.Type.Inserted);
        assertEquals(result.nextItem(), "ocho");
        assertEquals(result.nextRow(), QueryResult.Type.Inserted);
        assertEquals(result.nextItem(), "seven");
        assertEquals(result.nextRow(), QueryResult.Type.Inserted);
        assertEquals(result.nextItem(), "six");
        assertEquals(result.nextRow(), QueryResult.Type.Inserted);
        assertEquals(result.nextItem(), "tres");
        assertEquals(result.nextRow(), QueryResult.Type.End);

        builder = dbms.builder(first);

        builder.remove(nameIndex);

        Revision second = builder.commit();

        result = dbms.diff
          (tail, second, greaterThanAndLessThan, "four", "two");

        assertEquals(result.nextRow(), QueryResult.Type.Inserted);
        assertEquals(result.nextItem(), "tres");
        assertEquals(result.nextRow(), QueryResult.Type.Inserted);
        assertEquals(result.nextItem(), "six");
        assertEquals(result.nextRow(), QueryResult.Type.Inserted);
        assertEquals(result.nextItem(), "seven");
        assertEquals(result.nextRow(), QueryResult.Type.Inserted);
        assertEquals(result.nextItem(), "ocho");
        assertEquals(result.nextRow(), QueryResult.Type.Inserted);
        assertEquals(result.nextItem(), "nine");
        assertEquals(result.nextRow(), QueryResult.Type.End);
    }
    
    @Test
    public void testMultipleIndexDeletes(){
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
        builder.apply(insert, 4, "four");
        builder.apply(insert, 5, "five");
        builder.apply(insert, 6, "six");
        builder.apply(insert, 7, "seven");
        builder.apply(insert, 8, "eight");
        builder.apply(insert, 9, "nine");

        Index nameIndex = new Index(numbers, list(name));

        builder.add(nameIndex);

        TableReference numbersReference = new TableReference(numbers);

        PatchTemplate deleteWhereNumberEqual = new DeleteTemplate
          (numbersReference,
           new BinaryOperation
           (BinaryOperation.Type.Equal,
            new ColumnReference(numbersReference, number),
            new Parameter()));

        builder.apply(deleteWhereNumberEqual, 6);

        PatchTemplate deleteWhereNameEqual = new DeleteTemplate
          (numbersReference,
           new BinaryOperation
           (BinaryOperation.Type.Equal,
            new ColumnReference(numbersReference, name),
            new Parameter()));

        builder.apply(deleteWhereNameEqual, "four");

        Revision first = builder.commit();

        QueryTemplate greaterThanAndLessThanName = new QueryTemplate
          (list((Expression) new ColumnReference(numbersReference, name)),
           numbersReference,
           new BinaryOperation
           (BinaryOperation.Type.And,
            new BinaryOperation
            (BinaryOperation.Type.GreaterThan,
             new ColumnReference(numbersReference, name),
             new Parameter()),
            new BinaryOperation
            (BinaryOperation.Type.LessThan,
             new ColumnReference(numbersReference, name),
             new Parameter())));

        QueryResult result = dbms.diff
          (tail, first, greaterThanAndLessThanName, "f", "t");

        assertEquals(result.nextRow(), QueryResult.Type.Inserted);
        assertEquals(result.nextItem(), "five");
        assertEquals(result.nextRow(), QueryResult.Type.Inserted);
        assertEquals(result.nextItem(), "nine");
        assertEquals(result.nextRow(), QueryResult.Type.Inserted);
        assertEquals(result.nextItem(), "one");
        assertEquals(result.nextRow(), QueryResult.Type.Inserted);
        assertEquals(result.nextItem(), "seven");
        assertEquals(result.nextRow(), QueryResult.Type.End);

        QueryTemplate greaterThanAndLessThanNumber = new QueryTemplate
          (list((Expression) new ColumnReference(numbersReference, name)),
           numbersReference,
           new BinaryOperation
           (BinaryOperation.Type.And,
            new BinaryOperation
            (BinaryOperation.Type.GreaterThan,
             new ColumnReference(numbersReference, number),
             new Parameter()),
            new BinaryOperation
            (BinaryOperation.Type.LessThan,
             new ColumnReference(numbersReference, number),
             new Parameter())));

        result = dbms.diff(tail, first, greaterThanAndLessThanNumber, 2, 8);

        assertEquals(result.nextRow(), QueryResult.Type.Inserted);
        assertEquals(result.nextItem(), "three");
        assertEquals(result.nextRow(), QueryResult.Type.Inserted);
        assertEquals(result.nextItem(), "five");
        assertEquals(result.nextRow(), QueryResult.Type.Inserted);
        assertEquals(result.nextItem(), "seven");
        assertEquals(result.nextRow(), QueryResult.Type.End);

        builder = dbms.builder(first);

        builder.remove(nameIndex);

        Revision second = builder.commit();

        result = dbms.diff(tail, second, greaterThanAndLessThanName, "f", "t");

        assertEquals(result.nextRow(), QueryResult.Type.Inserted);
        assertEquals(result.nextItem(), "one");
        assertEquals(result.nextRow(), QueryResult.Type.Inserted);
        assertEquals(result.nextItem(), "five");
        assertEquals(result.nextRow(), QueryResult.Type.Inserted);
        assertEquals(result.nextItem(), "seven");
        assertEquals(result.nextRow(), QueryResult.Type.Inserted);
        assertEquals(result.nextItem(), "nine");
        assertEquals(result.nextRow(), QueryResult.Type.End);
    }
    
    @Test
    public void testMultipleIndexMerges(){
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

        Index nameIndex = new Index(numbers, list(name));

        builder.add(nameIndex);

        builder.apply(insert, 1, "one");
        builder.apply(insert, 2, "two");
        builder.apply(insert, 3, "three");
        builder.apply(insert, 4, "four");
        builder.apply(insert, 5, "five");

        Revision left = builder.commit();

        builder = dbms.builder(tail);

        builder.apply(insert, 4, "four");
        builder.apply(insert, 5, "five");
        builder.apply(insert, 6, "six");
        builder.apply(insert, 7, "seven");
        builder.apply(insert, 8, "eight");
        builder.apply(insert, 9, "nine");

        Revision right = builder.commit();

        Revision merge = dbms.merge(tail, left, right, new ConflictResolver() {
            public Object resolveConflict(Table table,
                                          Column column,
                                          Object[] primaryKeyValues,
                                          Object baseValue,
                                          Object leftValue,
                                          Object rightValue)
            {
              throw new RuntimeException();
            }
          }, null);

        TableReference numbersReference = new TableReference(numbers);

        QueryTemplate greaterThanAndLessThan = new QueryTemplate
          (list((Expression) new ColumnReference(numbersReference, name)),
           numbersReference,
           new BinaryOperation
           (BinaryOperation.Type.And,
            new BinaryOperation
            (BinaryOperation.Type.GreaterThan,
             new ColumnReference(numbersReference, name),
             new Parameter()),
            new BinaryOperation
            (BinaryOperation.Type.LessThan,
             new ColumnReference(numbersReference, name),
             new Parameter())));

        QueryResult result = dbms.diff
          (tail, merge, greaterThanAndLessThan, "four", "two");

        assertEquals(result.nextRow(), QueryResult.Type.Inserted);
        assertEquals(result.nextItem(), "nine");
        assertEquals(result.nextRow(), QueryResult.Type.Inserted);
        assertEquals(result.nextItem(), "one");
        assertEquals(result.nextRow(), QueryResult.Type.Inserted);
        assertEquals(result.nextItem(), "seven");
        assertEquals(result.nextRow(), QueryResult.Type.Inserted);
        assertEquals(result.nextItem(), "six");
        assertEquals(result.nextRow(), QueryResult.Type.Inserted);
        assertEquals(result.nextItem(), "three");
        assertEquals(result.nextRow(), QueryResult.Type.End);

        builder = dbms.builder(merge);

        builder.remove(nameIndex);

        Revision second = builder.commit();

        result = dbms.diff
          (tail, second, greaterThanAndLessThan, "four", "two");

        assertEquals(result.nextRow(), QueryResult.Type.Inserted);
        assertEquals(result.nextItem(), "one");
        assertEquals(result.nextRow(), QueryResult.Type.Inserted);
        assertEquals(result.nextItem(), "three");
        assertEquals(result.nextRow(), QueryResult.Type.Inserted);
        assertEquals(result.nextItem(), "six");
        assertEquals(result.nextRow(), QueryResult.Type.Inserted);
        assertEquals(result.nextItem(), "seven");
        assertEquals(result.nextRow(), QueryResult.Type.Inserted);
        assertEquals(result.nextItem(), "nine");
        assertEquals(result.nextRow(), QueryResult.Type.End);

        builder = dbms.builder(merge);

        PatchTemplate updateNameWhereNumberEqual = new UpdateTemplate
          (numbersReference,
           new BinaryOperation
           (BinaryOperation.Type.Equal,
            new ColumnReference(numbersReference, number),
            new Parameter()),
           list(name),
           list((Expression) new Parameter()));

        builder.apply(updateNameWhereNumberEqual, 1, "uno");
        builder.apply(updateNameWhereNumberEqual, 3, "tres");

        left = builder.commit();

        builder = dbms.builder(merge);

        PatchTemplate deleteWhereNumberEqual = new DeleteTemplate
          (numbersReference,
           new BinaryOperation
           (BinaryOperation.Type.Equal,
            new ColumnReference(numbersReference, number),
            new Parameter()));

        builder.apply(deleteWhereNumberEqual, 1);
        builder.apply(deleteWhereNumberEqual, 6);

        right = builder.commit();

        merge = dbms.merge(merge, left, right, new ConflictResolver() {
            public Object resolveConflict(Table table,
                                          Column column,
                                          Object[] primaryKeyValues,
                                          Object baseValue,
                                          Object leftValue,
                                          Object rightValue)
            {
              throw new RuntimeException();
            }
          }, null);

        result = dbms.diff
          (tail, merge, greaterThanAndLessThan, "four", "two");

        assertEquals(result.nextRow(), QueryResult.Type.Inserted);
        assertEquals(result.nextItem(), "nine");
        assertEquals(result.nextRow(), QueryResult.Type.Inserted);
        assertEquals(result.nextItem(), "seven");
        assertEquals(result.nextRow(), QueryResult.Type.Inserted);
        assertEquals(result.nextItem(), "tres");
        assertEquals(result.nextRow(), QueryResult.Type.End);

        builder = dbms.builder(merge);

        builder.remove(nameIndex);

        Revision third = builder.commit();

        result = dbms.diff
          (tail, third, greaterThanAndLessThan, "four", "two");

        assertEquals(result.nextRow(), QueryResult.Type.Inserted);
        assertEquals(result.nextItem(), "tres");
        assertEquals(result.nextRow(), QueryResult.Type.Inserted);
        assertEquals(result.nextItem(), "seven");
        assertEquals(result.nextRow(), QueryResult.Type.Inserted);
        assertEquals(result.nextItem(), "nine");
        assertEquals(result.nextRow(), QueryResult.Type.End);
    }
}

package unittests;

import org.junit.Test;

import static com.readytalk.oss.dbms.imp.Util.list;
import static org.junit.Assert.*;

import com.readytalk.oss.dbms.DBMS;
import com.readytalk.oss.dbms.DBMS.BinaryOperationType;
import com.readytalk.oss.dbms.DBMS.Column;
import com.readytalk.oss.dbms.DBMS.ConflictResolver;
import com.readytalk.oss.dbms.DBMS.Index;
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


public class MultipleIndex{
    @Test
    public void testMultipleIndexInserts(){
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

        Index nameIndex = dbms.index(numbers, list(name));

        dbms.add(context, nameIndex);

        dbms.apply(context, insert, 1, "one");
        dbms.apply(context, insert, 2, "two");
        dbms.apply(context, insert, 3, "three");
        dbms.apply(context, insert, 4, "four");
        dbms.apply(context, insert, 5, "five");
        dbms.apply(context, insert, 6, "six");
        dbms.apply(context, insert, 7, "seven");
        dbms.apply(context, insert, 8, "eight");
        dbms.apply(context, insert, 9, "nine");

        Revision first = dbms.commit(context);

        TableReference numbersReference = dbms.tableReference(numbers);
        
        QueryTemplate greaterThanAndLessThan = dbms.queryTemplate
          (list((Expression) dbms.columnReference(numbersReference, name)),
           numbersReference,
           dbms.operation
           (BinaryOperationType.And,
            dbms.operation
            (BinaryOperationType.GreaterThan,
             dbms.columnReference(numbersReference, name),
             dbms.parameter()),
            dbms.operation
            (BinaryOperationType.LessThan,
             dbms.columnReference(numbersReference, name),
             dbms.parameter())));

        QueryResult result = dbms.diff
          (tail, first, greaterThanAndLessThan, "four", "two");

        // We assume here that, by defining a query which is implemented
        // most efficiently in terms of the index on numbers.name, the
        // DBMS will actually use that index to execute it, and thus we
        // will visit the results in alphabetical order.

        assertEquals(result.nextRow(), ResultType.Inserted);
        assertEquals(result.nextItem(), "nine");
        assertEquals(result.nextRow(), ResultType.Inserted);
        assertEquals(result.nextItem(), "one");
        assertEquals(result.nextRow(), ResultType.Inserted);
        assertEquals(result.nextItem(), "seven");
        assertEquals(result.nextRow(), ResultType.Inserted);
        assertEquals(result.nextItem(), "six");
        assertEquals(result.nextRow(), ResultType.Inserted);
        assertEquals(result.nextItem(), "three");
        assertEquals(result.nextRow(), ResultType.End);

        context = dbms.patchContext(tail);

        dbms.apply(context, insert, 1, "one");
        dbms.apply(context, insert, 2, "two");
        dbms.apply(context, insert, 3, "three");
        dbms.apply(context, insert, 4, "four");

        dbms.add(context, nameIndex);

        dbms.apply(context, insert, 5, "five");
        dbms.apply(context, insert, 6, "six");
        dbms.apply(context, insert, 7, "seven");
        dbms.apply(context, insert, 8, "eight");
        dbms.apply(context, insert, 9, "nine");

        first = dbms.commit(context);

        result = dbms.diff
          (tail, first, greaterThanAndLessThan, "four", "two");

        assertEquals(result.nextRow(), ResultType.Inserted);
        assertEquals(result.nextItem(), "nine");
        assertEquals(result.nextRow(), ResultType.Inserted);
        assertEquals(result.nextItem(), "one");
        assertEquals(result.nextRow(), ResultType.Inserted);
        assertEquals(result.nextItem(), "seven");
        assertEquals(result.nextRow(), ResultType.Inserted);
        assertEquals(result.nextItem(), "six");
        assertEquals(result.nextRow(), ResultType.Inserted);
        assertEquals(result.nextItem(), "three");
        assertEquals(result.nextRow(), ResultType.End);

        context = dbms.patchContext(tail);

        dbms.apply(context, insert, 1, "one");
        dbms.apply(context, insert, 2, "two");
        dbms.apply(context, insert, 3, "three");
        dbms.apply(context, insert, 4, "four");
        dbms.apply(context, insert, 5, "five");
        dbms.apply(context, insert, 6, "six");
        dbms.apply(context, insert, 7, "seven");
        dbms.apply(context, insert, 8, "eight");
        dbms.apply(context, insert, 9, "nine");

        dbms.add(context, nameIndex);

        first = dbms.commit(context);

        result = dbms.diff
          (tail, first, greaterThanAndLessThan, "four", "two");

        assertEquals(result.nextRow(), ResultType.Inserted);
        assertEquals(result.nextItem(), "nine");
        assertEquals(result.nextRow(), ResultType.Inserted);
        assertEquals(result.nextItem(), "one");
        assertEquals(result.nextRow(), ResultType.Inserted);
        assertEquals(result.nextItem(), "seven");
        assertEquals(result.nextRow(), ResultType.Inserted);
        assertEquals(result.nextItem(), "six");
        assertEquals(result.nextRow(), ResultType.Inserted);
        assertEquals(result.nextItem(), "three");
        assertEquals(result.nextRow(), ResultType.End);

        context = dbms.patchContext(first);

        dbms.remove(context, nameIndex);

        Revision second = dbms.commit(context);

        result = dbms.diff
          (tail, second, greaterThanAndLessThan, "four", "two");

        assertEquals(result.nextRow(), ResultType.Inserted);
        assertEquals(result.nextItem(), "one");
        assertEquals(result.nextRow(), ResultType.Inserted);
        assertEquals(result.nextItem(), "three");
        assertEquals(result.nextRow(), ResultType.Inserted);
        assertEquals(result.nextItem(), "six");
        assertEquals(result.nextRow(), ResultType.Inserted);
        assertEquals(result.nextItem(), "seven");
        assertEquals(result.nextRow(), ResultType.Inserted);
        assertEquals(result.nextItem(), "nine");
        assertEquals(result.nextRow(), ResultType.End);
    }
    
    @Test
    public void testMultipleIndexUpdates(){
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
        dbms.apply(context, insert, 4, "four");
        dbms.apply(context, insert, 5, "five");
        dbms.apply(context, insert, 6, "six");
        dbms.apply(context, insert, 7, "seven");
        dbms.apply(context, insert, 8, "eight");
        dbms.apply(context, insert, 9, "nine");

        Index nameIndex = dbms.index(numbers, list(name));

        dbms.add(context, nameIndex);

        TableReference numbersReference = dbms.tableReference(numbers);

        PatchTemplate updateNameWhereNumberEqual = dbms.updateTemplate
          (numbersReference,
           dbms.operation
           (BinaryOperationType.Equal,
            dbms.columnReference(numbersReference, number),
            dbms.parameter()),
           list(name),
           list(dbms.parameter()));

        dbms.apply(context, updateNameWhereNumberEqual, 1, "uno");
        dbms.apply(context, updateNameWhereNumberEqual, 2, "dos");
        dbms.apply(context, updateNameWhereNumberEqual, 3, "tres");
        dbms.apply(context, updateNameWhereNumberEqual, 8, "ocho");

        Revision first = dbms.commit(context);

        QueryTemplate greaterThanAndLessThan = dbms.queryTemplate
          (list((Expression) dbms.columnReference(numbersReference, name)),
           numbersReference,
           dbms.operation
           (BinaryOperationType.And,
            dbms.operation
            (BinaryOperationType.GreaterThan,
             dbms.columnReference(numbersReference, name),
             dbms.parameter()),
            dbms.operation
            (BinaryOperationType.LessThan,
             dbms.columnReference(numbersReference, name),
             dbms.parameter())));

        QueryResult result = dbms.diff
          (tail, first, greaterThanAndLessThan, "four", "two");

        assertEquals(result.nextRow(), ResultType.Inserted);
        assertEquals(result.nextItem(), "nine");
        assertEquals(result.nextRow(), ResultType.Inserted);
        assertEquals(result.nextItem(), "ocho");
        assertEquals(result.nextRow(), ResultType.Inserted);
        assertEquals(result.nextItem(), "seven");
        assertEquals(result.nextRow(), ResultType.Inserted);
        assertEquals(result.nextItem(), "six");
        assertEquals(result.nextRow(), ResultType.Inserted);
        assertEquals(result.nextItem(), "tres");
        assertEquals(result.nextRow(), ResultType.End);

        context = dbms.patchContext(first);

        dbms.remove(context, nameIndex);

        Revision second = dbms.commit(context);

        result = dbms.diff
          (tail, second, greaterThanAndLessThan, "four", "two");

        assertEquals(result.nextRow(), ResultType.Inserted);
        assertEquals(result.nextItem(), "tres");
        assertEquals(result.nextRow(), ResultType.Inserted);
        assertEquals(result.nextItem(), "six");
        assertEquals(result.nextRow(), ResultType.Inserted);
        assertEquals(result.nextItem(), "seven");
        assertEquals(result.nextRow(), ResultType.Inserted);
        assertEquals(result.nextItem(), "ocho");
        assertEquals(result.nextRow(), ResultType.Inserted);
        assertEquals(result.nextItem(), "nine");
        assertEquals(result.nextRow(), ResultType.End);
    }
    
    @Test
    public void testMultipleIndexDeletes(){
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
        dbms.apply(context, insert, 4, "four");
        dbms.apply(context, insert, 5, "five");
        dbms.apply(context, insert, 6, "six");
        dbms.apply(context, insert, 7, "seven");
        dbms.apply(context, insert, 8, "eight");
        dbms.apply(context, insert, 9, "nine");

        Index nameIndex = dbms.index(numbers, list(name));

        dbms.add(context, nameIndex);

        TableReference numbersReference = dbms.tableReference(numbers);

        PatchTemplate deleteWhereNumberEqual = dbms.deleteTemplate
          (numbersReference,
           dbms.operation
           (BinaryOperationType.Equal,
            dbms.columnReference(numbersReference, number),
            dbms.parameter()));

        dbms.apply(context, deleteWhereNumberEqual, 6);

        PatchTemplate deleteWhereNameEqual = dbms.deleteTemplate
          (numbersReference,
           dbms.operation
           (BinaryOperationType.Equal,
            dbms.columnReference(numbersReference, name),
            dbms.parameter()));

        dbms.apply(context, deleteWhereNameEqual, "four");

        Revision first = dbms.commit(context);

        QueryTemplate greaterThanAndLessThanName = dbms.queryTemplate
          (list((Expression) dbms.columnReference(numbersReference, name)),
           numbersReference,
           dbms.operation
           (BinaryOperationType.And,
            dbms.operation
            (BinaryOperationType.GreaterThan,
             dbms.columnReference(numbersReference, name),
             dbms.parameter()),
            dbms.operation
            (BinaryOperationType.LessThan,
             dbms.columnReference(numbersReference, name),
             dbms.parameter())));

        QueryResult result = dbms.diff
          (tail, first, greaterThanAndLessThanName, "f", "t");

        assertEquals(result.nextRow(), ResultType.Inserted);
        assertEquals(result.nextItem(), "five");
        assertEquals(result.nextRow(), ResultType.Inserted);
        assertEquals(result.nextItem(), "nine");
        assertEquals(result.nextRow(), ResultType.Inserted);
        assertEquals(result.nextItem(), "one");
        assertEquals(result.nextRow(), ResultType.Inserted);
        assertEquals(result.nextItem(), "seven");
        assertEquals(result.nextRow(), ResultType.End);

        QueryTemplate greaterThanAndLessThanNumber = dbms.queryTemplate
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

        result = dbms.diff(tail, first, greaterThanAndLessThanNumber, 2, 8);

        assertEquals(result.nextRow(), ResultType.Inserted);
        assertEquals(result.nextItem(), "three");
        assertEquals(result.nextRow(), ResultType.Inserted);
        assertEquals(result.nextItem(), "five");
        assertEquals(result.nextRow(), ResultType.Inserted);
        assertEquals(result.nextItem(), "seven");
        assertEquals(result.nextRow(), ResultType.End);

        context = dbms.patchContext(first);

        dbms.remove(context, nameIndex);

        Revision second = dbms.commit(context);

        result = dbms.diff(tail, second, greaterThanAndLessThanName, "f", "t");

        assertEquals(result.nextRow(), ResultType.Inserted);
        assertEquals(result.nextItem(), "one");
        assertEquals(result.nextRow(), ResultType.Inserted);
        assertEquals(result.nextItem(), "five");
        assertEquals(result.nextRow(), ResultType.Inserted);
        assertEquals(result.nextItem(), "seven");
        assertEquals(result.nextRow(), ResultType.Inserted);
        assertEquals(result.nextItem(), "nine");
        assertEquals(result.nextRow(), ResultType.End);
    }
    
    @Test
    public void testMultipleIndexMerges(){
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

        Index nameIndex = dbms.index(numbers, list(name));

        dbms.add(context, nameIndex);

        dbms.apply(context, insert, 1, "one");
        dbms.apply(context, insert, 2, "two");
        dbms.apply(context, insert, 3, "three");
        dbms.apply(context, insert, 4, "four");
        dbms.apply(context, insert, 5, "five");

        Revision left = dbms.commit(context);

        context = dbms.patchContext(tail);

        dbms.apply(context, insert, 4, "four");
        dbms.apply(context, insert, 5, "five");
        dbms.apply(context, insert, 6, "six");
        dbms.apply(context, insert, 7, "seven");
        dbms.apply(context, insert, 8, "eight");
        dbms.apply(context, insert, 9, "nine");

        Revision right = dbms.commit(context);

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
          });

        TableReference numbersReference = dbms.tableReference(numbers);

        QueryTemplate greaterThanAndLessThan = dbms.queryTemplate
          (list((Expression) dbms.columnReference(numbersReference, name)),
           numbersReference,
           dbms.operation
           (BinaryOperationType.And,
            dbms.operation
            (BinaryOperationType.GreaterThan,
             dbms.columnReference(numbersReference, name),
             dbms.parameter()),
            dbms.operation
            (BinaryOperationType.LessThan,
             dbms.columnReference(numbersReference, name),
             dbms.parameter())));

        QueryResult result = dbms.diff
          (tail, merge, greaterThanAndLessThan, "four", "two");

        assertEquals(result.nextRow(), ResultType.Inserted);
        assertEquals(result.nextItem(), "nine");
        assertEquals(result.nextRow(), ResultType.Inserted);
        assertEquals(result.nextItem(), "one");
        assertEquals(result.nextRow(), ResultType.Inserted);
        assertEquals(result.nextItem(), "seven");
        assertEquals(result.nextRow(), ResultType.Inserted);
        assertEquals(result.nextItem(), "six");
        assertEquals(result.nextRow(), ResultType.Inserted);
        assertEquals(result.nextItem(), "three");
        assertEquals(result.nextRow(), ResultType.End);

        context = dbms.patchContext(merge);

        dbms.remove(context, nameIndex);

        Revision second = dbms.commit(context);

        result = dbms.diff
          (tail, second, greaterThanAndLessThan, "four", "two");

        assertEquals(result.nextRow(), ResultType.Inserted);
        assertEquals(result.nextItem(), "one");
        assertEquals(result.nextRow(), ResultType.Inserted);
        assertEquals(result.nextItem(), "three");
        assertEquals(result.nextRow(), ResultType.Inserted);
        assertEquals(result.nextItem(), "six");
        assertEquals(result.nextRow(), ResultType.Inserted);
        assertEquals(result.nextItem(), "seven");
        assertEquals(result.nextRow(), ResultType.Inserted);
        assertEquals(result.nextItem(), "nine");
        assertEquals(result.nextRow(), ResultType.End);

        context = dbms.patchContext(merge);

        PatchTemplate updateNameWhereNumberEqual = dbms.updateTemplate
          (numbersReference,
           dbms.operation
           (BinaryOperationType.Equal,
            dbms.columnReference(numbersReference, number),
            dbms.parameter()),
           list(name),
           list(dbms.parameter()));

        dbms.apply(context, updateNameWhereNumberEqual, 1, "uno");
        dbms.apply(context, updateNameWhereNumberEqual, 3, "tres");

        left = dbms.commit(context);

        context = dbms.patchContext(merge);

        PatchTemplate deleteWhereNumberEqual = dbms.deleteTemplate
          (numbersReference,
           dbms.operation
           (BinaryOperationType.Equal,
            dbms.columnReference(numbersReference, number),
            dbms.parameter()));

        dbms.apply(context, deleteWhereNumberEqual, 1);
        dbms.apply(context, deleteWhereNumberEqual, 6);

        right = dbms.commit(context);

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
          });

        result = dbms.diff
          (tail, merge, greaterThanAndLessThan, "four", "two");

        assertEquals(result.nextRow(), ResultType.Inserted);
        assertEquals(result.nextItem(), "nine");
        assertEquals(result.nextRow(), ResultType.Inserted);
        assertEquals(result.nextItem(), "seven");
        assertEquals(result.nextRow(), ResultType.Inserted);
        assertEquals(result.nextItem(), "tres");
        assertEquals(result.nextRow(), ResultType.End);

        context = dbms.patchContext(merge);

        dbms.remove(context, nameIndex);

        Revision third = dbms.commit(context);

        result = dbms.diff
          (tail, third, greaterThanAndLessThan, "four", "two");

        assertEquals(result.nextRow(), ResultType.Inserted);
        assertEquals(result.nextItem(), "tres");
        assertEquals(result.nextRow(), ResultType.Inserted);
        assertEquals(result.nextItem(), "seven");
        assertEquals(result.nextRow(), ResultType.Inserted);
        assertEquals(result.nextItem(), "nine");
        assertEquals(result.nextRow(), ResultType.End);
    }
}
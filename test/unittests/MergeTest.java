package unittests;

import junit.framework.TestCase;

import org.junit.Test;

import static com.readytalk.oss.dbms.imp.Util.list;
import static org.junit.Assert.*;

import com.readytalk.oss.dbms.DBMS;
import com.readytalk.oss.dbms.DBMS.BinaryOperationType;
import com.readytalk.oss.dbms.DBMS.Column;
import com.readytalk.oss.dbms.DBMS.ConflictResolver;
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

public class MergeTest extends TestCase{
    
    @Test
    public void testMerges(){
    	DBMS dbms = new MyDBMS();

        final Column number = dbms.column(Integer.class);
        final Column name = dbms.column(String.class);
        final Table numbers = dbms.table(list(number));

        Revision tail = dbms.revision();

        PatchTemplate insert = dbms.insertTemplate
          (numbers,
           list(number, name),
           list(dbms.parameter(),
                dbms.parameter()), DuplicateKeyResolution.Throw);

        PatchContext context = dbms.patchContext(tail);

        dbms.apply(context, insert,  1, "one");
        dbms.apply(context, insert,  2, "two");
        dbms.apply(context, insert,  6, "six");
        dbms.apply(context, insert,  7, "seven");
        dbms.apply(context, insert,  8, "eight");
        dbms.apply(context, insert,  9, "nine");
        dbms.apply(context, insert, 13, "thirteen");

        Revision base = dbms.commit(context);

        context = dbms.patchContext(base);

        dbms.apply(context, insert, 4, "four");

        Revision left = dbms.commit(context);

        TableReference numbersReference = dbms.tableReference(numbers);

        PatchTemplate update = dbms.updateTemplate
          (numbersReference,
           dbms.operation
           (BinaryOperationType.Equal,
            dbms.columnReference(numbersReference, number),
            dbms.parameter()),
           list(name),
           list(dbms.parameter()));

        context = dbms.patchContext(base);

        dbms.apply(context, update,  6, "roku");
        dbms.apply(context, insert, 42, "forty two");

        Revision right = dbms.commit(context);

        Revision merge = dbms.merge(base, left, right, null);

        QueryTemplate any = dbms.queryTemplate
          (list((Expression) dbms.columnReference(numbersReference, name)),
           numbersReference,
           dbms.constant(true));

        QueryResult result = dbms.diff(tail, merge, any);

        assertEquals(result.nextRow(), ResultType.Inserted);
        assertEquals(result.nextItem(), "one");
        assertEquals(result.nextRow(), ResultType.Inserted);
        assertEquals(result.nextItem(), "two");
        assertEquals(result.nextRow(), ResultType.Inserted);
        assertEquals(result.nextItem(), "four");
        assertEquals(result.nextRow(), ResultType.Inserted);
        assertEquals(result.nextItem(), "roku");
        assertEquals(result.nextRow(), ResultType.Inserted);
        assertEquals(result.nextItem(), "seven");
        assertEquals(result.nextRow(), ResultType.Inserted);
        assertEquals(result.nextItem(), "eight");
        assertEquals(result.nextRow(), ResultType.Inserted);
        assertEquals(result.nextItem(), "nine");
        assertEquals(result.nextRow(), ResultType.Inserted);
        assertEquals(result.nextItem(), "thirteen");
        assertEquals(result.nextRow(), ResultType.Inserted);
        assertEquals(result.nextItem(), "forty two");
        assertEquals(result.nextRow(), ResultType.End);
        
        context = dbms.patchContext(base);

        dbms.apply(context, insert, 4, "four");

        left = dbms.commit(context);

        context = dbms.patchContext(base);

        dbms.apply(context, insert, 4, "four");

        right = dbms.commit(context);

        merge = dbms.merge(base, left, right, null);

        result = dbms.diff(base, merge, any);

        assertEquals(result.nextRow(), ResultType.Inserted);
        assertEquals(result.nextItem(), "four");
        assertEquals(result.nextRow(), ResultType.End);
        
        PatchTemplate delete = dbms.deleteTemplate
          (numbersReference,
           dbms.operation
           (BinaryOperationType.Equal,
            dbms.columnReference(numbersReference, number),
            dbms.parameter()));

        context = dbms.patchContext(base);

        dbms.apply(context, delete, 8);

        left = dbms.commit(context);

        context = dbms.patchContext(base);

        dbms.apply(context, update, 8, "hachi");

        right = dbms.commit(context);

        merge = dbms.merge(base, left, right, null);

        result = dbms.diff(base, merge, any);

        assertEquals(result.nextRow(), ResultType.Deleted);
        assertEquals(result.nextItem(), "eight");
        assertEquals(result.nextRow(), ResultType.End);

        context = dbms.patchContext(base);

        dbms.apply(context, insert, 4, "four");

        left = dbms.commit(context);

        context = dbms.patchContext(base);

        dbms.apply(context, insert, 4, "shi");

        right = dbms.commit(context);

        merge = dbms.merge(base, left, right, new ConflictResolver() {
            public Object resolveConflict(Table table,
                                          Column column,
                                          Object[] primaryKeyValues,
                                          Object baseValue,
                                          Object leftValue,
                                          Object rightValue)
            {
              assertEquals(table, numbers);
              assertEquals(column, name);
              assertEquals(primaryKeyValues.length, 1);
              assertEquals(primaryKeyValues[0], 4);
              assertEquals(baseValue, null);
              assertEquals(leftValue, "four");
              assertEquals(rightValue, "shi");
              
              return "quatro";
            }
          });

        result = dbms.diff(base, merge, any);

        assertEquals(result.nextRow(), ResultType.Inserted);
        assertEquals(result.nextItem(), "quatro");
        assertEquals(result.nextRow(), ResultType.End);

        context = dbms.patchContext(base);

        dbms.apply(context, update, 1, "ichi");

        left = dbms.commit(context);

        context = dbms.patchContext(base);

        dbms.apply(context, update, 1, "uno");

        right = dbms.commit(context);

        merge = dbms.merge(base, left, right, new ConflictResolver() {
            public Object resolveConflict(Table table,
                                          Column column,
                                          Object[] primaryKeyValues,
                                          Object baseValue,
                                          Object leftValue,
                                          Object rightValue)
            {
              assertEquals(table, numbers);
              assertEquals(column, name);
              assertEquals(primaryKeyValues.length, 1);
              assertEquals(primaryKeyValues[0], 1);
              assertEquals(baseValue, "one");
              assertEquals(leftValue, "ichi");
              assertEquals(rightValue, "uno");
              
              return "unit";
            }
          });

        result = dbms.diff(base, merge, any);

        assertEquals(result.nextRow(), ResultType.Deleted);
        assertEquals(result.nextItem(), "one");
        assertEquals(result.nextRow(), ResultType.Inserted);
        assertEquals(result.nextItem(), "unit");
        assertEquals(result.nextRow(), ResultType.End);

        context = dbms.patchContext(tail);

        dbms.apply(context, insert, 1, "one");

        Revision t1 = dbms.commit(context);

        context = dbms.patchContext(tail);

        dbms.apply(context, insert, 1, "uno");

        Revision t2 = dbms.commit(context);

        merge = dbms.merge(tail, t1, t2, new ConflictResolver() {
            public Object resolveConflict(Table table,
                                          Column column,
                                          Object[] primaryKeyValues,
                                          Object baseValue,
                                          Object leftValue,
                                          Object rightValue)
            {
              assertEquals(table, numbers);
              assertEquals(column, name);
              assertEquals(primaryKeyValues.length, 1);
              assertEquals(primaryKeyValues[0], 1);
              assertEquals(baseValue, null);
              assertEquals(leftValue, "one");
              assertEquals(rightValue, "uno");
              
              return "unit";
            }
          });

        result = dbms.diff(tail, merge, any);

        assertEquals(result.nextRow(), ResultType.Inserted);
        assertEquals(result.nextItem(), "unit");
        assertEquals(result.nextRow(), ResultType.End);

        context = dbms.patchContext(tail);

        dbms.apply(context, insert, 1, "one");
        dbms.apply(context, insert, 2, "two");

        t1 = dbms.commit(context);

        context = dbms.patchContext(tail);

        dbms.apply(context, insert, 1, "uno");
        dbms.apply(context, insert, 3, "tres");

        t2 = dbms.commit(context);

        merge = dbms.merge(tail, t1, t2, new ConflictResolver() {
            public Object resolveConflict(Table table,
                                          Column column,
                                          Object[] primaryKeyValues,
                                          Object baseValue,
                                          Object leftValue,
                                          Object rightValue)
            {
              assertEquals(table, numbers);
              assertEquals(column, name);
              assertEquals(primaryKeyValues.length, 1);
              assertEquals(primaryKeyValues[0], 1);
              assertEquals(baseValue, null);
              assertEquals(leftValue, "one");
              assertEquals(rightValue, "uno");
              
              return "unit";
            }
          });

        result = dbms.diff(tail, merge, any);

        assertEquals(result.nextRow(), ResultType.Inserted);
        assertEquals(result.nextItem(), "unit");
        assertEquals(result.nextRow(), ResultType.Inserted);
        assertEquals(result.nextItem(), "two");
        assertEquals(result.nextRow(), ResultType.Inserted);
        assertEquals(result.nextItem(), "tres");
        assertEquals(result.nextRow(), ResultType.End);
    }
}
package unittests;

import junit.framework.TestCase;

import org.junit.Test;

import static com.readytalk.revori.util.Util.list;
import static com.readytalk.revori.util.Util.cols;

import com.readytalk.revori.BinaryOperation;
import com.readytalk.revori.Column;
import com.readytalk.revori.ConflictResolver;
import com.readytalk.revori.Table;
import com.readytalk.revori.Expression;
import com.readytalk.revori.Revision;
import com.readytalk.revori.Revisions;
import com.readytalk.revori.RevisionBuilder;
import com.readytalk.revori.PatchTemplate;
import com.readytalk.revori.InsertTemplate;
import com.readytalk.revori.UpdateTemplate;
import com.readytalk.revori.DeleteTemplate;
import com.readytalk.revori.TableReference;
import com.readytalk.revori.Parameter;
import com.readytalk.revori.Constant;
import com.readytalk.revori.ColumnReference;
import com.readytalk.revori.QueryTemplate;
import com.readytalk.revori.QueryResult;
import com.readytalk.revori.DuplicateKeyResolution;

public class MergeTest extends TestCase{
    
    @Test
    public void testMerges(){

        final Column<Integer> number = new Column<Integer>(Integer.class);
        final Column<String> name = new Column<String>(String.class);
        final Table numbers = new Table(cols(number));

        Revision tail = Revisions.Empty;

        PatchTemplate insert = new InsertTemplate
          (numbers,
           cols(number, name),
           list((Expression) new Parameter(), new Parameter()),
           DuplicateKeyResolution.Throw);

        RevisionBuilder builder = tail.builder();

        builder.apply(insert,  1, "one");
        builder.apply(insert,  2, "two");
        builder.apply(insert,  6, "six");
        builder.apply(insert,  7, "seven");
        builder.apply(insert,  8, "eight");
        builder.apply(insert,  9, "nine");
        builder.apply(insert, 13, "thirteen");

        Revision base = builder.commit();

        builder = base.builder();

        builder.apply(insert, 4, "four");

        Revision left = builder.commit();

        TableReference numbersReference = new TableReference(numbers);

        PatchTemplate update = new UpdateTemplate
          (numbersReference,
           new BinaryOperation
           (BinaryOperation.Type.Equal,
            new ColumnReference(numbersReference, number),
            new Parameter()),
           cols(name),
           list((Expression) new Parameter()));

        builder = base.builder();

        builder.apply(update,  6, "roku");
        builder.apply(insert, 42, "forty two");

        Revision right = builder.commit();

        Revision merge = base.merge(left, right, null, null);

        QueryTemplate any = new QueryTemplate
          (list((Expression) new ColumnReference(numbersReference, name)),
           numbersReference,
           new Constant(true));

        QueryResult result = tail.diff(merge, any);

        assertEquals(result.nextRow(), QueryResult.Type.Inserted);
        assertEquals(result.nextItem(), "one");
        assertEquals(result.nextRow(), QueryResult.Type.Inserted);
        assertEquals(result.nextItem(), "two");
        assertEquals(result.nextRow(), QueryResult.Type.Inserted);
        assertEquals(result.nextItem(), "four");
        assertEquals(result.nextRow(), QueryResult.Type.Inserted);
        assertEquals(result.nextItem(), "roku");
        assertEquals(result.nextRow(), QueryResult.Type.Inserted);
        assertEquals(result.nextItem(), "seven");
        assertEquals(result.nextRow(), QueryResult.Type.Inserted);
        assertEquals(result.nextItem(), "eight");
        assertEquals(result.nextRow(), QueryResult.Type.Inserted);
        assertEquals(result.nextItem(), "nine");
        assertEquals(result.nextRow(), QueryResult.Type.Inserted);
        assertEquals(result.nextItem(), "thirteen");
        assertEquals(result.nextRow(), QueryResult.Type.Inserted);
        assertEquals(result.nextItem(), "forty two");
        assertEquals(result.nextRow(), QueryResult.Type.End);
        
        builder = base.builder();

        builder.apply(insert, 4, "four");

        left = builder.commit();

        builder = base.builder();

        builder.apply(insert, 4, "four");

        right = builder.commit();

        merge = base.merge(left, right, null, null);

        result = base.diff(merge, any);

        assertEquals(result.nextRow(), QueryResult.Type.Inserted);
        assertEquals(result.nextItem(), "four");
        assertEquals(result.nextRow(), QueryResult.Type.End);
        
        PatchTemplate delete = new DeleteTemplate
          (numbersReference,
           new BinaryOperation
           (BinaryOperation.Type.Equal,
            new ColumnReference(numbersReference, number),
            new Parameter()));

        builder = base.builder();

        builder.apply(delete, 8);

        left = builder.commit();

        builder = base.builder();

        builder.apply(update, 8, "hachi");

        right = builder.commit();

        merge = base.merge(left, right, null, null);

        result = base.diff(merge, any);

        assertEquals(result.nextRow(), QueryResult.Type.Deleted);
        assertEquals(result.nextItem(), "eight");
        assertEquals(result.nextRow(), QueryResult.Type.End);

        builder = base.builder();

        builder.apply(insert, 4, "four");

        left = builder.commit();

        builder = base.builder();

        builder.apply(insert, 4, "shi");

        right = builder.commit();

        merge = base.merge(left, right, new ConflictResolver() {
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
          }, null);

        result = base.diff(merge, any);

        assertEquals(result.nextRow(), QueryResult.Type.Inserted);
        assertEquals(result.nextItem(), "quatro");
        assertEquals(result.nextRow(), QueryResult.Type.End);

        builder = base.builder();

        builder.apply(update, 1, "ichi");

        left = builder.commit();

        builder = base.builder();

        builder.apply(update, 1, "uno");

        right = builder.commit();

        merge = base.merge(left, right, new ConflictResolver() {
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
          }, null);

        result = base.diff(merge, any);

        assertEquals(result.nextRow(), QueryResult.Type.Deleted);
        assertEquals(result.nextItem(), "one");
        assertEquals(result.nextRow(), QueryResult.Type.Inserted);
        assertEquals(result.nextItem(), "unit");
        assertEquals(result.nextRow(), QueryResult.Type.End);

        builder = tail.builder();

        builder.apply(insert, 1, "one");

        Revision t1 = builder.commit();

        builder = tail.builder();

        builder.apply(insert, 1, "uno");

        Revision t2 = builder.commit();

        merge = tail.merge(t1, t2, new ConflictResolver() {
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
          }, null);

        result = tail.diff(merge, any);

        assertEquals(result.nextRow(), QueryResult.Type.Inserted);
        assertEquals(result.nextItem(), "unit");
        assertEquals(result.nextRow(), QueryResult.Type.End);

        builder = tail.builder();

        builder.apply(insert, 1, "one");
        builder.apply(insert, 2, "two");

        t1 = builder.commit();

        builder = tail.builder();

        builder.apply(insert, 1, "uno");
        builder.apply(insert, 3, "tres");

        t2 = builder.commit();

        merge = tail.merge(t1, t2, new ConflictResolver() {
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
          }, null);

        result = tail.diff(merge, any);

        assertEquals(result.nextRow(), QueryResult.Type.Inserted);
        assertEquals(result.nextItem(), "unit");
        assertEquals(result.nextRow(), QueryResult.Type.Inserted);
        assertEquals(result.nextItem(), "two");
        assertEquals(result.nextRow(), QueryResult.Type.Inserted);
        assertEquals(result.nextItem(), "tres");
        assertEquals(result.nextRow(), QueryResult.Type.End);
    }
}

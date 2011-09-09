package unittests;

import junit.framework.TestCase;

import org.junit.Test;

import static com.readytalk.oss.dbms.util.Util.list;
import static com.readytalk.oss.dbms.util.Util.cols;

import static com.readytalk.oss.dbms.ExpressionFactory.reference;

import com.readytalk.oss.dbms.BinaryOperation;
import com.readytalk.oss.dbms.Column;
import com.readytalk.oss.dbms.Revisions;
import com.readytalk.oss.dbms.Table;
import com.readytalk.oss.dbms.Expression;
import com.readytalk.oss.dbms.Revision;
import com.readytalk.oss.dbms.RevisionBuilder;
import com.readytalk.oss.dbms.Parameter;
import com.readytalk.oss.dbms.PatchTemplate;
import com.readytalk.oss.dbms.InsertTemplate;
import com.readytalk.oss.dbms.UpdateTemplate;
import com.readytalk.oss.dbms.DeleteTemplate;
import com.readytalk.oss.dbms.TableReference;
import com.readytalk.oss.dbms.QueryTemplate;
import com.readytalk.oss.dbms.QueryResult;
import com.readytalk.oss.dbms.DuplicateKeyResolution;

public class PartialIndexTest extends TestCase{
    @Test
    public void testUpdateOnPartialIndex(){
        Column<Integer> number = new Column<Integer>(Integer.class);
        Column<String> color = new Column<String>(String.class);
        Column<String> shape = new Column<String>(String.class);
        Table numbers = new Table(cols(number, color));

        Revision tail = Revisions.Empty;

        PatchTemplate insert = new InsertTemplate
          (numbers,
           cols(number, color, shape),
           list((Expression) new Parameter(),
                new Parameter(),
                new Parameter()), DuplicateKeyResolution.Throw);

        RevisionBuilder builder = tail.builder();

        builder.apply(insert, 1, "red", "triangle");
        builder.apply(insert, 1, "green", "circle");
        builder.apply(insert, 2, "yellow", "circle");
        builder.apply(insert, 3, "blue", "square");
        builder.apply(insert, 3, "orange", "square");

        Revision first = builder.commit();

        TableReference numbersReference = new TableReference(numbers);

        QueryTemplate numberEqual = new QueryTemplate
          (list(reference(numbersReference, color),
                reference(numbersReference, shape)),
           numbersReference,
           new BinaryOperation
           (BinaryOperation.Type.Equal,
            reference(numbersReference, number),
            new Parameter()));
        Object[] parameters = { 1 };

        QueryResult result = tail.diff(first, numberEqual, parameters);

        assertEquals(result.nextRow(), QueryResult.Type.Inserted);
        assertEquals(result.nextItem(), "green");
        assertEquals(result.nextItem(), "circle");
        assertEquals(result.nextRow(), QueryResult.Type.Inserted);
        assertEquals(result.nextItem(), "red");
        assertEquals(result.nextItem(), "triangle");
        assertEquals(result.nextRow(), QueryResult.Type.End);

        PatchTemplate updateShapeWhereNumberEqual = new UpdateTemplate
          (numbersReference,
           new BinaryOperation
           (BinaryOperation.Type.Equal,
            reference(numbersReference, number),
            new Parameter()),
           cols(shape),
           list((Expression) new Parameter()));

        builder = first.builder();

        builder.apply(updateShapeWhereNumberEqual, 1, "pentagon");

        Revision second = builder.commit();
        Object[] parameters1 = { 1 };

        result = tail.diff(second, numberEqual, parameters1);

        assertEquals(result.nextRow(), QueryResult.Type.Inserted);
        assertEquals(result.nextItem(), "green");
        assertEquals(result.nextItem(), "pentagon");
        assertEquals(result.nextRow(), QueryResult.Type.Inserted);
        assertEquals(result.nextItem(), "red");
        assertEquals(result.nextItem(), "pentagon");
        assertEquals(result.nextRow(), QueryResult.Type.End);
    }

    @Test
    public void testDeleteOnPartialIndex(){
        Column<Integer> number = new Column<Integer>(Integer.class);
        Column<String> color = new Column<String>(String.class);
        Column<String> shape = new Column<String>(String.class);
        Table numbers = new Table(cols(number, color));

        Revision tail = Revisions.Empty;

        PatchTemplate insert = new InsertTemplate
          (numbers,
           cols(number, color, shape),
           list((Expression) new Parameter(),
                new Parameter(),
                new Parameter()), DuplicateKeyResolution.Throw);

        RevisionBuilder builder = tail.builder();

        builder.apply(insert, 1, "red", "triangle");
        builder.apply(insert, 1, "green", "circle");
        builder.apply(insert, 2, "yellow", "circle");
        builder.apply(insert, 3, "blue", "square");
        builder.apply(insert, 3, "orange", "square");

        Revision first = builder.commit();

        TableReference numbersReference = new TableReference(numbers);

        QueryTemplate numberEqual = new QueryTemplate
          (list(reference(numbersReference, color),
                reference(numbersReference, shape)),
           numbersReference,
           new BinaryOperation
           (BinaryOperation.Type.Equal,
            reference(numbersReference, number),
            new Parameter()));
        Object[] parameters = { 1 };

        QueryResult result = tail.diff(first, numberEqual, parameters);

        assertEquals(result.nextRow(), QueryResult.Type.Inserted);
        assertEquals(result.nextItem(), "green");
        assertEquals(result.nextItem(), "circle");
        assertEquals(result.nextRow(), QueryResult.Type.Inserted);
        assertEquals(result.nextItem(), "red");
        assertEquals(result.nextItem(), "triangle");
        assertEquals(result.nextRow(), QueryResult.Type.End);

        PatchTemplate deleteWhereNumberEqual = new DeleteTemplate
          (numbersReference,
           new BinaryOperation
           (BinaryOperation.Type.Equal,
            reference(numbersReference, number),
            new Parameter()));

        builder = first.builder();

        builder.apply(deleteWhereNumberEqual, 1);

        Revision second = builder.commit();
        Object[] parameters1 = { 1 };

        result = tail.diff(second, numberEqual, parameters1);

        assertEquals(result.nextRow(), QueryResult.Type.End);
    }
}

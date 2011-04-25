package unittests;

import junit.framework.TestCase;

import org.junit.Test;

import static com.readytalk.oss.dbms.util.Util.list;
import static org.junit.Assert.*;

import com.readytalk.oss.dbms.DBMS;
import com.readytalk.oss.dbms.BinaryOperation;
import com.readytalk.oss.dbms.Column;
import com.readytalk.oss.dbms.Table;
import com.readytalk.oss.dbms.Expression;
import com.readytalk.oss.dbms.Revision;
import com.readytalk.oss.dbms.RevisionBuilder;
import com.readytalk.oss.dbms.Parameter;
import com.readytalk.oss.dbms.ColumnReference;
import com.readytalk.oss.dbms.PatchTemplate;
import com.readytalk.oss.dbms.InsertTemplate;
import com.readytalk.oss.dbms.UpdateTemplate;
import com.readytalk.oss.dbms.DeleteTemplate;
import com.readytalk.oss.dbms.TableReference;
import com.readytalk.oss.dbms.QueryTemplate;
import com.readytalk.oss.dbms.QueryResult;
import com.readytalk.oss.dbms.DuplicateKeyResolution;
import com.readytalk.oss.dbms.imp.MyDBMS;

public class PartialIndexTest extends TestCase{
    @Test
    public void testUpdateOnPartialIndex(){
    	DBMS dbms = new MyDBMS();

        Column number = new Column(Integer.class);
        Column color = new Column(String.class);
        Column shape = new Column(String.class);
        Table numbers = new Table(list(number, color));

        Revision tail = dbms.revision();

        PatchTemplate insert = new InsertTemplate
          (numbers,
           list(number, color, shape),
           list((Expression) new Parameter(),
                new Parameter(),
                new Parameter()), DuplicateKeyResolution.Throw);

        RevisionBuilder builder = dbms.builder(tail);

        builder.apply(insert, 1, "red", "triangle");
        builder.apply(insert, 1, "green", "circle");
        builder.apply(insert, 2, "yellow", "circle");
        builder.apply(insert, 3, "blue", "square");
        builder.apply(insert, 3, "orange", "square");

        Revision first = builder.commit();

        TableReference numbersReference = new TableReference(numbers);

        QueryTemplate numberEqual = new QueryTemplate
          (list((Expression) new ColumnReference(numbersReference, color),
                (Expression) new ColumnReference(numbersReference, shape)),
           numbersReference,
           new BinaryOperation
           (BinaryOperation.Type.Equal,
            new ColumnReference(numbersReference, number),
            new Parameter()));

        QueryResult result = dbms.diff(tail, first, numberEqual, 1);

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
            new ColumnReference(numbersReference, number),
            new Parameter()),
           list(shape),
           list((Expression) new Parameter()));

        builder = dbms.builder(first);

        builder.apply(updateShapeWhereNumberEqual, 1, "pentagon");

        Revision second = builder.commit();

        result = dbms.diff(tail, second, numberEqual, 1);

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
    	DBMS dbms = new MyDBMS();

        Column number = new Column(Integer.class);
        Column color = new Column(String.class);
        Column shape = new Column(String.class);
        Table numbers = new Table(list(number, color));

        Revision tail = dbms.revision();

        PatchTemplate insert = new InsertTemplate
          (numbers,
           list(number, color, shape),
           list((Expression) new Parameter(),
                new Parameter(),
                new Parameter()), DuplicateKeyResolution.Throw);

        RevisionBuilder builder = dbms.builder(tail);

        builder.apply(insert, 1, "red", "triangle");
        builder.apply(insert, 1, "green", "circle");
        builder.apply(insert, 2, "yellow", "circle");
        builder.apply(insert, 3, "blue", "square");
        builder.apply(insert, 3, "orange", "square");

        Revision first = builder.commit();

        TableReference numbersReference = new TableReference(numbers);

        QueryTemplate numberEqual = new QueryTemplate
          (list((Expression) new ColumnReference(numbersReference, color),
                (Expression) new ColumnReference(numbersReference, shape)),
           numbersReference,
           new BinaryOperation
           (BinaryOperation.Type.Equal,
            new ColumnReference(numbersReference, number),
            new Parameter()));

        QueryResult result = dbms.diff(tail, first, numberEqual, 1);

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
            new ColumnReference(numbersReference, number),
            new Parameter()));

        builder = dbms.builder(first);

        builder.apply(deleteWhereNumberEqual, 1);

        Revision second = builder.commit();

        result = dbms.diff(tail, second, numberEqual, 1);

        assertEquals(result.nextRow(), QueryResult.Type.End);
    }
}

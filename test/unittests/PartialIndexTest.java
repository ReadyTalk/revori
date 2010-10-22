package unittests;

import junit.framework.TestCase;

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
import com.readytalk.oss.dbms.imp.MyDBMS;

public class PartialIndexTest extends TestCase{
    @Test
    public void testUpdateOnPartialIndex(){
    	DBMS dbms = new MyDBMS();

        Column number = dbms.column(Integer.class);
        Column color = dbms.column(String.class);
        Column shape = dbms.column(String.class);
        Table numbers = dbms.table(list(number, color));

        Revision tail = dbms.revision();

        PatchTemplate insert = dbms.insertTemplate
          (numbers,
           list(number, color, shape),
           list(dbms.parameter(),
                dbms.parameter(),
                dbms.parameter()), DuplicateKeyResolution.Throw);

        PatchContext context = dbms.patchContext(tail);

        dbms.apply(context, insert, 1, "red", "triangle");
        dbms.apply(context, insert, 1, "green", "circle");
        dbms.apply(context, insert, 2, "yellow", "circle");
        dbms.apply(context, insert, 3, "blue", "square");
        dbms.apply(context, insert, 3, "orange", "square");

        Revision first = dbms.commit(context);

        TableReference numbersReference = dbms.tableReference(numbers);

        QueryTemplate numberEqual = dbms.queryTemplate
          (list((Expression) dbms.columnReference(numbersReference, color),
                (Expression) dbms.columnReference(numbersReference, shape)),
           numbersReference,
           dbms.operation
           (BinaryOperationType.Equal,
            dbms.columnReference(numbersReference, number),
            dbms.parameter()));

        QueryResult result = dbms.diff(tail, first, numberEqual, 1);

        assertEquals(result.nextRow(), ResultType.Inserted);
        assertEquals(result.nextItem(), "green");
        assertEquals(result.nextItem(), "circle");
        assertEquals(result.nextRow(), ResultType.Inserted);
        assertEquals(result.nextItem(), "red");
        assertEquals(result.nextItem(), "triangle");
        assertEquals(result.nextRow(), ResultType.End);

        PatchTemplate updateShapeWhereNumberEqual = dbms.updateTemplate
          (numbersReference,
           dbms.operation
           (BinaryOperationType.Equal,
            dbms.columnReference(numbersReference, number),
            dbms.parameter()),
           list(shape),
           list(dbms.parameter()));

        context = dbms.patchContext(first);

        dbms.apply(context, updateShapeWhereNumberEqual, 1, "pentagon");

        Revision second = dbms.commit(context);

        result = dbms.diff(tail, second, numberEqual, 1);

        assertEquals(result.nextRow(), ResultType.Inserted);
        assertEquals(result.nextItem(), "green");
        assertEquals(result.nextItem(), "pentagon");
        assertEquals(result.nextRow(), ResultType.Inserted);
        assertEquals(result.nextItem(), "red");
        assertEquals(result.nextItem(), "pentagon");
        assertEquals(result.nextRow(), ResultType.End);
    }

    @Test
    public void testDeleteOnPartialIndex(){
    	DBMS dbms = new MyDBMS();

        Column number = dbms.column(Integer.class);
        Column color = dbms.column(String.class);
        Column shape = dbms.column(String.class);
        Table numbers = dbms.table(list(number, color));

        Revision tail = dbms.revision();

        PatchTemplate insert = dbms.insertTemplate
          (numbers,
           list(number, color, shape),
           list(dbms.parameter(),
                dbms.parameter(),
                dbms.parameter()), DuplicateKeyResolution.Throw);

        PatchContext context = dbms.patchContext(tail);

        dbms.apply(context, insert, 1, "red", "triangle");
        dbms.apply(context, insert, 1, "green", "circle");
        dbms.apply(context, insert, 2, "yellow", "circle");
        dbms.apply(context, insert, 3, "blue", "square");
        dbms.apply(context, insert, 3, "orange", "square");

        Revision first = dbms.commit(context);

        TableReference numbersReference = dbms.tableReference(numbers);

        QueryTemplate numberEqual = dbms.queryTemplate
          (list((Expression) dbms.columnReference(numbersReference, color),
                (Expression) dbms.columnReference(numbersReference, shape)),
           numbersReference,
           dbms.operation
           (BinaryOperationType.Equal,
            dbms.columnReference(numbersReference, number),
            dbms.parameter()));

        QueryResult result = dbms.diff(tail, first, numberEqual, 1);

        assertEquals(result.nextRow(), ResultType.Inserted);
        assertEquals(result.nextItem(), "green");
        assertEquals(result.nextItem(), "circle");
        assertEquals(result.nextRow(), ResultType.Inserted);
        assertEquals(result.nextItem(), "red");
        assertEquals(result.nextItem(), "triangle");
        assertEquals(result.nextRow(), ResultType.End);

        PatchTemplate deleteWhereNumberEqual = dbms.deleteTemplate
          (numbersReference,
           dbms.operation
           (BinaryOperationType.Equal,
            dbms.columnReference(numbersReference, number),
            dbms.parameter()));

        context = dbms.patchContext(first);

        dbms.apply(context, deleteWhereNumberEqual, 1);

        Revision second = dbms.commit(context);

        result = dbms.diff(tail, second, numberEqual, 1);

        assertEquals(result.nextRow(), ResultType.End);
    }
}
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
import com.readytalk.oss.dbms.PatchTemplate;
import com.readytalk.oss.dbms.InsertTemplate;
import com.readytalk.oss.dbms.TableReference;
import com.readytalk.oss.dbms.QueryTemplate;
import com.readytalk.oss.dbms.QueryResult;
import com.readytalk.oss.dbms.Parameter;
import com.readytalk.oss.dbms.ColumnReference;
import com.readytalk.oss.dbms.DuplicateKeyResolution;
import com.readytalk.oss.dbms.imp.MyDBMS;

public class IndexesTest extends TestCase{
    
    @Test
    public void testMultiLevelIndexes(){
        DBMS dbms = new MyDBMS();

        Column country = new Column(String.class);
        Column state = new Column(String.class);
        Column city = new Column(String.class);
        Column zip = new Column(Integer.class);
        Column color = new Column(String.class);
        Table places = new Table(list(country, state, city));

        Revision tail = dbms.revision();

        PatchTemplate insert = new InsertTemplate
          (places,
           list(country, state, city, zip, color),
           list((Expression) new Parameter(),
                new Parameter(),
                new Parameter(),
                new Parameter(),
                new Parameter()), DuplicateKeyResolution.Throw);
        
        RevisionBuilder builder = dbms.builder(tail);

        builder.apply(insert,
                   "USA", "Colorado", "Denver", 80209, "teal");
        builder.apply(insert,
                   "USA", "Colorado", "Glenwood Springs", 81601, "orange");
        builder.apply(insert,
                   "USA", "New York", "New York", 10001, "blue");
        builder.apply(insert,
                   "France", "N/A", "Paris", 0, "pink");
        builder.apply(insert,
                   "England", "N/A", "London", 0, "red");
        builder.apply(insert,
                   "China", "N/A", "Beijing", 0, "red");
        builder.apply(insert,
                   "China", "N/A", "Shanghai", 0, "green");

        Revision first = builder.commit();

        TableReference placesReference = new TableReference(places);

        QueryTemplate stateEqual = new QueryTemplate
          (list((Expression) new ColumnReference(placesReference, color),
                (Expression) new ColumnReference(placesReference, zip)),
           placesReference,
           new BinaryOperation
           (BinaryOperation.Type.Equal,
            new ColumnReference(placesReference, state),
            new Parameter()));

        QueryResult result = dbms.diff
          (tail, first, stateEqual, "Colorado");

        assertEquals(result.nextRow(), QueryResult.Type.Inserted);
        assertEquals(result.nextItem(), "teal");
        assertEquals(result.nextItem(), 80209);
        assertEquals(result.nextRow(), QueryResult.Type.Inserted);
        assertEquals(result.nextItem(), "orange");
        assertEquals(result.nextItem(), 81601);
        assertEquals(result.nextRow(), QueryResult.Type.End);

        result = dbms.diff(first, tail, stateEqual, "Colorado");

        assertEquals(result.nextRow(), QueryResult.Type.Deleted);
        assertEquals(result.nextItem(), "teal");
        assertEquals(result.nextItem(), 80209);
        assertEquals(result.nextRow(), QueryResult.Type.Deleted);
        assertEquals(result.nextItem(), "orange");
        assertEquals(result.nextItem(), 81601);
        assertEquals(result.nextRow(), QueryResult.Type.End);

        result = dbms.diff(tail, first, stateEqual, "N/A");

        assertEquals(result.nextRow(), QueryResult.Type.Inserted);
        assertEquals(result.nextItem(), "red");
        assertEquals(result.nextItem(), 0);
        assertEquals(result.nextRow(), QueryResult.Type.Inserted);
        assertEquals(result.nextItem(), "green");
        assertEquals(result.nextItem(), 0);
        assertEquals(result.nextRow(), QueryResult.Type.Inserted);
        assertEquals(result.nextItem(), "red");
        assertEquals(result.nextItem(), 0);
        assertEquals(result.nextRow(), QueryResult.Type.Inserted);
        assertEquals(result.nextItem(), "pink");
        assertEquals(result.nextItem(), 0);
        assertEquals(result.nextRow(), QueryResult.Type.End);

        QueryTemplate countryEqual = new QueryTemplate
          (list((Expression) new ColumnReference(placesReference, color),
                (Expression) new ColumnReference(placesReference, city)),
           placesReference,
           new BinaryOperation
           (BinaryOperation.Type.Equal,
            new ColumnReference(placesReference, country),
            new Parameter()));

        result = dbms.diff(tail, first, countryEqual, "France");

        assertEquals(result.nextRow(), QueryResult.Type.Inserted);
        assertEquals(result.nextItem(), "pink");
        assertEquals(result.nextItem(), "Paris");
        assertEquals(result.nextRow(), QueryResult.Type.End);

        result = dbms.diff(tail, first, countryEqual, "China");

        assertEquals(result.nextRow(), QueryResult.Type.Inserted);
        assertEquals(result.nextItem(), "red");
        assertEquals(result.nextItem(), "Beijing");
        assertEquals(result.nextRow(), QueryResult.Type.Inserted);
        assertEquals(result.nextItem(), "green");
        assertEquals(result.nextItem(), "Shanghai");
        assertEquals(result.nextRow(), QueryResult.Type.End);

        QueryTemplate countryStateCityEqual = new QueryTemplate
          (list((Expression) new ColumnReference(placesReference, color),
                (Expression) new ColumnReference(placesReference, city)),
           placesReference,
           new BinaryOperation
           (BinaryOperation.Type.And,
            new BinaryOperation
            (BinaryOperation.Type.And,
             new BinaryOperation
             (BinaryOperation.Type.Equal,
              new ColumnReference(placesReference, country),
              new Parameter()),
             new BinaryOperation
             (BinaryOperation.Type.Equal,
              new ColumnReference(placesReference, state),
              new Parameter())),
            new BinaryOperation
            (BinaryOperation.Type.Equal,
             new ColumnReference(placesReference, city),
             new Parameter())));

        result = dbms.diff(tail, first, countryStateCityEqual,
                           "France", "Colorado", "Paris");

        assertEquals(result.nextRow(), QueryResult.Type.End);

        result = dbms.diff(tail, first, countryStateCityEqual,
                           "France", "N/A", "Paris");

        assertEquals(result.nextRow(), QueryResult.Type.Inserted);
        assertEquals(result.nextItem(), "pink");
        assertEquals(result.nextItem(), "Paris");
        assertEquals(result.nextRow(), QueryResult.Type.End);
    }
}

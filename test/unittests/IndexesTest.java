package unittests;

import junit.framework.TestCase;

import org.junit.Test;

import static com.readytalk.oss.dbms.util.Util.list;
import static org.junit.Assert.*;

import com.readytalk.oss.dbms.BinaryOperation;
import com.readytalk.oss.dbms.Column;
import com.readytalk.oss.dbms.Revisions;
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

public class IndexesTest extends TestCase{
    
    @Test
    public void testMultiLevelIndexes(){
        Column country = new Column(String.class);
        Column state = new Column(String.class);
        Column city = new Column(String.class);
        Column zip = new Column(Integer.class);
        Column color = new Column(String.class);
        Table places = new Table(list(country, state, city));

        Revision tail = Revisions.Empty;

        PatchTemplate insert = new InsertTemplate
          (places,
           list(country, state, city, zip, color),
           list((Expression) new Parameter(),
                new Parameter(),
                new Parameter(),
                new Parameter(),
                new Parameter()), DuplicateKeyResolution.Throw);
        
        RevisionBuilder builder = tail.builder();

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
        Object[] parameters = { "Colorado" };

        QueryResult result = tail.diff(first, stateEqual, parameters);

        assertEquals(result.nextRow(), QueryResult.Type.Inserted);
        assertEquals(result.nextItem(), "teal");
        assertEquals(result.nextItem(), 80209);
        assertEquals(result.nextRow(), QueryResult.Type.Inserted);
        assertEquals(result.nextItem(), "orange");
        assertEquals(result.nextItem(), 81601);
        assertEquals(result.nextRow(), QueryResult.Type.End);
        Object[] parameters1 = { "Colorado" };

        result = first.diff(tail, stateEqual, parameters1);

        assertEquals(result.nextRow(), QueryResult.Type.Deleted);
        assertEquals(result.nextItem(), "teal");
        assertEquals(result.nextItem(), 80209);
        assertEquals(result.nextRow(), QueryResult.Type.Deleted);
        assertEquals(result.nextItem(), "orange");
        assertEquals(result.nextItem(), 81601);
        assertEquals(result.nextRow(), QueryResult.Type.End);
        Object[] parameters2 = { "N/A" };

        result = tail.diff(first, stateEqual, parameters2);

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
        Object[] parameters3 = { "France" };

        result = tail.diff(first, countryEqual, parameters3);

        assertEquals(result.nextRow(), QueryResult.Type.Inserted);
        assertEquals(result.nextItem(), "pink");
        assertEquals(result.nextItem(), "Paris");
        assertEquals(result.nextRow(), QueryResult.Type.End);
        Object[] parameters4 = { "China" };

        result = tail.diff(first, countryEqual, parameters4);

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
        Object[] parameters5 = { "France", "Colorado", "Paris" };

        result = tail.diff(first, countryStateCityEqual, parameters5);

        assertEquals(result.nextRow(), QueryResult.Type.End);
        Object[] parameters6 = { "France", "N/A", "Paris" };

        result = tail.diff(first, countryStateCityEqual, parameters6);

        assertEquals(result.nextRow(), QueryResult.Type.Inserted);
        assertEquals(result.nextItem(), "pink");
        assertEquals(result.nextItem(), "Paris");
        assertEquals(result.nextRow(), QueryResult.Type.End);
    }
}

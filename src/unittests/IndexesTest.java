package unittests;

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

public class IndexesTest {
    
    @Test
    public void testMultiLevelIndexes(){
        DBMS dbms = new MyDBMS();

        Column country = dbms.column(String.class);
        Column state = dbms.column(String.class);
        Column city = dbms.column(String.class);
        Column zip = dbms.column(Integer.class);
        Column color = dbms.column(String.class);
        Table places = dbms.table(list(country, state, city));

        Revision tail = dbms.revision();

        PatchTemplate insert = dbms.insertTemplate
          (places,
           list(country, state, city, zip, color),
           list(dbms.parameter(),
                dbms.parameter(),
                dbms.parameter(),
                dbms.parameter(),
                dbms.parameter()), DuplicateKeyResolution.Throw);
        
        PatchContext context = dbms.patchContext(tail);

        dbms.apply(context, insert,
                   "USA", "Colorado", "Denver", 80209, "teal");
        dbms.apply(context, insert,
                   "USA", "Colorado", "Glenwood Springs", 81601, "orange");
        dbms.apply(context, insert,
                   "USA", "New York", "New York", 10001, "blue");
        dbms.apply(context, insert,
                   "France", "N/A", "Paris", 0, "pink");
        dbms.apply(context, insert,
                   "England", "N/A", "London", 0, "red");
        dbms.apply(context, insert,
                   "China", "N/A", "Beijing", 0, "red");
        dbms.apply(context, insert,
                   "China", "N/A", "Shanghai", 0, "green");

        Revision first = dbms.commit(context);

        TableReference placesReference = dbms.tableReference(places);

        QueryTemplate stateEqual = dbms.queryTemplate
          (list((Expression) dbms.columnReference(placesReference, color),
                (Expression) dbms.columnReference(placesReference, zip)),
           placesReference,
           dbms.operation
           (BinaryOperationType.Equal,
            dbms.columnReference(placesReference, state),
            dbms.parameter()));

        QueryResult result = dbms.diff
          (tail, first, stateEqual, "Colorado");

        assertEquals(result.nextRow(), ResultType.Inserted);
        assertEquals(result.nextItem(), "teal");
        assertEquals(result.nextItem(), 80209);
        assertEquals(result.nextRow(), ResultType.Inserted);
        assertEquals(result.nextItem(), "orange");
        assertEquals(result.nextItem(), 81601);
        assertEquals(result.nextRow(), ResultType.End);

        result = dbms.diff(first, tail, stateEqual, "Colorado");

        assertEquals(result.nextRow(), ResultType.Deleted);
        assertEquals(result.nextItem(), "teal");
        assertEquals(result.nextItem(), 80209);
        assertEquals(result.nextRow(), ResultType.Deleted);
        assertEquals(result.nextItem(), "orange");
        assertEquals(result.nextItem(), 81601);
        assertEquals(result.nextRow(), ResultType.End);

        result = dbms.diff(tail, first, stateEqual, "N/A");

        assertEquals(result.nextRow(), ResultType.Inserted);
        assertEquals(result.nextItem(), "red");
        assertEquals(result.nextItem(), 0);
        assertEquals(result.nextRow(), ResultType.Inserted);
        assertEquals(result.nextItem(), "green");
        assertEquals(result.nextItem(), 0);
        assertEquals(result.nextRow(), ResultType.Inserted);
        assertEquals(result.nextItem(), "red");
        assertEquals(result.nextItem(), 0);
        assertEquals(result.nextRow(), ResultType.Inserted);
        assertEquals(result.nextItem(), "pink");
        assertEquals(result.nextItem(), 0);
        assertEquals(result.nextRow(), ResultType.End);

        QueryTemplate countryEqual = dbms.queryTemplate
          (list((Expression) dbms.columnReference(placesReference, color),
                (Expression) dbms.columnReference(placesReference, city)),
           placesReference,
           dbms.operation
           (BinaryOperationType.Equal,
            dbms.columnReference(placesReference, country),
            dbms.parameter()));

        result = dbms.diff(tail, first, countryEqual, "France");

        assertEquals(result.nextRow(), ResultType.Inserted);
        assertEquals(result.nextItem(), "pink");
        assertEquals(result.nextItem(), "Paris");
        assertEquals(result.nextRow(), ResultType.End);

        result = dbms.diff(tail, first, countryEqual, "China");

        assertEquals(result.nextRow(), ResultType.Inserted);
        assertEquals(result.nextItem(), "red");
        assertEquals(result.nextItem(), "Beijing");
        assertEquals(result.nextRow(), ResultType.Inserted);
        assertEquals(result.nextItem(), "green");
        assertEquals(result.nextItem(), "Shanghai");
        assertEquals(result.nextRow(), ResultType.End);

        QueryTemplate countryStateCityEqual = dbms.queryTemplate
          (list((Expression) dbms.columnReference(placesReference, color),
                (Expression) dbms.columnReference(placesReference, city)),
           placesReference,
           dbms.operation
           (BinaryOperationType.And,
            dbms.operation
            (BinaryOperationType.And,
             dbms.operation
             (BinaryOperationType.Equal,
              dbms.columnReference(placesReference, country),
              dbms.parameter()),
             dbms.operation
             (BinaryOperationType.Equal,
              dbms.columnReference(placesReference, state),
              dbms.parameter())),
            dbms.operation
            (BinaryOperationType.Equal,
             dbms.columnReference(placesReference, city),
             dbms.parameter())));

        result = dbms.diff(tail, first, countryStateCityEqual,
                           "France", "Colorado", "Paris");

        assertEquals(result.nextRow(), ResultType.End);

        result = dbms.diff(tail, first, countryStateCityEqual,
                           "France", "N/A", "Paris");

        assertEquals(result.nextRow(), ResultType.Inserted);
        assertEquals(result.nextItem(), "pink");
        assertEquals(result.nextItem(), "Paris");
        assertEquals(result.nextRow(), ResultType.End);
    }
}

package unittests;

import static com.readytalk.oss.dbms.ExpressionFactory.reference;
import static com.readytalk.oss.dbms.util.Util.cols;
import static com.readytalk.oss.dbms.util.Util.list;
import static org.junit.Assert.*;

import org.junit.Test;

import com.readytalk.oss.dbms.Column;
import com.readytalk.oss.dbms.Constant;
import com.readytalk.oss.dbms.DuplicateKeyResolution;
import com.readytalk.oss.dbms.Expression;
import com.readytalk.oss.dbms.InsertTemplate;
import com.readytalk.oss.dbms.Parameter;
import com.readytalk.oss.dbms.PatchTemplate;
import com.readytalk.oss.dbms.QueryResult;
import com.readytalk.oss.dbms.QueryResult.Type;
import com.readytalk.oss.dbms.QueryTemplate;
import com.readytalk.oss.dbms.Revision;
import com.readytalk.oss.dbms.RevisionBuilder;
import com.readytalk.oss.dbms.Revisions;
import com.readytalk.oss.dbms.Table;
import com.readytalk.oss.dbms.TableReference;

public class Diffs {

  @Test
  public void testUpdateNonQueriedColumns() {
    Column<Integer> number = new Column<Integer>(Integer.class);
    Column<String> name = new Column<String>(String.class);
    Column<String> origin = new Column<String>(String.class);
    Table numbers = new Table(cols(number));
  
    Revision tail = Revisions.Empty;
  
    PatchTemplate insert = new InsertTemplate
      (numbers,
       cols(number, name, origin),
       list((Expression) new Parameter(), new Parameter(), new Parameter()),
       DuplicateKeyResolution.Throw);
  
    RevisionBuilder builder = tail.builder();
  
    builder.apply(insert, 0, "zero", "far too recently");
    builder.apply(insert, 1, "one", "long ago");
    builder.apply(insert, 2, "two", "long ago");
    builder.apply(insert, 3, "three", "a little less long ago");

    Revision first = builder.commit();
    
    TableReference numbersReference = new TableReference(numbers);
    
    QueryTemplate query = new QueryTemplate
    (list((Expression) reference(numbersReference, name)),
     numbersReference,
     new Constant(true));

    QueryResult result = tail.diff(first, query);

    assertEquals(QueryResult.Type.Inserted, result.nextRow());
    assertEquals("zero", result.nextItem());
    assertEquals(QueryResult.Type.Inserted, result.nextRow());
    assertEquals("one", result.nextItem());
    assertEquals(QueryResult.Type.Inserted, result.nextRow());
    assertEquals("two", result.nextItem());
    assertEquals(QueryResult.Type.Inserted, result.nextRow());
    assertEquals("three", result.nextItem());
    assertEquals(QueryResult.Type.End, result.nextRow());

    
    builder = first.builder();
  
    // rewrite history
    builder.table(numbers)
      .row(0)
      .column(origin, "not long after <one>")
      .up().up();

    Revision second = builder.commit();

    result = first.diff(second, query);

    // we didn't change any of the queried columns, so we shouldn't see any difference.
    assertEquals(QueryResult.Type.End, result.nextRow());
    
  }
}

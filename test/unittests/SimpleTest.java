package unittests;

import junit.framework.TestCase;

import org.junit.Test;

import static com.readytalk.oss.dbms.util.Util.list;
import static org.junit.Assert.*;

import com.readytalk.oss.dbms.DBMS;
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
import com.readytalk.oss.dbms.Constant;
import com.readytalk.oss.dbms.ColumnReference;
import com.readytalk.oss.dbms.DuplicateKeyResolution;
import com.readytalk.oss.dbms.imp.MyDBMS;

public class SimpleTest extends TestCase{
    
	@Test
    public void testSimpleInsertQuery(){
        DBMS dbms = new MyDBMS();
        
        Column key = new Column(Integer.class);
        Column firstName = new Column(String.class);
        Column lastName = new Column(String.class);
        Table names = new Table(list(key));
        Revision tail = dbms.revision();
        
        RevisionBuilder builder = dbms.builder(tail);
        PatchTemplate insert = new InsertTemplate
         (names,
          list(key, firstName, lastName),
          list((Expression) new Parameter(),
               new Parameter(),
               new Parameter()), DuplicateKeyResolution.Throw);
        builder.apply(insert, 1, "Charles", "Norris");
        builder.apply(insert, 2, "Chuck", "Norris");
        builder.apply(insert, 3, "Chuck", "Taylor");
        
        Revision first = builder.commit();
        
        TableReference namesReference = new TableReference(names);
        
        QueryTemplate any = new QueryTemplate
          (list((Expression) new ColumnReference(namesReference, key),
        		  (Expression) new ColumnReference(namesReference, firstName),
                  (Expression) new ColumnReference(namesReference, lastName)),
                  namesReference,
                  new Constant(true));

        QueryResult result = dbms.diff(tail, first, any);
        
        assertEquals(result.nextRow(), QueryResult.Type.Inserted);
        assertEquals(result.nextItem(), 1);
        assertEquals(result.nextItem(), "Charles");
        assertEquals(result.nextItem(), "Norris");
        assertEquals(result.nextRow(), QueryResult.Type.Inserted);
        assertEquals(result.nextItem(), 2);
        assertEquals(result.nextItem(), "Chuck");
        assertEquals(result.nextItem(), "Norris");
        assertEquals(result.nextRow(), QueryResult.Type.Inserted);
        assertEquals(result.nextItem(), 3);
        assertEquals(result.nextItem(), "Chuck");
        assertEquals(result.nextItem(), "Taylor");
        assertEquals(result.nextRow(), QueryResult.Type.End);
        }
    
    @Test
    public void testMultipleColumnInsertQuery(){
        DBMS dbms = new MyDBMS();
        
        Column key = new Column(Integer.class);
        Column firstName = new Column(String.class);
        Column lastName = new Column(String.class);
        Column city = new Column(String.class);
        Column age = new Column(Integer.class);
        Table names = new Table(list(key));
        Revision tail = dbms.revision();
        //Insert 4 columns
        RevisionBuilder builder = dbms.builder(tail);
        PatchTemplate insert = new InsertTemplate
         (names,
                 list(key, firstName, lastName, city),
                 list((Expression) new Parameter(),
                         new Parameter(),
                         new Parameter(),
                         new Parameter()), DuplicateKeyResolution.Throw);
        builder.apply(insert, 1, "Charles", "Norris", "Montreal");
        
        Revision first = builder.commit();
        
        //Insert 2 columns
        builder = dbms.builder(first);
        insert = new InsertTemplate
         (names,
                 list(key, firstName),
                 list((Expression) new Parameter(),
                         new Parameter()), DuplicateKeyResolution.Throw);
        builder.apply(insert, 2, "Charleston");
        
        Revision second = builder.commit();
        
        //Insert 5 columns
        builder = dbms.builder(second);
        insert = new InsertTemplate
         (names,
                 list(key, firstName, lastName, city, age),
                 list((Expression) new Parameter(),
                         new Parameter(),
                         new Parameter(),
                         new Parameter(),
                         new Parameter()), DuplicateKeyResolution.Throw);
        builder.apply(insert, 3, "Wickerson", "Jones", "Lancaster", 54);
        
        Revision third = builder.commit();
        
        
        //Validate data
        TableReference namesReference = new TableReference(names);
        
        QueryTemplate any = new QueryTemplate
          (list((Expression) new ColumnReference(namesReference, key),
        		  (Expression) new ColumnReference(namesReference, firstName),
                  (Expression) new ColumnReference(namesReference, lastName),
                  (Expression) new ColumnReference(namesReference, city),
                  (Expression) new ColumnReference(namesReference, age)),
                  namesReference,
                  new Constant(true));

          QueryResult result = dbms.diff(tail, third, any);
        
        assertEquals(result.nextRow(), QueryResult.Type.Inserted);
        assertEquals(result.nextItem(), 1);
        assertEquals(result.nextItem(), "Charles");
        assertEquals(result.nextItem(), "Norris");
        assertEquals(result.nextItem(), "Montreal");
        assertEquals(result.nextItem(), null);
        assertEquals(result.nextRow(), QueryResult.Type.Inserted);
        assertEquals(result.nextItem(), 2);
        assertEquals(result.nextItem(), "Charleston");
        assertEquals(result.nextItem(), null);
        assertEquals(result.nextItem(), null);
        assertEquals(result.nextItem(), null);
        assertEquals(result.nextRow(), QueryResult.Type.Inserted);
        assertEquals(result.nextItem(), 3);
        assertEquals(result.nextItem(), "Wickerson");
        assertEquals(result.nextItem(), "Jones");
        assertEquals(result.nextItem(), "Lancaster");
        assertEquals(result.nextItem(), 54);
        assertEquals(result.nextRow(), QueryResult.Type.End);
        }
    
    
    @Test
    public void testNotEnoughColumnsForPrimaryKeyQuery(){
        DBMS dbms = new MyDBMS();
        
        Column key = new Column(Integer.class);
        Column firstName = new Column(String.class);
        Column lastName = new Column(String.class);
        Column city = new Column(String.class);
        Table names = new Table(list(key, city));
        Revision tail = dbms.revision();
        
        RevisionBuilder builder = dbms.builder(tail);
        try{
        PatchTemplate insert = new InsertTemplate
         (names,
          list(key, firstName, lastName),
          list((Expression) new Parameter(),
                         new Parameter(),
                         new Parameter()), DuplicateKeyResolution.Throw);
        fail("Expected IllegalArgumentException...");
                }catch(IllegalArgumentException expected){}
        }
    }

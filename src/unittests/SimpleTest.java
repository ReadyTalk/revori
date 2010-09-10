package unittests;

import org.junit.Test;

import static com.readytalk.oss.dbms.imp.Util.list;
import static org.junit.Assert.*;

import com.readytalk.oss.dbms.DBMS;
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

public class SimpleTest{
    
    @Test
    public void testSimpleInsertQuery(){
        DBMS dbms = new MyDBMS();
        
        Column key = dbms.column(Integer.class);
        Column firstName = dbms.column(String.class);
        Column lastName = dbms.column(String.class);
        Table names = dbms.table(list(key));
        Revision tail = dbms.revision();
        
        PatchContext context = dbms.patchContext(tail);
        PatchTemplate insert = dbms.insertTemplate
         (names,
                 list(key, firstName, lastName),
                 list(dbms.parameter(),
                         dbms.parameter(),
                         dbms.parameter()), DuplicateKeyResolution.Throw);
        dbms.apply(context, insert, 1, "Charles", "Norris");
        dbms.apply(context, insert, 2, "Chuck", "Norris");
        dbms.apply(context, insert, 3, "Chuck", "Taylor");
        
        Revision first = dbms.commit(context);
        
        TableReference namesReference = dbms.tableReference(names);
        
        QueryTemplate any = dbms.queryTemplate
          (list((Expression) dbms.columnReference(namesReference, key),
        		  (Expression) dbms.columnReference(namesReference, firstName),
                  (Expression) dbms.columnReference(namesReference, lastName)),
                  namesReference,
                  dbms.constant(true));

        QueryResult result = dbms.diff(tail, first, any);
        
        assertEquals(result.nextRow(), ResultType.Inserted);
        assertEquals(result.nextItem(), 1);
        assertEquals(result.nextItem(), "Charles");
        assertEquals(result.nextItem(), "Norris");
        assertEquals(result.nextRow(), ResultType.Inserted);
        assertEquals(result.nextItem(), 2);
        assertEquals(result.nextItem(), "Chuck");
        assertEquals(result.nextItem(), "Norris");
        assertEquals(result.nextRow(), ResultType.Inserted);
        assertEquals(result.nextItem(), 3);
        assertEquals(result.nextItem(), "Chuck");
        assertEquals(result.nextItem(), "Taylor");
        assertEquals(result.nextRow(), ResultType.End);
        }
    }

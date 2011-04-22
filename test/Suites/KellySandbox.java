package Suites;

import junit.framework.TestCase;

import org.junit.Test;

import static com.readytalk.oss.dbms.imp.Util.list;
import static org.junit.Assert.*;

import com.readytalk.oss.dbms.DBMS;
import com.readytalk.oss.dbms.DBMS.BinaryOperationType;
import com.readytalk.oss.dbms.DBMS.Column;
import com.readytalk.oss.dbms.DBMS.Expression;
import com.readytalk.oss.dbms.DBMS.QueryResult;
import com.readytalk.oss.dbms.DBMS.QueryTemplate;
import com.readytalk.oss.dbms.DBMS.ResultType;
import com.readytalk.oss.dbms.DBMS.Table;
import com.readytalk.oss.dbms.DBMS.Revision;
import com.readytalk.oss.dbms.DBMS.PatchContext;
import com.readytalk.oss.dbms.DBMS.PatchTemplate;
import com.readytalk.oss.dbms.DBMS.TableReference;
import com.readytalk.oss.dbms.DBMS.DuplicateKeyResolution;
import com.readytalk.oss.dbms.imp.MyDBMS;


public class KellySandbox extends TestCase{
   @Test
   public void testColumnTypes(){
	   DBMS dbms = new MyDBMS();

	    Column number = dbms.column(Integer.class);
	    Column name = dbms.column(String.class);
	    Table numbers = dbms.table(list(number));

	    Revision tail = dbms.revision();

	    PatchTemplate insert = dbms.insertTemplate
	      (numbers,
	       list(number, name),
	       list(dbms.parameter(),
	            dbms.parameter()), DuplicateKeyResolution.Throw);

	    try {
	      dbms.apply(dbms.patchContext(tail), insert, "1", "one");
	      throw new RuntimeException();
	    } catch (ClassCastException e) { }

	    try {
	      dbms.apply(dbms.patchContext(tail), insert, 1, 1);
	      throw new RuntimeException();
	    } catch (ClassCastException e) { }

	    PatchContext context = dbms.patchContext(tail);

	    dbms.apply(context, insert, 1, "one");

	    Revision first = dbms.commit(context);

	    TableReference numbersReference = dbms.tableReference(numbers);

	    PatchTemplate updateNameWhereNumberEqual = dbms.updateTemplate
	      (numbersReference,
	       dbms.operation
	       (BinaryOperationType.Equal,
	        dbms.columnReference(numbersReference, number),
	        dbms.parameter()),
	       list(name),
	       list(dbms.parameter()));

	    try {
	      dbms.apply(dbms.patchContext(first), updateNameWhereNumberEqual, 1, 2);
	      throw new RuntimeException();
	    } catch (ClassCastException e) { }
   }
   
   @Test (expected=IllegalArgumentException.class)
   public void testNotEnoughColumnsForPrimaryKeyQuery(){
       DBMS dbms = new MyDBMS();
       
       Column key = dbms.column(Integer.class);
       Column firstName = dbms.column(String.class);
       Column lastName = dbms.column(String.class);
       Column city = dbms.column(String.class);
       Table names = dbms.table(list(key, city));
       Revision tail = dbms.revision();
       
       PatchContext context = dbms.patchContext(tail);
       try{
       PatchTemplate insert = dbms.insertTemplate
        (names,
                list(key, firstName, lastName),
                list(dbms.parameter(),
                        dbms.parameter(),
                        dbms.parameter()), DuplicateKeyResolution.Throw);
       fail("Expecting IllegalArgumentException...");
       }catch(IllegalArgumentException expected){}
      
       }
}
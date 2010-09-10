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


public class GeneralTests {
	
	@Test
	public void testSimpleInsertDiffs(){
		DBMS dbms = new MyDBMS();

	    Column number = dbms.column(Integer.class);
	    Column name = dbms.column(String.class);
	    Table numbers = dbms.table(list(number));

	    Revision tail = dbms.revision();

	    PatchContext context = dbms.patchContext(tail);

	    PatchTemplate insert = dbms.insertTemplate
	      (numbers,
	       list(number, name),
	       list(dbms.parameter(),
	            dbms.parameter()), DuplicateKeyResolution.Throw);

	    dbms.apply(context, insert, 42, "forty two");

	    Revision first = dbms.commit(context);

	    TableReference numbersReference = dbms.tableReference(numbers);

	    QueryTemplate equal = dbms.queryTemplate
	      (list((Expression) dbms.columnReference(numbersReference, name)),
	       numbersReference,
	       dbms.operation
	       (BinaryOperationType.Equal,
	        dbms.columnReference(numbersReference, number),
	        dbms.parameter()));

	    QueryResult result = dbms.diff(tail, first, equal, 42);

	    assertEquals(result.nextRow(), ResultType.Inserted);
	    assertEquals(result.nextItem(), "forty two");
	    assertEquals(result.nextRow(), ResultType.End);

	    result = dbms.diff(first, tail, equal, 42);

	    assertEquals(result.nextRow(), ResultType.Deleted);
	    assertEquals(result.nextItem(), "forty two");
	    assertEquals(result.nextRow(), ResultType.End);

	    result = dbms.diff(tail, first, equal, 43);

	    assertEquals(result.nextRow(), ResultType.End);

	    result = dbms.diff(tail, tail, equal, 42);

	    assertEquals(result.nextRow(), ResultType.End);

	    result = dbms.diff(first, first, equal, 42);

	    assertEquals(result.nextRow(), ResultType.End);
	}
	
	@Test
	public void testLargerInsertDiffs(){
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

		    PatchContext context = dbms.patchContext(tail);

		    dbms.apply(context, insert, 42, "forty two");
		    dbms.apply(context, insert, 43, "forty three");
		    dbms.apply(context, insert, 44, "forty four");
		    dbms.apply(context, insert,  2, "two");
		    dbms.apply(context, insert, 65, "sixty five");
		    dbms.apply(context, insert,  8, "eight");

		    Revision first = dbms.commit(context);

		    TableReference numbersReference = dbms.tableReference(numbers);
		    QueryTemplate equal = dbms.queryTemplate
		      (list((Expression) dbms.columnReference(numbersReference, name)),
		       numbersReference,
		       dbms.operation
		       (BinaryOperationType.Equal,
		        dbms.columnReference(numbersReference, number),
		        dbms.parameter()));

		    QueryResult result = dbms.diff(tail, first, equal, 42);

		    assertEquals(result.nextRow(), ResultType.Inserted);
		    assertEquals(result.nextItem(), "forty two");
		    assertEquals(result.nextRow(), ResultType.End);

		    result = dbms.diff(first, tail, equal, 42);

		    assertEquals(result.nextRow(), ResultType.Deleted);
		    assertEquals(result.nextItem(), "forty two");
		    assertEquals(result.nextRow(), ResultType.End);

		    result = dbms.diff(tail, first, equal, 43);

		    assertEquals(result.nextRow(), ResultType.Inserted);
		    assertEquals(result.nextItem(), "forty three");
		    assertEquals(result.nextRow(), ResultType.End);

		    result = dbms.diff(tail, tail, equal, 42);

		    assertEquals(result.nextRow(), ResultType.End);

		    result = dbms.diff(first, first, equal, 42);

		    assertEquals(result.nextRow(), ResultType.End);

		    context = dbms.patchContext(first);

		    dbms.apply(context, insert, 1, "one");
		    dbms.apply(context, insert, 3, "three");
		    dbms.apply(context, insert, 5, "five");
		    dbms.apply(context, insert, 6, "six");

		    Revision second = dbms.commit(context);

		    result = dbms.diff(tail, second, equal, 43);

		    assertEquals(result.nextRow(), ResultType.Inserted);
		    assertEquals(result.nextItem(), "forty three");
		    assertEquals(result.nextRow(), ResultType.End);

		    result = dbms.diff(first, second, equal, 43);

		    assertEquals(result.nextRow(), ResultType.End);
		    
		    result = dbms.diff(first, second, equal, 5);

		    assertEquals(result.nextRow(), ResultType.Inserted);
		    assertEquals(result.nextItem(), "five");
		    assertEquals(result.nextRow(), ResultType.End);
		    
		    result = dbms.diff(tail, first, equal, 5);

		    assertEquals(result.nextRow(), ResultType.End);
		    
		    result = dbms.diff(second, first, equal, 5);

		    assertEquals(result.nextRow(), ResultType.Deleted);
		    assertEquals(result.nextItem(), "five");
		    assertEquals(result.nextRow(), ResultType.End);
	}
}

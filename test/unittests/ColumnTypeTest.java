package unittests;

import junit.framework.TestCase;

import org.junit.Test;

import static com.readytalk.oss.dbms.util.Util.list;

import com.readytalk.oss.dbms.DBMS;
import com.readytalk.oss.dbms.BinaryOperation;
import com.readytalk.oss.dbms.Column;
import com.readytalk.oss.dbms.Table;
import com.readytalk.oss.dbms.Revision;
import com.readytalk.oss.dbms.RevisionBuilder;
import com.readytalk.oss.dbms.PatchTemplate;
import com.readytalk.oss.dbms.InsertTemplate;
import com.readytalk.oss.dbms.UpdateTemplate;
import com.readytalk.oss.dbms.Parameter;
import com.readytalk.oss.dbms.TableReference;
import com.readytalk.oss.dbms.ColumnReference;
import com.readytalk.oss.dbms.Expression;
import com.readytalk.oss.dbms.DuplicateKeyResolution;
import com.readytalk.oss.dbms.imp.MyDBMS;


public class ColumnTypeTest extends TestCase{
   @Test
   public void testColumnTypes(){
	   DBMS dbms = new MyDBMS();

	    Column number = new Column(Integer.class);
	    Column name = new Column(String.class);
	    Table numbers = new Table(list(number));

	    Revision tail = dbms.revision();

	    PatchTemplate insert = new InsertTemplate
	      (numbers,
	       list(number, name),
	       list((Expression) new Parameter(), new Parameter()),
               DuplicateKeyResolution.Throw);

	    try {
	      dbms.builder(tail).apply(insert, "1", "one");
	      throw new RuntimeException();
	    } catch (ClassCastException e) { }

	    try {
	      dbms.builder(tail).apply(insert, 1, 1);
	      throw new RuntimeException();
	    } catch (ClassCastException e) { }

	    RevisionBuilder builder = dbms.builder(tail);

	    builder.apply(insert, 1, "one");

	    Revision first = builder.commit();

	    TableReference numbersReference = new TableReference(numbers);

	    PatchTemplate updateNameWhereNumberEqual = new UpdateTemplate
	      (numbersReference,
	       new BinaryOperation
	       (BinaryOperation.Type.Equal,
	        new ColumnReference(numbersReference, number),
	        new Parameter()),
	       list(name),
	       list((Expression) new Parameter()));

	    try {
	      dbms.builder(first).apply(updateNameWhereNumberEqual, 1, 2);
	      throw new RuntimeException();
	    } catch (ClassCastException e) { }
   }
}

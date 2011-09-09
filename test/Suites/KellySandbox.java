package Suites;

import junit.framework.TestCase;

import org.junit.Test;

import static com.readytalk.oss.dbms.util.Util.list;
import static com.readytalk.oss.dbms.util.Util.cols;

import static com.readytalk.oss.dbms.ExpressionFactory.reference;

import com.readytalk.oss.dbms.BinaryOperation;
import com.readytalk.oss.dbms.Column;
import com.readytalk.oss.dbms.Parameter;
import com.readytalk.oss.dbms.Expression;
import com.readytalk.oss.dbms.Revisions;
import com.readytalk.oss.dbms.Table;
import com.readytalk.oss.dbms.Revision;
import com.readytalk.oss.dbms.RevisionBuilder;
import com.readytalk.oss.dbms.PatchTemplate;
import com.readytalk.oss.dbms.InsertTemplate;
import com.readytalk.oss.dbms.UpdateTemplate;
import com.readytalk.oss.dbms.TableReference;
import com.readytalk.oss.dbms.DuplicateKeyResolution;


public class KellySandbox extends TestCase{
   @Test
   public void testColumnTypes(){
	    Column<Integer> number = new Column<Integer>(Integer.class);
	    Column<String> name = new Column<String>(String.class);
	    Table numbers = new Table(cols(number));

	    Revision tail = Revisions.Empty;

	    PatchTemplate insert = new InsertTemplate
	      (numbers,
	       cols(number, name),
	       list((Expression) new Parameter(),
	            new Parameter()), DuplicateKeyResolution.Throw);

	    try {
	      tail.builder().apply(insert, "1", "one");
	      throw new RuntimeException();
	    } catch (ClassCastException e) { }

	    try {
	      tail.builder().apply(insert, 1, 1);
	      throw new RuntimeException();
	    } catch (ClassCastException e) { }

	    RevisionBuilder builder = tail.builder();

	    builder.apply(insert, 1, "one");

	    Revision first = builder.commit();

	    TableReference numbersReference = new TableReference(numbers);

	    PatchTemplate updateNameWhereNumberEqual = new UpdateTemplate
	      (numbersReference,
	       new BinaryOperation
	       (BinaryOperation.Type.Equal,
	        reference(numbersReference, number),
	        new Parameter()),
	       cols(name),
	       list((Expression) new Parameter()));

	    try {
	      first.builder().apply(updateNameWhereNumberEqual, 1, 2);
	      throw new RuntimeException();
	    } catch (ClassCastException e) { }
   }
   
   @Test (expected=IllegalArgumentException.class)
   public void testNotEnoughColumnsForPrimaryKeyQuery(){
       Column<Integer> key = new Column<Integer>(Integer.class);
       Column<String> firstName = new Column<String>(String.class);
       Column<String> lastName = new Column<String>(String.class);
       Column<String> city = new Column<String>(String.class);
       Table names = new Table(cols(key, city));
       
       try{
           new InsertTemplate
            (names,
                    cols(key, firstName, lastName),
                    list((Expression) new Parameter(),
                            new Parameter(),
                            new Parameter()), DuplicateKeyResolution.Throw);
           fail("Expecting IllegalArgumentException...");
       } catch(IllegalArgumentException expected){}
      
   }
}

package Suites;

import java.sql.Time;
import java.util.Date;
import java.util.UUID;

import junit.framework.TestCase;

import org.junit.Test;

import static com.readytalk.oss.dbms.util.Util.list;
import static com.readytalk.oss.dbms.util.Util.cols;
import static com.readytalk.oss.dbms.ExpressionFactory.reference;

import com.readytalk.oss.dbms.BinaryOperation;
import com.readytalk.oss.dbms.Column;
import com.readytalk.oss.dbms.Expression;
import com.readytalk.oss.dbms.QueryResult;
import com.readytalk.oss.dbms.QueryTemplate;
import com.readytalk.oss.dbms.Revisions;
import com.readytalk.oss.dbms.Table;
import com.readytalk.oss.dbms.Revision;
import com.readytalk.oss.dbms.Parameter;
import com.readytalk.oss.dbms.RevisionBuilder;
import com.readytalk.oss.dbms.PatchTemplate;
import com.readytalk.oss.dbms.InsertTemplate;
import com.readytalk.oss.dbms.UpdateTemplate;
import com.readytalk.oss.dbms.TableReference;
import com.readytalk.oss.dbms.ColumnReference;
import com.readytalk.oss.dbms.Constant;
import com.readytalk.oss.dbms.DuplicateKeyResolution;

public class AlexSandbox extends TestCase{
	
	public class CustomClass{
		int a;
		int b;
		String name;
		
		public void setName(String n){
			name=n;
		}
		public void setA(int n){
			a=n;
		}
		public void setB(int n){
			b=n;
		}
		public void setAll(int x, int y, String str){
			a=x;
			b=y;
			name=str;
		}
	}
	
	@Test
	public void testDataTypes(){
        
    Column<UUID> uuidColumn = new Column<UUID>(UUID.class);
    Column<Long> longColumn= new Column<Long>(Long.class);
		Column<String> nameColumn = new Column<String>(String.class);
		Column<Date> dateColumn = new Column<Date>(Date.class);		
		Column<Float> floatColumn = new Column<Float>(Float.class);
		Column<Integer> intColumn = new Column<Integer>(Integer.class);
		Column<Byte> byteColumn = new Column<Byte>(Byte.class);
		Column<Time> timeColumn = new Column<Time>(Time.class);
		Column<Boolean> boolColumn = new Column<Boolean>(Boolean.class);
		Column<Integer[]> intArrayColumn = new Column<Integer[]>(Integer[].class);
		Column<CustomClass> customClassColumn = new Column<CustomClass>(CustomClass.class);
		
		//Try to create a null data column
		try{
			Column nullColumn = new Column(null);
			fail("We expected an NPE here...");
		}catch(NullPointerException expected){}
		
		//Need to ask Joel about some of these scenarios...
		//Try to create a table with null pk
		Table bogus = new Table(cols(new Column<CustomClass>(CustomClass.class)));		
		
		//Table numbers = new Table(cols(uuidColumn, longColumn, nameColumn, dateColumn, floatColumn,
		//		intColumn, byteColumn, timeColumn, boolColumn));
		
		Table numbers = new Table(cols(uuidColumn, longColumn, nameColumn, dateColumn, floatColumn,
				intColumn, byteColumn, timeColumn));

		Revision tail = Revisions.Empty;
		
		//Setup Insert Data
		UUID uuidData = UUID.randomUUID();
		Long longData = new Long("1234567890");
		String nameData = "Some fine looking string of text...";
		Date creationDate = new Date();
		Float floatData = 42.7f;
		Integer intData = 2147483647;
		Byte byteData = intData.byteValue();
		Time timeData = new Time(longData);
		Boolean boolData = false;
		Integer[] intArrayData = {1,2,3,4,5,6,7,8,9};
		CustomClass customClassData = new CustomClass();
		customClassData.setAll(1,2,"Some Name");
				
		RevisionBuilder builder = tail.builder();

		PatchTemplate insert = new InsertTemplate
		(numbers,
				cols(uuidColumn, longColumn, nameColumn, dateColumn, floatColumn,
						intColumn, byteColumn, timeColumn, boolColumn, intArrayColumn, customClassColumn),
						list((Expression) new Parameter(), new Parameter(), new Parameter(), new Parameter(),
						new Parameter(), new Parameter(), new Parameter(), new Parameter(),
						new Parameter(), new Parameter(), new Parameter()),
						DuplicateKeyResolution.Throw);
		
		builder.apply(insert, uuidData, longData, nameData, creationDate,
				floatData, intData, byteData, timeData, boolData, intArrayData, customClassData);

		Revision first = builder.commit();

		TableReference numbersReference = new TableReference(numbers);
		
		QueryTemplate any = new QueryTemplate
        (list(reference(numbersReference, uuidColumn),
      		    reference(numbersReference, longColumn),
                reference(numbersReference, nameColumn),
                reference(numbersReference, dateColumn),
                reference(numbersReference, floatColumn),
                reference(numbersReference, intColumn),
                reference(numbersReference, byteColumn),
                reference(numbersReference, timeColumn),
                reference(numbersReference, boolColumn),
                reference(numbersReference, intArrayColumn),
                reference(numbersReference, customClassColumn)),
                numbersReference,
                new Constant(true));
		
		QueryResult result = tail.diff(first, any);
		
		assertEquals(result.nextRow(), QueryResult.Type.Inserted);
        assertEquals(result.nextItem(), uuidData);
        assertEquals(result.nextItem(), longData);
        assertEquals(result.nextItem(), nameData);
        assertEquals(result.nextItem(), creationDate);
        assertEquals(result.nextItem(), floatData);
        assertEquals(result.nextItem(), intData);
        assertEquals(result.nextItem(), byteData);
        assertEquals(result.nextItem(), timeData);
        assertEquals(result.nextItem(), boolData);
        assertEquals(result.nextItem(), intArrayData);
        assertEquals(result.nextItem(), customClassData);
        assertEquals(result.nextRow(), QueryResult.Type.End);
	}
	
	@Test
	public void testInsertIncorrectDataType(){
        
        Column<Integer> key = new Column<Integer>(Integer.class);
        Column<Integer> number = new Column<Integer>(Integer.class);
        
        Table table = new Table(cols(key));
        
        Revision tail = Revisions.Empty;
        
        RevisionBuilder builder = tail.builder();
        PatchTemplate insert = new InsertTemplate(
   	    table, 
   	    cols(key, number),
   	    list((Expression) new Parameter(), new Parameter()),
   	    DuplicateKeyResolution.Throw);
        
        try{
        builder.apply(insert, 1, "one");
        fail("Inside testInsertIncorrectDataType: expected ClassCastException...");
        }catch(ClassCastException expected){}
	}
	
	@Test
	public void testInsertIncorrectDataTypeMultiKey(){
        Column<Integer> key = new Column<Integer>(Integer.class);
        Column<Integer> number = new Column<Integer>(Integer.class);
        Column<String> name = new Column<String>(String.class);
        
        Table table = new Table(cols(key, number));
        
        Revision tail = Revisions.Empty;
        
        RevisionBuilder builder = tail.builder();
        PatchTemplate insert = new InsertTemplate(
   	    table, 
   	    cols(key, number, name),
   	    list((Expression) new Parameter(), new Parameter(), new Parameter()),
   	    DuplicateKeyResolution.Throw);
        try{
            builder.apply(insert, 1, 2, 3);
            fail("Inside testInsertIncorrectDataTypeMultiKey: expected ClassCastException...");
        }catch(ClassCastException expected){}
	}
	
	@Test
	public void testApplyAlreadyCommitted(){
        Column<Integer> key = new Column<Integer>(Integer.class);
        Column<Integer> number = new Column<Integer>(Integer.class);
        Column<String> name = new Column<String>(String.class);
        
        Table table = new Table(cols(key, number));
        
        Revision tail = Revisions.Empty;
        
        RevisionBuilder builder = tail.builder();
        PatchTemplate insert = new InsertTemplate(
   	    table, 
   	    cols(key, number, name),
   	    list((Expression) new Parameter(), new Parameter(), new Parameter()),
   	    DuplicateKeyResolution.Throw);
        
        builder.apply(insert, 1, 2, "three");
        
        Revision first = builder.commit();
        
        try{
        	builder.apply(insert, 2,3,"four");
        	fail("Inside testApplyAlreadyCommitted: expected IllegalStateException...");
        }catch(IllegalStateException expected){}
	}
	
	
	@Test
	public void testInvalidApplyWrongNumberOfParameters(){
	 Column<Integer> number = new Column<Integer>(Integer.class);
	 Column<Integer> number1 = new Column<Integer>(Integer.class);
	 Column<Integer> number2 = new Column<Integer>(Integer.class);
	 
	 Table numbers = new Table(cols(number));
		   
	 Revision tail = Revisions.Empty;
	 RevisionBuilder builder = tail.builder();
	 PatchTemplate insert = new InsertTemplate(
	   numbers, 
	   cols(number, number1, number2),
	   list((Expression) new Parameter(), new Parameter(), new Parameter()),
	   DuplicateKeyResolution.Throw);
		   
	 builder.apply(insert, 1, 2, 3);
	 try{
	 builder.apply(insert, 2);
	 fail("Expected Illegal Argument Exception...");
	 }catch(IllegalArgumentException expected){}
	 
	 builder = tail.builder();
	 try{
     builder.apply(insert, 3, 2, 1, 1);
     fail("Expected Illegal Argument Exception...");
	 }catch(IllegalArgumentException expected){}
	 
	 builder = tail.builder();

	 Revision first = builder.commit();
		   
	 TableReference numbersReference = new TableReference(numbers);
		   
	 QueryTemplate myQT = new QueryTemplate(
	   list(reference(numbersReference, number)),
	   numbersReference,
	   new Constant(true));
		   
	 QueryResult result = tail.diff(first, myQT);
		   
	 assertEquals(result.nextRow(), QueryResult.Type.End);
	}
	
	
   @Test
   public void testNoValuesInserted(){
       Column<Integer> number = new Column<Integer>(Integer.class);
       Table numbers = new Table(cols(number));
       
       Revision tail = Revisions.Empty;
       RevisionBuilder builder = tail.builder();
       PatchTemplate insert = new InsertTemplate(
               numbers,
               cols(number),
               list((Expression) new Parameter()),
               DuplicateKeyResolution.Throw);


       Revision first = builder.commit();

       TableReference numbersReference = new TableReference(numbers);

       QueryTemplate myQT = new QueryTemplate(
        list(reference(numbersReference, number)),
				numbersReference,
				new Constant(true));

       QueryResult result = tail.diff(first, myQT);

       assertEquals(result.nextRow(), QueryResult.Type.End);
   }

   @Test
   public void testInsertNoPrimaryKey(){
	   Column<Integer> key = new Column<Integer>(Integer.class);
	   Column<Integer> number = new Column<Integer>(Integer.class);
	   Table numbers = new Table(cols(key));
	   
	   Revision tail = Revisions.Empty;
	   RevisionBuilder builder = tail.builder();
	   try{
	   PatchTemplate insert = new InsertTemplate(
			   numbers, cols(number), list((Expression) new Parameter()), DuplicateKeyResolution.Throw);
	   fail("Expected Illegal Argument Exception");
	   }catch(IllegalArgumentException expected){}

	   Revision first = builder.commit();
	   
	   TableReference numbersReference = new TableReference(numbers);
	   
	   QueryTemplate myQT = new QueryTemplate(
	       list(reference(numbersReference, key)),
			   numbersReference,
			   new Constant(true));
	   
	   QueryResult result = tail.diff(first, myQT);
	   
	   assertEquals(result.nextRow(), QueryResult.Type.End);	   
   }
   	
	
   @Test
   public void testInsertKeyOnly(){
	   Column<Integer> number = new Column<Integer>(Integer.class);
	   Table numbers = new Table(cols(number));
	   
	   Revision tail = Revisions.Empty;
	   RevisionBuilder builder = tail.builder();
	   PatchTemplate insert = new InsertTemplate(
			   numbers, cols(number), list((Expression) new Parameter()), DuplicateKeyResolution.Throw);
	   
	   builder.apply(insert, 1);
	   builder.apply(insert, 2);
	   builder.apply(insert, 3);
	   builder.apply(insert, 4);
	   builder.apply(insert, 5);
	   
	   Revision first = builder.commit();
	   
	   TableReference numbersReference = new TableReference(numbers);
	   
	   QueryTemplate myQT = new QueryTemplate(
	       list(reference(numbersReference, number)),
			   numbersReference,
			   new Constant(true));
	   
	   QueryResult result = tail.diff(first, myQT);
	   
	   assertEquals(result.nextRow(), QueryResult.Type.Inserted);
	   assertEquals(result.nextItem(), 1);
	   assertEquals(result.nextRow(), QueryResult.Type.Inserted);
	   assertEquals(result.nextItem(), 2);
	   assertEquals(result.nextRow(), QueryResult.Type.Inserted);
	   assertEquals(result.nextItem(), 3);
	   assertEquals(result.nextRow(), QueryResult.Type.Inserted);
	   assertEquals(result.nextItem(), 4);
	   assertEquals(result.nextRow(), QueryResult.Type.Inserted);
	   assertEquals(result.nextItem(), 5);
	   assertEquals(result.nextRow(), QueryResult.Type.End);
   }
	
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
       Revision tail = Revisions.Empty;
       
       RevisionBuilder builder = tail.builder();
       try{
       PatchTemplate insert = new InsertTemplate
        (names,
                cols(key, firstName, lastName),
                list((Expression) new Parameter(),
                        new Parameter(),
                        new Parameter()), DuplicateKeyResolution.Throw);
       fail("Expecting IllegalArgumentException...");
       }catch(IllegalArgumentException expected){}
      
       }
  
}

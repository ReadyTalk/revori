package Suites;

import java.awt.Image;
import java.awt.List;
import java.nio.charset.Charset;
import java.sql.Time;
import java.text.DecimalFormat;
import java.util.Date;
import java.util.UUID;

import javax.xml.soap.Text;

import junit.framework.TestCase;

import org.junit.Test;

import static com.readytalk.oss.dbms.util.Util.list;
import static org.junit.Assert.*;

import com.readytalk.oss.dbms.DBMS;
import com.readytalk.oss.dbms.BinaryOperation;
import com.readytalk.oss.dbms.Column;
import com.readytalk.oss.dbms.Expression;
import com.readytalk.oss.dbms.QueryResult;
import com.readytalk.oss.dbms.QueryTemplate;
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
import com.readytalk.oss.dbms.imp.MyDBMS;


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
		DBMS dbms = new MyDBMS();
        
        Column uuidColumn = new Column(UUID.class);
        Column longColumn= new Column(Long.class);
		Column nameColumn = new Column(String.class);
		Column dateColumn = new Column(Date.class);		
		Column floatColumn = new Column(Float.class);
		Column intColumn = new Column(Integer.class);
		Column byteColumn = new Column(Byte.class);
		Column timeColumn = new Column(Time.class);
		Column boolColumn = new Column(Boolean.class);
		Column intArrayColumn = new Column(Integer[].class);
		Column customClassColumn = new Column(CustomClass.class);
		
		//Try to create a null data column
		try{
			Column nullColumn = new Column(null);
			fail("We expected an NPE here...");
		}catch(NullPointerException expected){}
		
		//Need to ask Joel about some of these scenarios...
		//Try to create a table with null pk
		Table bogus = new Table(list(new Column(CustomClass.class)));		
		
		//Table numbers = new Table(list(uuidColumn, longColumn, nameColumn, dateColumn, floatColumn,
		//		intColumn, byteColumn, timeColumn, boolColumn));
		
		Table numbers = new Table(list(uuidColumn, longColumn, nameColumn, dateColumn, floatColumn,
				intColumn, byteColumn, timeColumn));

		Revision tail = dbms.revision();
		
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
				
		RevisionBuilder builder = dbms.builder(tail);

		PatchTemplate insert = new InsertTemplate
		(numbers,
				list(uuidColumn, longColumn, nameColumn, dateColumn, floatColumn,
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
        (list((Expression) new ColumnReference(numbersReference, uuidColumn),
      		    (Expression) new ColumnReference(numbersReference, longColumn),
                (Expression) new ColumnReference(numbersReference, nameColumn),
                (Expression) new ColumnReference(numbersReference, dateColumn),
                (Expression) new ColumnReference(numbersReference, floatColumn),
                (Expression) new ColumnReference(numbersReference, intColumn),
                (Expression) new ColumnReference(numbersReference, byteColumn),
                (Expression) new ColumnReference(numbersReference, timeColumn),
                (Expression) new ColumnReference(numbersReference, boolColumn),
                (Expression) new ColumnReference(numbersReference, intArrayColumn),
                (Expression) new ColumnReference(numbersReference, customClassColumn)),
                numbersReference,
                new Constant(true));
		
		QueryResult result = dbms.diff(tail, first, any);
		
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
        DBMS dbms = new MyDBMS();
        
        Column key = new Column(Integer.class);
        Column number = new Column(Integer.class);
        
        Table table = new Table(list(key));
        
        Revision tail = dbms.revision();
        
        RevisionBuilder builder = dbms.builder(tail);
        PatchTemplate insert = new InsertTemplate(
   	    table, 
   	    list(key, number),
   	    list((Expression) new Parameter(), new Parameter()),
   	    DuplicateKeyResolution.Throw);
        
        try{
        builder.apply(insert, 1, "one");
        fail("Inside testInsertIncorrectDataType: expected ClassCastException...");
        }catch(ClassCastException expected){}
	}
	
	@Test
	public void testInsertIncorrectDataTypeMultiKey(){
        DBMS dbms = new MyDBMS();
        
        Column key = new Column(Integer.class);
        Column number = new Column(Integer.class);
        Column name = new Column(String.class);
        
        Table table = new Table(list(key, number));
        
        Revision tail = dbms.revision();
        
        RevisionBuilder builder = dbms.builder(tail);
        PatchTemplate insert = new InsertTemplate(
   	    table, 
   	    list(key, number, name),
   	    list((Expression) new Parameter(), new Parameter(), new Parameter()),
   	    DuplicateKeyResolution.Throw);
        try{
            builder.apply(insert, 1, 2, 3);
            fail("Inside testInsertIncorrectDataTypeMultiKey: expected ClassCastException...");
        }catch(ClassCastException expected){}
	}
	
	@Test
	public void testApplyAlreadyCommitted(){
        DBMS dbms = new MyDBMS();
        
        Column key = new Column(Integer.class);
        Column number = new Column(Integer.class);
        Column name = new Column(String.class);
        
        Table table = new Table(list(key, number));
        
        Revision tail = dbms.revision();
        
        RevisionBuilder builder = dbms.builder(tail);
        PatchTemplate insert = new InsertTemplate(
   	    table, 
   	    list(key, number, name),
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
	 DBMS dbms = new MyDBMS();
	 Column number = new Column(Integer.class);
	 Column number1 = new Column(Integer.class);
	 Column number2 = new Column(Integer.class);
	 
	 Table numbers = new Table(list(number));
		   
	 Revision tail = dbms.revision();
	 RevisionBuilder builder = dbms.builder(tail);
	 PatchTemplate insert = new InsertTemplate(
	   numbers, 
	   list(number, number1, number2),
	   list((Expression) new Parameter(), new Parameter(), new Parameter()),
	   DuplicateKeyResolution.Throw);
		   
	 builder.apply(insert, 1, 2, 3);
	 try{
	 builder.apply(insert, 2);
	 fail("Expected Illegal Argument Exception...");
	 }catch(IllegalArgumentException expected){}
	 
	 builder = dbms.builder(tail);
	 try{
     builder.apply(insert, 3, 2, 1, 1);
     fail("Expected Illegal Argument Exception...");
	 }catch(IllegalArgumentException expected){}
	 
	 builder = dbms.builder(tail);

	 Revision first = builder.commit();
		   
	 TableReference numbersReference = new TableReference(numbers);
		   
	 QueryTemplate myQT = new QueryTemplate(
	   list((Expression) new ColumnReference(numbersReference, number)),
	   numbersReference,
	   new Constant(true));
		   
	 QueryResult result = dbms.diff(tail, first, myQT);
		   
	 assertEquals(result.nextRow(), QueryResult.Type.End);
	}
	
	
   @Test
   public void testNoValuesInserted(){
       DBMS dbms = new MyDBMS();
       Column number = new Column(Integer.class);
       Table numbers = new Table(list(number));
       
       Revision tail = dbms.revision();
       RevisionBuilder builder = dbms.builder(tail);
       PatchTemplate insert = new InsertTemplate(
               numbers,
               list(number),
               list((Expression) new Parameter()),
               DuplicateKeyResolution.Throw);


       Revision first = builder.commit();

       TableReference numbersReference = new TableReference(numbers);

       QueryTemplate myQT = new QueryTemplate(
				list((Expression) new ColumnReference(numbersReference, number)),
				numbersReference,
				new Constant(true));

       QueryResult result = dbms.diff(tail, first, myQT);

       assertEquals(result.nextRow(), QueryResult.Type.End);
   }

   @Test
   public void testInsertNoPrimaryKey(){
	   DBMS dbms = new MyDBMS();
	   Column key = new Column(Integer.class);
	   Column number = new Column(Integer.class);
	   Table numbers = new Table(list(key));
	   
	   Revision tail = dbms.revision();
	   RevisionBuilder builder = dbms.builder(tail);
	   try{
	   PatchTemplate insert = new InsertTemplate(
			   numbers, list(number), list((Expression) new Parameter()), DuplicateKeyResolution.Throw);
	   fail("Expected Illegal Argument Exception");
	   }catch(IllegalArgumentException expected){}

	   Revision first = builder.commit();
	   
	   TableReference numbersReference = new TableReference(numbers);
	   
	   QueryTemplate myQT = new QueryTemplate(
			   list((Expression) new ColumnReference(numbersReference, key)),
			   numbersReference,
			   new Constant(true));
	   
	   QueryResult result = dbms.diff(tail, first, myQT);
	   
	   assertEquals(result.nextRow(), QueryResult.Type.End);	   
   }
   	
	
   @Test
   public void testInsertKeyOnly(){
	   DBMS dbms = new MyDBMS();
	   Column number = new Column(Integer.class);
	   Table numbers = new Table(list(number));
	   
	   Revision tail = dbms.revision();
	   RevisionBuilder builder = dbms.builder(tail);
	   PatchTemplate insert = new InsertTemplate(
			   numbers, list(number), list((Expression) new Parameter()), DuplicateKeyResolution.Throw);
	   
	   builder.apply(insert, 1);
	   builder.apply(insert, 2);
	   builder.apply(insert, 3);
	   builder.apply(insert, 4);
	   builder.apply(insert, 5);
	   
	   Revision first = builder.commit();
	   
	   TableReference numbersReference = new TableReference(numbers);
	   
	   QueryTemplate myQT = new QueryTemplate(
			   list((Expression) new ColumnReference(numbersReference, number)),
			   numbersReference,
			   new Constant(true));
	   
	   QueryResult result = dbms.diff(tail, first, myQT);
	   
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
	   DBMS dbms = new MyDBMS();

	    Column number = new Column(Integer.class);
	    Column name = new Column(String.class);
	    Table numbers = new Table(list(number));

	    Revision tail = dbms.revision();

	    PatchTemplate insert = new InsertTemplate
	      (numbers,
	       list(number, name),
	       list((Expression) new Parameter(),
	            new Parameter()), DuplicateKeyResolution.Throw);

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
   
   @Test (expected=IllegalArgumentException.class)
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
       fail("Expecting IllegalArgumentException...");
       }catch(IllegalArgumentException expected){}
      
       }
  
}

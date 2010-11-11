package Suites;

import java.awt.Image;
import java.awt.List;
import java.nio.charset.Charset;
import java.sql.Time;
import java.text.DecimalFormat;
import java.util.Date;
import java.util.UUID;

import javax.swing.text.StyleContext.SmallAttributeSet;
import javax.xml.soap.Text;

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
        
        Column uuidColumn = dbms.column(UUID.class);
        Column longColumn= dbms.column(Long.class);
		Column nameColumn = dbms.column(String.class);
		Column dateColumn = dbms.column(Date.class);		
		Column floatColumn = dbms.column(Float.class);
		Column intColumn = dbms.column(Integer.class);
		Column byteColumn = dbms.column(Byte.class);
		Column timeColumn = dbms.column(Time.class);
		Column boolColumn = dbms.column(Boolean.class);
		Column intArrayColumn = dbms.column(Integer[].class);
		Column customClassColumn = dbms.column(CustomClass.class);
		
		//Try to create a null data column
		try{
			Column nullColumn = dbms.column(null);
			fail("We expected an NPE here...");
		}catch(NullPointerException expected){}
		
		//Need to ask Joel about some of these scenarios...
		//Try to create a table with null pk
		Table bogus = dbms.table(list(dbms.column(CustomClass.class)));		
		
		//Table numbers = dbms.table(list(uuidColumn, longColumn, nameColumn, dateColumn, floatColumn,
		//		intColumn, byteColumn, timeColumn, boolColumn));
		
		Table numbers = dbms.table(list(uuidColumn, longColumn, nameColumn, dateColumn, floatColumn,
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
				
		PatchContext context = dbms.patchContext(tail);

		PatchTemplate insert = dbms.insertTemplate
		(numbers,
				list(uuidColumn, longColumn, nameColumn, dateColumn, floatColumn,
						intColumn, byteColumn, timeColumn, boolColumn, intArrayColumn, customClassColumn),
				list(dbms.parameter(), dbms.parameter(), dbms.parameter(), dbms.parameter(),
						dbms.parameter(), dbms.parameter(), dbms.parameter(), dbms.parameter(),
						dbms.parameter(), dbms.parameter(), dbms.parameter()),
						DuplicateKeyResolution.Throw);
		
		dbms.apply(context, insert, uuidData, longData, nameData, creationDate,
				floatData, intData, byteData, timeData, boolData, intArrayData, customClassData);

		Revision first = dbms.commit(context);

		TableReference numbersReference = dbms.tableReference(numbers);
		
		QueryTemplate any = dbms.queryTemplate
        (list((Expression) dbms.columnReference(numbersReference, uuidColumn),
      		    (Expression) dbms.columnReference(numbersReference, longColumn),
                (Expression) dbms.columnReference(numbersReference, nameColumn),
                (Expression) dbms.columnReference(numbersReference, dateColumn),
                (Expression) dbms.columnReference(numbersReference, floatColumn),
                (Expression) dbms.columnReference(numbersReference, intColumn),
                (Expression) dbms.columnReference(numbersReference, byteColumn),
                (Expression) dbms.columnReference(numbersReference, timeColumn),
                (Expression) dbms.columnReference(numbersReference, boolColumn),
                (Expression) dbms.columnReference(numbersReference, intArrayColumn),
                (Expression) dbms.columnReference(numbersReference, customClassColumn)),
                numbersReference,
                dbms.constant(true));
		
		QueryResult result = dbms.diff(tail, first, any);
		
		assertEquals(result.nextRow(), ResultType.Inserted);
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
        assertEquals(result.nextRow(), ResultType.End);
	}
	
	@Test
	public void testInsertIncorrectDataType(){
        DBMS dbms = new MyDBMS();
        
        Column key = dbms.column(Integer.class);
        Column number = dbms.column(Integer.class);
        
        Table table = dbms.table(list(key));
        
        Revision tail = dbms.revision();
        
        PatchContext context = dbms.patchContext(tail);
        PatchTemplate insert = dbms.insertTemplate(
   	    table, 
   	    list(key, number),
   	    list(dbms.parameter(), dbms.parameter()),
   	    DuplicateKeyResolution.Throw);
        
        try{
        dbms.apply(context, insert, 1, "one");
        fail("Inside testInsertIncorrectDataType: expected ClassCastException...");
        }catch(ClassCastException expected){}
	}
	
	@Test
	public void testInsertIncorrectDataTypeMultiKey(){
        DBMS dbms = new MyDBMS();
        
        Column key = dbms.column(Integer.class);
        Column number = dbms.column(Integer.class);
        Column name = dbms.column(String.class);
        
        Table table = dbms.table(list(key, number));
        
        Revision tail = dbms.revision();
        
        PatchContext context = dbms.patchContext(tail);
        PatchTemplate insert = dbms.insertTemplate(
   	    table, 
   	    list(key, number, name),
   	    list(dbms.parameter(), dbms.parameter(), dbms.parameter()),
   	    DuplicateKeyResolution.Throw);
        try{
            dbms.apply(context, insert, 1, 2, 3);
            fail("Inside testInsertIncorrectDataTypeMultiKey: expected ClassCastException...");
        }catch(ClassCastException expected){}
	}
	
	@Test
	public void testApplyAlreadyCommitted(){
        DBMS dbms = new MyDBMS();
        
        Column key = dbms.column(Integer.class);
        Column number = dbms.column(Integer.class);
        Column name = dbms.column(String.class);
        
        Table table = dbms.table(list(key, number));
        
        Revision tail = dbms.revision();
        
        PatchContext context = dbms.patchContext(tail);
        PatchTemplate insert = dbms.insertTemplate(
   	    table, 
   	    list(key, number, name),
   	    list(dbms.parameter(), dbms.parameter(), dbms.parameter()),
   	    DuplicateKeyResolution.Throw);
        
        dbms.apply(context, insert, 1, 2, "three");
        
        Revision first = dbms.commit(context);
        
        try{
        	dbms.apply(context, insert, 2,3,"four");
        	fail("Inside testApplyAlreadyCommitted: expected IllegalStateException...");
        }catch(IllegalStateException expected){}
	}
	
	
	@Test
	public void testInvalidApplyWrongNumberOfParameters(){
	 DBMS dbms = new MyDBMS();
	 Column number = dbms.column(Integer.class);
	 Column number1 = dbms.column(Integer.class);
	 Column number2 = dbms.column(Integer.class);
	 
	 Table numbers = dbms.table(list(number));
		   
	 Revision tail = dbms.revision();
	 PatchContext context = dbms.patchContext(tail);
	 PatchTemplate insert = dbms.insertTemplate(
	   numbers, 
	   list(number, number1, number2),
	   list(dbms.parameter(), dbms.parameter(), dbms.parameter()),
	   DuplicateKeyResolution.Throw);
		   
	 dbms.apply(context, insert, 1, 2, 3);
	 try{
	 dbms.apply(context, insert, 2);
	 fail("Expected Illegal Argument Exception...");
	 }catch(IllegalArgumentException expected){}
	 
	 context = dbms.patchContext(tail);
	 try{
     dbms.apply(context, insert, 3, 2, 1, 1);
     fail("Expected Illegal Argument Exception...");
	 }catch(IllegalArgumentException expected){}
	 
	 context = dbms.patchContext(tail);

	 Revision first = dbms.commit(context);
		   
	 TableReference numbersReference = dbms.tableReference(numbers);
		   
	 QueryTemplate myQT = dbms.queryTemplate(
	   list((Expression) dbms.columnReference(numbersReference, number)),
	   numbersReference,
	   dbms.constant(true));
		   
	 QueryResult result = dbms.diff(tail, first, myQT);
		   
	 assertEquals(result.nextRow(), ResultType.End);
	}
	
	
   @Test
   public void testNoValuesInserted(){
       DBMS dbms = new MyDBMS();
       Column number = dbms.column(Integer.class);
       Table numbers = dbms.table(list(number));
       
       Revision tail = dbms.revision();
       PatchContext context = dbms.patchContext(tail);
       PatchTemplate insert = dbms.insertTemplate(
               numbers,
               list(number),
               list(dbms.parameter()),
               DuplicateKeyResolution.Throw);


       Revision first = dbms.commit(context);

       TableReference numbersReference = dbms.tableReference(numbers);

       QueryTemplate myQT = dbms.queryTemplate(
				list((Expression) dbms.columnReference(numbersReference, number)),
				numbersReference,
				dbms.constant(true));

       QueryResult result = dbms.diff(tail, first, myQT);

       assertEquals(result.nextRow(), ResultType.End);
   }

   @Test
   public void testInsertNoPrimaryKey(){
	   DBMS dbms = new MyDBMS();
	   Column key = dbms.column(Integer.class);
	   Column number = dbms.column(Integer.class);
	   Table numbers = dbms.table(list(key));
	   
	   Revision tail = dbms.revision();
	   PatchContext context = dbms.patchContext(tail);
	   try{
	   PatchTemplate insert = dbms.insertTemplate(
			   numbers, list(number), list(dbms.parameter()), DuplicateKeyResolution.Throw);
	   fail("Expected Illegal Argument Exception");
	   }catch(IllegalArgumentException expected){}

	   Revision first = dbms.commit(context);
	   
	   TableReference numbersReference = dbms.tableReference(numbers);
	   
	   QueryTemplate myQT = dbms.queryTemplate(
			   list((Expression) dbms.columnReference(numbersReference, key)),
			   numbersReference,
			   dbms.constant(true));
	   
	   QueryResult result = dbms.diff(tail, first, myQT);
	   
	   assertEquals(result.nextRow(), ResultType.End);	   
   }
   	
	
   @Test
   public void testInsertKeyOnly(){
	   DBMS dbms = new MyDBMS();
	   Column number = dbms.column(Integer.class);
	   Table numbers = dbms.table(list(number));
	   
	   Revision tail = dbms.revision();
	   PatchContext context = dbms.patchContext(tail);
	   PatchTemplate insert = dbms.insertTemplate(
			   numbers, list(number), list(dbms.parameter()), DuplicateKeyResolution.Throw);
	   
	   dbms.apply(context, insert, 1);
	   dbms.apply(context, insert, 2);
	   dbms.apply(context, insert, 3);
	   dbms.apply(context, insert, 4);
	   dbms.apply(context, insert, 5);
	   
	   Revision first = dbms.commit(context);
	   
	   TableReference numbersReference = dbms.tableReference(numbers);
	   
	   QueryTemplate myQT = dbms.queryTemplate(
			   list((Expression) dbms.columnReference(numbersReference, number)),
			   numbersReference,
			   dbms.constant(true));
	   
	   QueryResult result = dbms.diff(tail, first, myQT);
	   
	   assertEquals(result.nextRow(), ResultType.Inserted);
	   assertEquals(result.nextItem(), 1);
	   assertEquals(result.nextRow(), ResultType.Inserted);
	   assertEquals(result.nextItem(), 2);
	   assertEquals(result.nextRow(), ResultType.Inserted);
	   assertEquals(result.nextItem(), 3);
	   assertEquals(result.nextRow(), ResultType.Inserted);
	   assertEquals(result.nextItem(), 4);
	   assertEquals(result.nextRow(), ResultType.Inserted);
	   assertEquals(result.nextItem(), 5);
	   assertEquals(result.nextRow(), ResultType.End);
   }
	
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
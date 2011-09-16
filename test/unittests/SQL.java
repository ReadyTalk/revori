package unittests;

import static com.readytalk.oss.dbms.server.SQLServer.readString;

import java.io.IOException;
import java.io.InputStream;

import org.junit.Test;
import junit.framework.TestCase;

import com.readytalk.oss.dbms.server.SQLServer;
import com.readytalk.oss.dbms.server.SQLServer.Connection;
import com.readytalk.oss.dbms.server.SQLServer.Response;
import com.readytalk.oss.dbms.server.SQLServer.RowSetFlag;

public class SQL extends TestCase {
  @Test
  public void testLiterals() throws IOException {
    Connection connection = new SQLServer("test").makeConnection();

    assertEquals(Response.Success.ordinal(), connection.execute("create database test").read());

    assertEquals(Response.NewDatabase.ordinal(), connection.execute("use database test").read());

    assertEquals(Response.Success.ordinal(), connection.execute
     ("create table test"
      + " ( number int32, name string, primary key ( number ) )").read());

    assertEquals(Response.Success.ordinal(), connection.execute
     ("insert into test values ( 42, 'forty-two' )").read());

    InputStream in = connection.execute("select number from test");

    assertEquals(Response.RowSet.ordinal(), in.read());
    assertEquals(RowSetFlag.InsertedRow.ordinal(), in.read());
    assertEquals(RowSetFlag.Item.ordinal(), in.read());
    assertEquals("42", readString(in));
    assertEquals(RowSetFlag.End.ordinal(), in.read());

    in = connection.execute("select name from test where number = 42");

    assertEquals(Response.RowSet.ordinal(), in.read());
    assertEquals(RowSetFlag.InsertedRow.ordinal(), in.read());
    assertEquals(RowSetFlag.Item.ordinal(), in.read());
    assertEquals("forty-two", readString(in));
    assertEquals(RowSetFlag.End.ordinal(), in.read());

    assertEquals(Response.Success.ordinal(), connection.execute
     ("update test set name = null").read());

    in = connection.execute("select name from test where number = 42");

    assertEquals(Response.RowSet.ordinal(), in.read());
    assertEquals(RowSetFlag.InsertedRow.ordinal(), in.read());
    assertEquals(RowSetFlag.Item.ordinal(), in.read());
    assertEquals("null", readString(in));
    assertEquals(RowSetFlag.End.ordinal(), in.read());
  }
  
  /*@Test
  public void testAggregates()  throws IOException {
    Connection connection = new SQLServer("test").makeConnection();

    assertEquals(Response.Success.ordinal(), connection.execute("create database test").read());

    assertEquals(Response.NewDatabase.ordinal(), connection.execute("use database test").read());

    assertEquals(Response.Success.ordinal(), connection.execute
     ("create table test"
      + " ( number int32, name string, primary key ( number ) )").read());

    assertEquals(Response.Success.ordinal(), connection.execute
     ("insert into test values ( 42, 'forty-two' )").read());

    assertEquals(Response.Success.ordinal(), connection.execute
     ("insert into test values ( 28, 'twenty-eight' )").read());

    InputStream in = connection.execute("select count(number) from test");

    assertEquals(Response.RowSet.ordinal(), in.read());
    assertEquals(RowSetFlag.InsertedRow.ordinal(), in.read());
    assertEquals(RowSetFlag.Item.ordinal(), in.read());
    assertEquals("2", readString(in));
    assertEquals(RowSetFlag.End.ordinal(), in.read());
  }*/
  
  @Test
  public void testOrderBy() throws IOException {
    Connection connection = new SQLServer("test").makeConnection();

    assertEquals(Response.Success.ordinal(), connection.execute("create database test").read());

    assertEquals(Response.NewDatabase.ordinal(), connection.execute("use database test").read());

    assertEquals(Response.Success.ordinal(), connection.execute
     ("create table test"
      + " ( number int32, name string, primary key ( number ) )").read());

    assertEquals(Response.Success.ordinal(), connection.execute
     ("insert into test values ( 42, 'forty-two' )").read());

    assertEquals(Response.Success.ordinal(), connection.execute
     ("insert into test values ( 28, 'twenty-eight' )").read());

    InputStream in = connection.execute("select name from test order by number asc");

    assertEquals(Response.RowSet.ordinal(), in.read());
    assertEquals(RowSetFlag.InsertedRow.ordinal(), in.read());
    assertEquals(RowSetFlag.Item.ordinal(), in.read());
    assertEquals("twenty-eight", readString(in));
    assertEquals(RowSetFlag.InsertedRow.ordinal(), in.read());
    assertEquals(RowSetFlag.Item.ordinal(), in.read());
    assertEquals("forty-two", readString(in));
    assertEquals(RowSetFlag.End.ordinal(), in.read());
    
    in = connection.execute("select name from test order by name");

    assertEquals(Response.RowSet.ordinal(), in.read());
    assertEquals(RowSetFlag.InsertedRow.ordinal(), in.read());
    assertEquals(RowSetFlag.Item.ordinal(), in.read());
    assertEquals("forty-two", readString(in));
    assertEquals(RowSetFlag.InsertedRow.ordinal(), in.read());
    assertEquals(RowSetFlag.Item.ordinal(), in.read());
    assertEquals("twenty-eight", readString(in));
    assertEquals(RowSetFlag.End.ordinal(), in.read());

    in = connection.execute("select name from test order by name desc");

    assertEquals(Response.RowSet.ordinal(), in.read());
    assertEquals(RowSetFlag.InsertedRow.ordinal(), in.read());
    assertEquals(RowSetFlag.Item.ordinal(), in.read());
    assertEquals("twenty-eight", readString(in));
    assertEquals(RowSetFlag.InsertedRow.ordinal(), in.read());
    assertEquals(RowSetFlag.Item.ordinal(), in.read());
    assertEquals("forty-two", readString(in));
    assertEquals(RowSetFlag.End.ordinal(), in.read());

  }
}

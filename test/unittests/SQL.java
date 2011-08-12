package unittests;

import static org.junit.Assert.assertEquals;

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
  private static void expectEqual(Object actual, Object expected) {
    assertEquals(expected, actual);
  }

  @Test
  public void testLiterals() throws IOException {
    Connection connection = new SQLServer("test").makeConnection();

    expectEqual
      (connection.execute("create database test").read(),
       Response.Success.ordinal());

    expectEqual
      (connection.execute("use database test").read(),
       Response.NewDatabase.ordinal());

    expectEqual
      (connection.execute
       ("create table test"
        + " ( number int32, name string, primary key ( number ) )").read(),
       Response.Success.ordinal());

    expectEqual
      (connection.execute
       ("insert into test values ( 42, 'forty-two' )").read(),
       Response.Success.ordinal());

    InputStream in = connection.execute("select number from test");

    expectEqual(in.read(), Response.RowSet.ordinal());
    expectEqual(in.read(), RowSetFlag.InsertedRow.ordinal());
    expectEqual(in.read(), RowSetFlag.Item.ordinal());
    expectEqual(readString(in), "42");
    expectEqual(in.read(), RowSetFlag.End.ordinal());

    in = connection.execute("select name from test where number = 42");

    expectEqual(in.read(), Response.RowSet.ordinal());
    expectEqual(in.read(), RowSetFlag.InsertedRow.ordinal());
    expectEqual(in.read(), RowSetFlag.Item.ordinal());
    expectEqual(readString(in), "forty-two");
    expectEqual(in.read(), RowSetFlag.End.ordinal());
  }
}

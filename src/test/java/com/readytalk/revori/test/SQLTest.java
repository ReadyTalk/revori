/* Copyright (c) 2010-2012, Revori Contributors

   Permission to use, copy, modify, and/or distribute this software
   for any purpose with or without fee is hereby granted, provided
   that the above copyright notice and this permission notice appear
   in all copies. */

package com.readytalk.revori.test;

import static com.readytalk.revori.server.SQLServer.readString;
import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.io.InputStream;

import org.junit.Test;

import com.readytalk.revori.server.SQLServer;
import com.readytalk.revori.server.SQLServer.Connection;
import com.readytalk.revori.server.SQLServer.Response;
import com.readytalk.revori.server.SQLServer.RowSetFlag;

public class SQLTest {
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

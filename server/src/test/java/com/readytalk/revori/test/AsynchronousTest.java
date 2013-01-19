/* Copyright (c) 2010-2012, Revori Contributors

   Permission to use, copy, modify, and/or distribute this software
   for any purpose with or without fee is hereby granted, provided
   that the above copyright notice and this permission notice appear
   in all copies. */

package com.readytalk.revori.test;

import static com.readytalk.revori.util.Util.cols;
import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.List;

import org.junit.Test;

import com.readytalk.revori.Column;
import com.readytalk.revori.ConflictResolvers;
import com.readytalk.revori.ForeignKeyResolvers;
import com.readytalk.revori.Revision;
import com.readytalk.revori.Table;
import com.readytalk.revori.server.RevisionServer;
import com.readytalk.revori.server.Servers;
import com.readytalk.revori.server.Servers.TaskHandler;
import com.readytalk.revori.server.simple.SimpleRevisionServer;

public class AsynchronousTest {
  @Test
  public void test() {
    RevisionServer rawServer = new SimpleRevisionServer
      (ConflictResolvers.Restrict, ForeignKeyResolvers.Restrict);

    MyTaskHandler handler = new MyTaskHandler();

    RevisionServer asyncServer = Servers.asynchronousRevisionServer
      (rawServer, ConflictResolvers.Restrict, ForeignKeyResolvers.Restrict,
       handler);

    Column<Integer> number = new Column<Integer>(Integer.class);
    Column<String> name = new Column<String>(String.class);
    Table numbers = new Table(cols(number));

    Revision base = rawServer.head();
    rawServer.merge(base, base.builder().table(numbers).row(11)
                    .update(name, "once").commit());

    assertEquals("once",
                 rawServer.head().query(name, numbers.primaryKey, 11));

    assertEquals(null,
                 asyncServer.head().query(name, numbers.primaryKey, 11));

    handler.flush();

    assertEquals("once",
                 rawServer.head().query(name, numbers.primaryKey, 11));
    
    assertEquals("once",
                 asyncServer.head().query(name, numbers.primaryKey, 11));

    base = asyncServer.head();
    asyncServer.merge(base, base.builder().table(numbers).row(12)
                      .update(name, "twelve").commit());

    base = asyncServer.head();
    asyncServer.merge(base, base.builder().table(numbers).row(12)
                      .update(name, "doce").commit());

    base = rawServer.head();
    rawServer.merge(base, base.builder().table(numbers).row(10)
                    .update(name, "diez").commit());

    assertEquals("diez",
                 rawServer.head().query(name, numbers.primaryKey, 10));

    assertEquals(null,
                 asyncServer.head().query(name, numbers.primaryKey, 10));

    assertEquals("once",
                 rawServer.head().query(name, numbers.primaryKey, 11));
    
    assertEquals("once",
                 asyncServer.head().query(name, numbers.primaryKey, 11));

    assertEquals("doce",
                 rawServer.head().query(name, numbers.primaryKey, 12));
    
    assertEquals("doce",
                 asyncServer.head().query(name, numbers.primaryKey, 12));

    handler.flush();

    assertEquals("diez",
                 rawServer.head().query(name, numbers.primaryKey, 10));

    assertEquals("diez",
                 asyncServer.head().query(name, numbers.primaryKey, 10));

    assertEquals("once",
                 rawServer.head().query(name, numbers.primaryKey, 11));
    
    assertEquals("once",
                 asyncServer.head().query(name, numbers.primaryKey, 11));

    assertEquals("doce",
                 rawServer.head().query(name, numbers.primaryKey, 12));
    
    assertEquals("doce",
                 asyncServer.head().query(name, numbers.primaryKey, 12));
  }

  private static class MyTaskHandler implements TaskHandler {
    public final List<Runnable> tasks = new ArrayList<Runnable>();

    public void handleTask(Runnable task) {
      tasks.add(task);
    }

    public void flush() {
      for (Runnable task: tasks) {
        task.run();
      }
      tasks.clear();
    }
  }
}

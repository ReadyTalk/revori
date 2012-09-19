/* Copyright (c) 2010-2012, Revori Contributors

   Permission to use, copy, modify, and/or distribute this software
   for any purpose with or without fee is hereby granted, provided
   that the above copyright notice and this permission notice appear
   in all copies. */

package unittests;

import org.junit.Test;
import junit.framework.TestCase;

import java.util.Queue;
import java.util.LinkedList;
import java.util.Arrays;

import com.readytalk.revori.Table;
import com.readytalk.revori.Column;
import com.readytalk.revori.Revision;
import com.readytalk.revori.RevisionBuilder;
import com.readytalk.revori.ForeignKeyResolvers;
import com.readytalk.revori.ConflictResolvers;
import com.readytalk.revori.QueryTemplate;
import com.readytalk.revori.Constant;
import com.readytalk.revori.Expression;
import com.readytalk.revori.TableReference;
import com.readytalk.revori.server.SimpleRevisionServer;
import com.readytalk.revori.subscribe.DiffServer;
import com.readytalk.revori.subscribe.DiffMachine;
import com.readytalk.revori.subscribe.RowListener;

import static com.readytalk.revori.util.Util.cols;
import static com.readytalk.revori.util.Util.set;
import static com.readytalk.revori.util.Util.list;
import static com.readytalk.revori.ExpressionFactory.reference;
import static com.readytalk.revori.DuplicateKeyResolution.Throw;
import static com.readytalk.revori.DuplicateKeyResolution.Overwrite;


public class Subscribe extends TestCase {

  private enum Kind { Update, Delete }

  private static class Update {
    Kind kind;
    Object[] row;

    public Update(Kind kind, Object[] row) {
      this.kind = kind;
      this.row = Arrays.copyOf(row, row.length);
    }

    public boolean equals(Object o) {
      if(!(o instanceof Update)) {
        return false;
      }
      Update u = (Update) o;
      return kind == u.kind && Arrays.equals(row, u.row);
    }

    public String toString() {
      return kind.toString() + " " + Arrays.toString(row);
    }
  }

  private static class MyRowListener implements RowListener {
    Queue<Update> updates = new LinkedList<Update>();

    public void handleUpdate(Object[] row) {
      updates.add(new Update(Kind.Update, row));
    }

    public void handleDelete(Object[] row) {
      updates.add(new Update(Kind.Delete, row));
    }

    public void expect(Kind kind, Object... row) {
      Update u = new Update(kind, row);
      Update real = updates.poll();
      assertEquals(u, real);
    }

    public void expectNothing() {
      assertEquals(null, updates.peek());
    }
  }

  @Test
  public void testSimpleCase() {
    SimpleRevisionServer server = new SimpleRevisionServer(ConflictResolvers.Restrict, ForeignKeyResolvers.Delete);
    DiffServer diffServer = new DiffServer(server);
    DiffMachine machine = new DiffMachine(diffServer);

    Column<Integer> number = new Column<Integer>(Integer.class);
    Column<String> name = new Column<String>(String.class);
    Table numbers = new Table(cols(number));

    Revision base = server.head();
    RevisionBuilder builder = base.builder();
    
    builder.insert(Throw, numbers, 1, name, "one");

    server.merge(base, builder.commit());

    TableReference numbersReference = new TableReference(numbers);

    QueryTemplate query = new QueryTemplate
      (list((Expression) reference(numbersReference, name)),
       numbersReference,
       new Constant(true));

    MyRowListener listener = new MyRowListener();

    machine.register(listener, query);

    listener.expect(Kind.Update, 1, "one");
    listener.expectNothing();

    base = server.head();
    builder = base.builder();

    builder.insert(Throw, numbers, 2, name, "two");

    server.merge(base, builder.commit());

    listener.expect(Kind.Update, 2, "two");
    listener.expectNothing();
  }
}
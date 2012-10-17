/* Copyright (c) 2010-2012, Revori Contributors

   Permission to use, copy, modify, and/or distribute this software
   for any purpose with or without fee is hereby granted, provided
   that the above copyright notice and this permission notice appear
   in all copies. */

package unittests;

import org.junit.Test;

import static com.readytalk.revori.util.Util.cols;
import static com.readytalk.revori.util.Util.set;
import static com.readytalk.revori.DuplicateKeyResolution.Throw;
import static com.readytalk.revori.DuplicateKeyResolution.Overwrite;

import com.readytalk.revori.Table;
import com.readytalk.revori.Index;
import com.readytalk.revori.Column;
import com.readytalk.revori.Revision;
import com.readytalk.revori.RevisionBuilder;
import com.readytalk.revori.ForeignKeyResolver;
import com.readytalk.revori.ForeignKeyResolvers;
import com.readytalk.revori.util.BufferOutputStream;
import com.readytalk.revori.server.EpidemicServer;
import com.readytalk.revori.server.NetworkServer;
import com.readytalk.revori.server.NetworkServer.NodeID;
import com.readytalk.revori.server.NetworkServer.Network;
import com.readytalk.revori.server.NetworkServer.NodeConflictResolver;
import com.readytalk.revori.server.protocol.ReadContext;
import com.readytalk.revori.server.protocol.Writable;
import com.readytalk.revori.server.protocol.Readable;
import com.readytalk.revori.server.protocol.WriteContext;

import java.io.IOException;
import java.io.ByteArrayInputStream;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;
import java.util.Set;

import junit.framework.TestCase;

public class Epidemic extends TestCase{
  private static void expectEqual(Object actual, Object expected) {
    assertEquals(expected, actual);
  }

  @Test
  public void testTwoNodeNetwork() {
    NodeNetwork network = new NodeNetwork();
    NodeConflictResolver conflictResolver = new MyConflictResolver();
    ForeignKeyResolver foreignKeyResolver = ForeignKeyResolvers.Delete;

    Node n1 = new Node(conflictResolver, foreignKeyResolver, network, 1);
    Node n2 = new Node(conflictResolver, foreignKeyResolver, network, 2);
    Node observer = new Node(conflictResolver, foreignKeyResolver, network, 3);

    n1.server.updateView(set(n2.id));
    n2.server.updateView(set(n1.id));

    Column<Integer> number = new Column<Integer>(Integer.class);
    Column<String> name = new Column<String>(String.class);
    Table numbers = new Table(cols(number));

    Revision base = n1.server.head();
    RevisionBuilder builder = base.builder();
    
    builder.insert(Throw, numbers, 1, name, "one");

    n1.server.merge(base, builder.commit());

    Index numbersKey = numbers.primaryKey;

    expectEqual(n1.server.head().query(numbersKey, 1, name), "one");

    base = n2.server.head();
    builder = base.builder();
    
    builder.insert(Throw, numbers, 1, name, "uno");

    n2.server.merge(base, builder.commit());

    expectEqual(n2.server.head().query(numbersKey, 1, name), "uno");

    flush(network);

    expectEqual(n1.server.head().query(numbersKey, 1, name), "one");
    expectEqual(n2.server.head().query(numbersKey, 1, name), "one");

    base = n2.server.head();
    builder = base.builder();

    builder.insert(Overwrite, numbers, 1, name, "ichi");

    n2.server.merge(base, builder.commit());

    expectEqual(n2.server.head().query(numbersKey, 1, name), "ichi");

    n1.server.updateView(set(n2.id, observer.id));
    n2.server.updateView(set(n1.id, observer.id));
    observer.server.updateView(set(n1.id, n2.id));

    flush(network);

    expectEqual(n1.server.head().query(numbersKey, 1, name), "ichi");
    expectEqual(n2.server.head().query(numbersKey, 1, name), "ichi");
    expectEqual(observer.server.head().query(numbersKey, 1, name), "ichi");

    base = n2.server.head();
    builder = base.builder();

    builder.insert(Overwrite, numbers, 3);

    n2.server.merge(base, builder.commit());

    expectEqual(n2.server.head().query(numbersKey, 3, number), 3);

    flush(network);

    expectEqual(n1.server.head().query(numbersKey, 3, number), 3);
    expectEqual(n2.server.head().query(numbersKey, 3, number), 3);
    expectEqual(observer.server.head().query(numbersKey, 3, number), 3);

    // System.out.println("\n\n**** yay! ****\n\n");
  }
  
  @Test
  public void testFrequentUpdates() {

    NodeNetwork network = new NodeNetwork();
    NodeConflictResolver conflictResolver = new NoConflictResolver();
    ForeignKeyResolver foreignKeyResolver = ForeignKeyResolvers.Delete;

    Node n1 = new Node(conflictResolver, foreignKeyResolver, network, 1);
    Node n2 = new Node(conflictResolver, foreignKeyResolver, network, 2);

    n1.server.updateView(set(n2.id));
    n2.server.updateView(set(n1.id));

    Column<Integer> id = new Column<Integer>(Integer.class);
    Column<Integer> value = new Column<Integer>(Integer.class);
    Table valueTable = new Table(cols(id));
    
    flush(network);

    Revision base = n1.server.head();
    RevisionBuilder builder = base.builder();
    
    builder.insert(Throw, valueTable, 0, value, 0);

    n1.server.merge(base, builder.commit());
    
    flush(network, n1.id);

    base = n1.server.head();
    builder = base.builder();
    
    builder.insert(Overwrite, valueTable, 0, value, 1);

    n1.server.merge(base, builder.commit());
    
    flush(network, n1.id);
    
    base = n1.server.head();
    builder = base.builder();
    
    builder.insert(Overwrite, valueTable, 0, value, 2);

    n1.server.merge(base, builder.commit());
    
    flush(network, n2.id);
    
    flush(network);

    Index valueKey = valueTable.primaryKey;
    
    expectEqual(n1.server.head().query(valueKey, 0, value), 2);
    expectEqual(n2.server.head().query(valueKey, 0, value), 2);
  }

  @Test
  public void testAcks() {
    NodeNetwork network = new NodeNetwork();
    NodeConflictResolver conflictResolver = new NoConflictResolver();
    ForeignKeyResolver foreignKeyResolver = ForeignKeyResolvers.Delete;

    Node n1 = new Node(conflictResolver, foreignKeyResolver, network, 1);
    Node n2 = new Node(conflictResolver, foreignKeyResolver, network, 2);

    n1.server.updateView(set(n2.id));
    n2.server.updateView(set(n1.id));

    Column<Integer> number = new Column<Integer>(Integer.class);
    Column<Integer> value = new Column<Integer>(Integer.class);
    Table numbers = new Table(cols(number));

    Revision base = n1.server.head();
    RevisionBuilder builder = base.builder();
    
    builder.insert(Throw, numbers, 1, value, 42);

    n1.server.merge(base, builder.commit());

    Index numbersKey = numbers.primaryKey;

    expectEqual(n1.server.head().query(numbersKey, 1, value), 42);

    base = n2.server.head();
    builder = base.builder();
    
    builder.insert(Throw, numbers, 2, value, 57);

    n2.server.merge(base, builder.commit());

    expectEqual(n2.server.head().query(numbersKey, 2, value), 57);

    flush(network);

    expectEqual(n1.server.head().query(numbersKey, 1, value), 42);
    expectEqual(n2.server.head().query(numbersKey, 1, value), 42);
    expectEqual(n1.server.head().query(numbersKey, 2, value), 57);
    expectEqual(n2.server.head().query(numbersKey, 2, value), 57);

    base = n1.server.head();
    builder = base.builder();
    
    builder.insert(Overwrite, numbers, 1, value, 43);

    n1.server.merge(base, builder.commit());

    base = n2.server.head();
    builder = base.builder();

    builder.insert(Overwrite, numbers, 2, value, 58);

    n2.server.merge(base, builder.commit());

    flush(network);

    expectEqual(n1.server.head().query(numbersKey, 1, value), 43);
    expectEqual(n2.server.head().query(numbersKey, 1, value), 43);
    expectEqual(n1.server.head().query(numbersKey, 2, value), 58);
    expectEqual(n2.server.head().query(numbersKey, 2, value), 58);

    base = n1.server.head();
    builder = base.builder();
    
    builder.insert(Overwrite, numbers, 1, value, 44);

    n1.server.merge(base, builder.commit());

    base = n2.server.head();
    builder = base.builder();

    builder.insert(Overwrite, numbers, 2, value, 59);

    n2.server.merge(base, builder.commit());

    flush(network);

    expectEqual(n1.server.head().query(numbersKey, 1, value), 44);
    expectEqual(n2.server.head().query(numbersKey, 1, value), 44);
    expectEqual(n1.server.head().query(numbersKey, 2, value), 59);
    expectEqual(n2.server.head().query(numbersKey, 2, value), 59);
  }

  @Test
  public void testTwoNodeRestart() {
    NodeNetwork network = new NodeNetwork();
    NodeConflictResolver conflictResolver = new MyConflictResolver();
    ForeignKeyResolver foreignKeyResolver = ForeignKeyResolvers.Delete;

    Node n1 = new Node(conflictResolver, foreignKeyResolver, network, 1);
    Node n2 = new Node(conflictResolver, foreignKeyResolver, network, 2);

    n1.server.updateView(set(n2.id));
    n2.server.updateView(set(n1.id));

    Column<Integer> number = new Column<Integer>(Integer.class);
    Column<String> name = new Column<String>(String.class);
    Table numbers = new Table(cols(number));

    Revision base = n1.server.head();
    RevisionBuilder builder = base.builder();
    
    builder.insert(Throw, numbers, 1, name, "one");

    n1.server.merge(base, builder.commit());
    
    Index numbersKey = numbers.primaryKey;

    flush(network);

    expectEqual(n1.server.head().query(numbersKey, 1, name), "one");
    expectEqual(n2.server.head().query(numbersKey, 1, name), "one");
    
    n1.server.updateView(Collections.<NodeID>emptySet());

    n2.start();
    n1.server.updateView(set(n2.id));
    n2.server.updateView(set(n1.id));
    
    base = n1.server.head();
    builder = base.builder();
    builder.insert(Throw, numbers, 2, name, "two");
    n1.server.merge(base, builder.commit());
    
    flush(network);

    expectEqual(n1.server.head().query(numbersKey, 1, name), "one");
    expectEqual(n2.server.head().query(numbersKey, 1, name), "one");
    expectEqual(n1.server.head().query(numbersKey, 2, name), "two");
    expectEqual(n2.server.head().query(numbersKey, 2, name), "two");
  }
  
  @Test
  public void testTwoNodeRestartWithChanges() {
    NodeNetwork network = new NodeNetwork();
    NodeConflictResolver conflictResolver = new MyConflictResolver();
    ForeignKeyResolver foreignKeyResolver = ForeignKeyResolvers.Delete;

    Node n1 = new Node(conflictResolver, foreignKeyResolver, network, 1);
    Node n2 = new Node(conflictResolver, foreignKeyResolver, network, 2);

    n1.server.updateView(set(n2.id));
    n2.server.updateView(set(n1.id));

    Column<Integer> number = new Column<Integer>(Integer.class);
    Column<String> name = new Column<String>(String.class);
    Table numbers = new Table(cols(number));

    Revision base = n1.server.head();
    RevisionBuilder builder = base.builder();
    
    builder.insert(Throw, numbers, 1, name, "one");

    n1.server.merge(base, builder.commit());

    base = n2.server.head();
    builder = base.builder();
    builder.insert(Throw, numbers, 2, name, "two");
    n2.server.merge(base, builder.commit());
    
    flush(network);

    Index numbersKey = numbers.primaryKey;

    expectEqual(n1.server.head().query(numbersKey, 1, name), "one");
    expectEqual(n2.server.head().query(numbersKey, 1, name), "one");
    expectEqual(n1.server.head().query(numbersKey, 2, name), "two");
    expectEqual(n2.server.head().query(numbersKey, 2, name), "two");
    
    n1.server.updateView(Collections.<NodeID>emptySet());

    n2.start();
    n1.server.updateView(set(n2.id));
    n2.server.updateView(set(n1.id));
    
    base = n2.server.head();
    builder = base.builder();
    builder.insert(Throw, numbers, 3, name, "three");
    n2.server.merge(base, builder.commit());
    
    flush(network);

    expectEqual(n1.server.head().query(numbersKey, 1, name), "one");
    expectEqual(n2.server.head().query(numbersKey, 1, name), "one");
    expectEqual(n1.server.head().query(numbersKey, 2, name), "two");
    expectEqual(n2.server.head().query(numbersKey, 2, name), "two");
    expectEqual(n1.server.head().query(numbersKey, 3, name), "three");
    expectEqual(n2.server.head().query(numbersKey, 3, name), "three");
  }
  
  @Test
  public void testThreeNodeRestart() {
    NodeNetwork network = new NodeNetwork();
    NodeConflictResolver conflictResolver = new MyConflictResolver();
    ForeignKeyResolver foreignKeyResolver = ForeignKeyResolvers.Delete;

    Node n1 = new Node(conflictResolver, foreignKeyResolver, network, 1);
    Node n2 = new Node(conflictResolver, foreignKeyResolver, network, 2);
    Node n3 = new Node(conflictResolver, foreignKeyResolver, network, 3);

    n1.server.updateView(set(n2.id));
    n2.server.updateView(set(n1.id, n3.id));
    n3.server.updateView(set(n2.id));

    Column<Integer> number = new Column<Integer>(Integer.class);
    Column<String> name = new Column<String>(String.class);
    Table numbers = new Table(cols(number));

    Revision base = n1.server.head();
    RevisionBuilder builder = base.builder();
    
    builder.insert(Throw, numbers, 1, name, "one");

    n1.server.merge(base, builder.commit());
    
    Index numbersKey = numbers.primaryKey;

    flush(network);

    expectEqual(n1.server.head().query(numbersKey, 1, name), "one");
    expectEqual(n2.server.head().query(numbersKey, 1, name), "one");
    expectEqual(n3.server.head().query(numbersKey, 1, name), "one");
    
    n2.server.updateView(set(n1.id));

    n3.start();
    n2.server.updateView(set(n1.id, n3.id));
    n3.server.updateView(set(n2.id));
    
    base = n1.server.head();
    builder = base.builder();
    builder.insert(Throw, numbers, 2, name, "two");
    n1.server.merge(base, builder.commit());
    
    flush(network);

    expectEqual(n1.server.head().query(numbersKey, 1, name), "one");
    expectEqual(n2.server.head().query(numbersKey, 1, name), "one");
    expectEqual(n3.server.head().query(numbersKey, 1, name), "one");
    expectEqual(n1.server.head().query(numbersKey, 2, name), "two");
    expectEqual(n2.server.head().query(numbersKey, 2, name), "two");
    expectEqual(n3.server.head().query(numbersKey, 2, name), "two");
  }
  
  @Test
  public void testReconnect() {
    NodeNetwork network = new NodeNetwork();
    NodeConflictResolver conflictResolver = new MyConflictResolver();
    ForeignKeyResolver foreignKeyResolver = ForeignKeyResolvers.Delete;

    Node n1 = new Node(conflictResolver, foreignKeyResolver, network, 1);
    Node n2 = new Node(conflictResolver, foreignKeyResolver, network, 2);

    n1.server.updateView(set(n2.id));
    n2.server.updateView(set(n1.id));

    Column<Integer> number = new Column<Integer>(Integer.class);
    Column<String> name = new Column<String>(String.class);
    Table numbers = new Table(cols(number));

    Revision base = n1.server.head();
    RevisionBuilder builder = base.builder();
    
    builder.insert(Throw, numbers, 1, name, "one");

    n1.server.merge(base, builder.commit());
    
    Index numbersKey = numbers.primaryKey;

    flush(network);

    expectEqual(n1.server.head().query(numbersKey, 1, name), "one");
    expectEqual(n2.server.head().query(numbersKey, 1, name), "one");
    
    n1.server.updateView(Collections.<NodeID>emptySet());
    n2.server.updateView(Collections.<NodeID>emptySet());
    
    base = n1.server.head();
    builder = base.builder();
    builder.insert(Throw, numbers, 2, name, "two");
    n1.server.merge(base, builder.commit());

    if (! network.messages.isEmpty()) {
      fail("network traffic exists despite disconnection");
    }

    n1.server.updateView(set(n2.id));
    n2.server.updateView(set(n1.id));
    
    flush(network);

    // System.out.print("n1: "); n1.server.dump(System.out);
    // System.out.print("n2: "); n2.server.dump(System.out);

    expectEqual(n1.server.head().query(numbersKey, 1, name), "one");
    expectEqual(n2.server.head().query(numbersKey, 1, name), "one");
    expectEqual(n1.server.head().query(numbersKey, 2, name), "two");
    expectEqual(n2.server.head().query(numbersKey, 2, name), "two");
  }

  private static void flush(NodeNetwork network, NodeID... dontDeliverTo) {
    final int MaxIterations = 100;
    final Set<NodeID> ddt = new HashSet<NodeID>(Arrays.asList(dontDeliverTo));
    int iteration = 0;
    // System.out.println("-------flush-------");
    List<Message> undelivered = new ArrayList<Message>();
    while (network.messages.size() > 0) {
      if (iteration++ > MaxIterations) {
        throw new RuntimeException("exceeded maximum iteration count");
      }

      List<Message> messages = new ArrayList<Message>(network.messages);
      network.messages.clear();

      for (Message m: messages) {
        if(ddt.contains(m.destination)) {
          undelivered.add(m);
        } else {
          try {
            Node source = network.nodes.get(m.source);
            Node destination = network.nodes.get(m.destination);
  
            BufferOutputStream buffer = new BufferOutputStream();
            m.body.writeTo(new WriteContext(buffer));
  
            Readable result = (Readable) m.body.getClass().newInstance();
            result.readFrom
              (new ReadContext(new ByteArrayInputStream(buffer.getBuffer(), 0, buffer.size())));
  
            destination.server.accept(m.source, result);
          } catch (InstantiationException e) {
            throw new RuntimeException(e);
          } catch (IllegalAccessException e) {
            throw new RuntimeException(e);
          } catch (IOException e) {
            throw new RuntimeException(e);
          }
        }
      }
    }
    // System.out.println("-------done-------");
    network.messages.addAll(undelivered);
  }

  @Test
  public void testDeleteAndInsert() {
    NodeNetwork network = new NodeNetwork();
    NodeConflictResolver conflictResolver = NetworkServer.NodeConflictRestrict;
    ForeignKeyResolver foreignKeyResolver = ForeignKeyResolvers.Restrict;

    Node n1 = new Node(conflictResolver, foreignKeyResolver, network, 1);
    Node n2 = new Node(conflictResolver, foreignKeyResolver, network, 2);
    Node observer = new Node(conflictResolver, foreignKeyResolver, network, 3);

    n1.server.updateView(set(n2.id));
    n2.server.updateView(set(n1.id));

    Column<Integer> first = new Column<Integer>(Integer.class);
    Column<Integer> second = new Column<Integer>(Integer.class);
    Column<Integer> third = new Column<Integer>(Integer.class);
    Column<String> name = new Column<String>(String.class);
    Table table = new Table(cols(first, second, third));

    Revision base = n1.server.head();

    n1.server.merge
      (base, base.builder().table(table).row(1, 2, 1).update(name, "foo")
       .commit());

    Index key = table.primaryKey;

    expectEqual(n1.server.head().query(name, key, 1, 2, 1), "foo");
    expectEqual(n2.server.head().query(name, key, 1, 2, 1), null);

    flush(network);

    expectEqual(n1.server.head().query(name, key, 1, 2, 1), "foo");
    expectEqual(n2.server.head().query(name, key, 1, 2, 1), "foo");

    base = n2.server.head();
    n2.server.merge
      (base, base.builder().table(table).delete(1, 2, 1).commit());

    base = n1.server.head();

    n1.server.merge
      (base, base.builder().table(table).row(1, 2, 2).update(name, "bar")
       .commit());

    expectEqual(n1.server.head().query(name, key, 1, 2, 1), "foo");
    expectEqual(n2.server.head().query(name, key, 1, 2, 1), null);
    expectEqual(n1.server.head().query(name, key, 1, 2, 2), "bar");
    expectEqual(n2.server.head().query(name, key, 1, 2, 2), null);

    flush(network);

    expectEqual(n1.server.head().query(name, key, 1, 2, 1), null);
    expectEqual(n2.server.head().query(name, key, 1, 2, 1), null);
    expectEqual(n1.server.head().query(name, key, 1, 2, 2), "bar");
    expectEqual(n2.server.head().query(name, key, 1, 2, 2), "bar");
  }

  private static class MyConflictResolver implements NodeConflictResolver {
    public Object resolveConflict(NodeID leftNode,
                                  NodeID rightNode,
                                  Table table,
                                  Column column,
                                  Object[] primaryKeyValues,
                                  Object baseValue,
                                  Object leftValue,
                                  Object rightValue)
    {
      if (Integer.parseInt(leftNode.id) <= Integer.parseInt(rightNode.id)) {
        return leftValue;
      } else {
        return rightValue;
      }
    }
  }

  private static class NoConflictResolver implements NodeConflictResolver {
    public Object resolveConflict(NodeID leftNode,
                                  NodeID rightNode,
                                  Table table,
                                  Column column,
                                  Object[] primaryKeyValues,
                                  Object baseValue,
                                  Object leftValue,
                                  Object rightValue)
    {
      fail("conflict encountered");
      throw new UnsupportedOperationException();
    }    
  }

  private static class Node {
    public final NodeID id;
    public EpidemicServer server;
    private final NodeConflictResolver conflictResolver;
    private final ForeignKeyResolver foreignKeyResolver;
    private final Network network;

    public Node(NodeConflictResolver conflictResolver,
                ForeignKeyResolver foreignKeyResolver,
                NodeNetwork network,
                int id)
    {
      this.id = new NodeID(String.valueOf(id));
      this.conflictResolver = conflictResolver;
      this.foreignKeyResolver = foreignKeyResolver;
      this.network = network;
      network.nodes.put(this.id, this);
      start();
    }

    public void start() {
      this.server = new EpidemicServer
        (conflictResolver, foreignKeyResolver, network, this.id);
    }
  }

  private static class Message {
    public final NodeID source;
    public final NodeID destination;
    public final Writable body;

    public Message(NodeID source, NodeID destination, Writable body) {
      this.source = source;
      this.destination = destination;
      this.body = body;
    }
  }

  private static class NodeNetwork implements Network {
    public final Map<NodeID, Node> nodes = new HashMap<NodeID, Node>();
    public final List<Message> messages = new ArrayList<Message>();

    public void send(NodeID source, NodeID destination, Writable message) {
      messages.add(new Message(source, destination, message));
    }
  }
}

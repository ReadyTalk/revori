/* Copyright (c) 2010-2012, Revori Contributors

   Permission to use, copy, modify, and/or distribute this software
   for any purpose with or without fee is hereby granted, provided
   that the above copyright notice and this permission notice appear
   in all copies. */

package com.readytalk.revori.test;

import static com.readytalk.revori.DuplicateKeyResolution.Overwrite;
import static com.readytalk.revori.DuplicateKeyResolution.Throw;
import static com.readytalk.revori.util.Util.cols;
import static com.readytalk.revori.util.Util.set;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.junit.Test;

import com.readytalk.revori.Column;
import com.readytalk.revori.ForeignKeyResolver;
import com.readytalk.revori.ForeignKeyResolvers;
import com.readytalk.revori.Index;
import com.readytalk.revori.Revision;
import com.readytalk.revori.RevisionBuilder;
import com.readytalk.revori.Table;
import com.readytalk.revori.server.EpidemicServer;
import com.readytalk.revori.server.NetworkServer;
import com.readytalk.revori.server.NetworkServer.Network;
import com.readytalk.revori.server.NetworkServer.NodeConflictResolver;
import com.readytalk.revori.server.NetworkServer.NodeID;
import com.readytalk.revori.server.TreeServer;
import com.readytalk.revori.server.protocol.ReadContext;
import com.readytalk.revori.server.protocol.Readable;
import com.readytalk.revori.server.protocol.Writable;
import com.readytalk.revori.server.protocol.WriteContext;
import com.readytalk.revori.util.BufferOutputStream;

public class EpidemicTest {

  @Test
  public void testTwoNodeNetwork() {
    NodeConfig config = new NodeConfig
      (new MyConflictResolver(),
       ForeignKeyResolvers.Delete,
       new NodeNetwork(),
       EpidemicFactory);

    Node n1 = new Node(config, 1);
    Node n2 = new Node(config, 2);
    Node observer = new Node(config, 3);

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

    assertEquals("one", n1.server.head().query(numbersKey, 1, name));

    base = n2.server.head();
    builder = base.builder();
    
    builder.insert(Throw, numbers, 1, name, "uno");

    n2.server.merge(base, builder.commit());

    assertEquals("uno", n2.server.head().query(numbersKey, 1, name));

    flush(config.network);

    assertEquals("one", n1.server.head().query(numbersKey, 1, name));
    assertEquals("one", n2.server.head().query(numbersKey, 1, name));

    base = n2.server.head();
    builder = base.builder();

    builder.insert(Overwrite, numbers, 1, name, "ichi");

    n2.server.merge(base, builder.commit());

    assertEquals("ichi", n2.server.head().query(numbersKey, 1, name));

    n1.server.updateView(set(n2.id, observer.id));
    n2.server.updateView(set(n1.id, observer.id));
    observer.server.updateView(set(n1.id, n2.id));

    flush(config.network);

    assertEquals("ichi", n1.server.head().query(numbersKey, 1, name));
    assertEquals("ichi", n2.server.head().query(numbersKey, 1, name));
    assertEquals("ichi", observer.server.head().query(numbersKey, 1, name));

    base = n2.server.head();
    builder = base.builder();

    builder.insert(Overwrite, numbers, 3);

    n2.server.merge(base, builder.commit());

    assertEquals(3, n2.server.head().query(numbersKey, 3, number));

    flush(config.network);

    assertEquals(3, n1.server.head().query(numbersKey, 3, number));
    assertEquals(3, n2.server.head().query(numbersKey, 3, number));
    assertEquals(3, observer.server.head().query(numbersKey, 3, number));

    // System.out.println("\n\n**** yay! ****\n\n");
  }
  
  @Test
  public void testFrequentUpdates() {
    NodeConfig config = new NodeConfig
      (new MyConflictResolver(),
       ForeignKeyResolvers.Delete,
       new NodeNetwork(),
       EpidemicFactory);

    Node n1 = new Node(config, 1);
    Node n2 = new Node(config, 2);

    n1.server.updateView(set(n2.id));
    n2.server.updateView(set(n1.id));

    Column<Integer> id = new Column<Integer>(Integer.class);
    Column<Integer> value = new Column<Integer>(Integer.class);
    Table valueTable = new Table(cols(id));
    
    flush(config.network);

    Revision base = n1.server.head();
    RevisionBuilder builder = base.builder();
    
    builder.insert(Throw, valueTable, 0, value, 0);

    n1.server.merge(base, builder.commit());
    
    flush(config.network, n1.id);

    base = n1.server.head();
    builder = base.builder();
    
    builder.insert(Overwrite, valueTable, 0, value, 1);

    n1.server.merge(base, builder.commit());
    
    flush(config.network, n1.id);
    
    base = n1.server.head();
    builder = base.builder();
    
    builder.insert(Overwrite, valueTable, 0, value, 2);

    n1.server.merge(base, builder.commit());
    
    flush(config.network, n2.id);
    
    flush(config.network);

    Index valueKey = valueTable.primaryKey;
    
    assertEquals(2, n1.server.head().query(valueKey, 0, value));
    assertEquals(2, n2.server.head().query(valueKey, 0, value));
  }

  @Test
  public void testAcks() {
    NodeConfig config = new NodeConfig
      (new MyConflictResolver(),
       ForeignKeyResolvers.Delete,
       new NodeNetwork(),
       EpidemicFactory);

    Node n1 = new Node(config, 1);
    Node n2 = new Node(config, 2);

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

    assertEquals(42, n1.server.head().query(numbersKey, 1, value));

    base = n2.server.head();
    builder = base.builder();
    
    builder.insert(Throw, numbers, 2, value, 57);

    n2.server.merge(base, builder.commit());

    assertEquals(57, n2.server.head().query(numbersKey, 2, value));

    flush(config.network);

    assertEquals(42, n1.server.head().query(numbersKey, 1, value));
    assertEquals(42, n2.server.head().query(numbersKey, 1, value));
    assertEquals(57, n1.server.head().query(numbersKey, 2, value));
    assertEquals(57, n2.server.head().query(numbersKey, 2, value));

    base = n1.server.head();
    builder = base.builder();
    
    builder.insert(Overwrite, numbers, 1, value, 43);

    n1.server.merge(base, builder.commit());

    base = n2.server.head();
    builder = base.builder();

    builder.insert(Overwrite, numbers, 2, value, 58);

    n2.server.merge(base, builder.commit());

    flush(config.network);

    assertEquals(43, n1.server.head().query(numbersKey, 1, value));
    assertEquals(43, n2.server.head().query(numbersKey, 1, value));
    assertEquals(58, n1.server.head().query(numbersKey, 2, value));
    assertEquals(58, n2.server.head().query(numbersKey, 2, value));

    base = n1.server.head();
    builder = base.builder();
    
    builder.insert(Overwrite, numbers, 1, value, 44);

    n1.server.merge(base, builder.commit());

    base = n2.server.head();
    builder = base.builder();

    builder.insert(Overwrite, numbers, 2, value, 59);

    n2.server.merge(base, builder.commit());

    flush(config.network);

    assertEquals(44, n1.server.head().query(numbersKey, 1, value));
    assertEquals(44, n2.server.head().query(numbersKey, 1, value));
    assertEquals(59, n1.server.head().query(numbersKey, 2, value));
    assertEquals(59, n2.server.head().query(numbersKey, 2, value));
  }

  @Test
  public void testTwoNodeRestart() {
    NodeConfig config = new NodeConfig
      (new MyConflictResolver(),
       ForeignKeyResolvers.Delete,
       new NodeNetwork(),
       EpidemicFactory);

    Node n1 = new Node(config, 1);
    Node n2 = new Node(config, 2);

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

    flush(config.network);

    assertEquals("one", n1.server.head().query(numbersKey, 1, name));
    assertEquals("one", n2.server.head().query(numbersKey, 1, name));
    
    n1.server.updateView(Collections.<NodeID>emptySet());

    n2.start();
    n1.server.updateView(set(n2.id));
    n2.server.updateView(set(n1.id));
    
    base = n1.server.head();
    builder = base.builder();
    builder.insert(Throw, numbers, 2, name, "two");
    n1.server.merge(base, builder.commit());
    
    flush(config.network);

    assertEquals("one", n1.server.head().query(numbersKey, 1, name));
    assertEquals("one", n2.server.head().query(numbersKey, 1, name));
    assertEquals("two", n1.server.head().query(numbersKey, 2, name));
    assertEquals("two", n2.server.head().query(numbersKey, 2, name));
  }
  
  @Test
  public void testTwoNodeRestartWithChanges() {
    NodeConfig config = new NodeConfig
      (new MyConflictResolver(),
       ForeignKeyResolvers.Delete,
       new NodeNetwork(),
       EpidemicFactory);

    Node n1 = new Node(config, 1);
    Node n2 = new Node(config, 2);

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
    
    flush(config.network);

    Index numbersKey = numbers.primaryKey;

    assertEquals("one", n1.server.head().query(numbersKey, 1, name));
    assertEquals("one", n2.server.head().query(numbersKey, 1, name));
    assertEquals("two", n1.server.head().query(numbersKey, 2, name));
    assertEquals("two", n2.server.head().query(numbersKey, 2, name));
    
    n1.server.updateView(Collections.<NodeID>emptySet());

    n2.start();
    n1.server.updateView(set(n2.id));
    n2.server.updateView(set(n1.id));
    
    base = n2.server.head();
    builder = base.builder();
    builder.insert(Throw, numbers, 3, name, "three");
    n2.server.merge(base, builder.commit());
    
    flush(config.network);

    assertEquals("one", n1.server.head().query(numbersKey, 1, name));
    assertEquals("one", n2.server.head().query(numbersKey, 1, name));
    assertEquals("two", n1.server.head().query(numbersKey, 2, name));
    assertEquals("two", n2.server.head().query(numbersKey, 2, name));
    assertEquals("three", n1.server.head().query(numbersKey, 3, name));
    assertEquals("three", n2.server.head().query(numbersKey, 3, name));
  }
  
  public void testThreeNodeRestart(ServerFactory factory) {
    NodeConfig config = new NodeConfig
      (new MyConflictResolver(),
       ForeignKeyResolvers.Delete,
       new NodeNetwork(),
       factory);

    Node n1 = new Node(config, 1);
    Node n2 = new Node(config, 2);
    Node n3 = new Node(config, 3);

    n1.server.updateView(set(n2.id));
    n2.server.updateView(set(n1.id, n3.id));
    n3.server.updateView(set(n2.id));

    Column<Integer> number = new Column<Integer>(Integer.class);
    Column<String> name = new Column<String>(String.class);
    Table numbers = new Table(cols(number));

    Revision base = n1.server.head();

    n1.server.merge
      (base, base.builder().table(numbers).row(1)
       .update(name, "one").commit());
    
    Index numbersKey = numbers.primaryKey;

    flush(config.network);

    assertEquals("one", n1.server.head().query(numbersKey, 1, name));
    assertEquals("one", n2.server.head().query(numbersKey, 1, name));
    assertEquals("one", n3.server.head().query(numbersKey, 1, name));
    
    n2.server.updateView(set(n1.id));

    n3.start();
    n2.server.updateView(set(n1.id, n3.id));
    n3.server.updateView(set(n2.id));

    base = n1.server.head();
    n1.server.merge
      (base, base.builder().table(numbers).row(2)
       .update(name, "two").commit());
    
    flush(config.network);

    assertEquals("one", n1.server.head().query(numbersKey, 1, name));
    assertEquals("one", n2.server.head().query(numbersKey, 1, name));
    assertEquals("one", n3.server.head().query(numbersKey, 1, name));
    assertEquals("two", n1.server.head().query(numbersKey, 2, name));
    assertEquals("two", n2.server.head().query(numbersKey, 2, name));
    assertEquals("two", n3.server.head().query(numbersKey, 2, name));

    base = n2.server.head();
    n2.server.merge
      (base, base.builder().table(numbers).row(3)
       .update(name, "three").commit());

    base = n3.server.head();
    n3.server.merge
      (base, base.builder().table(numbers).delete(1).commit());

    flush(config.network);

    assertNull(n1.server.head().query(numbersKey, 1, name));
    assertNull(n2.server.head().query(numbersKey, 1, name));
    assertNull(n3.server.head().query(numbersKey, 1, name));
    assertEquals("two", n1.server.head().query(numbersKey, 2, name));
    assertEquals("two", n2.server.head().query(numbersKey, 2, name));
    assertEquals("two", n3.server.head().query(numbersKey, 2, name));
    assertEquals("three", n1.server.head().query(numbersKey, 3, name));
    assertEquals("three", n2.server.head().query(numbersKey, 3, name));
    assertEquals("three", n3.server.head().query(numbersKey, 3, name));
  }
  
  @Test
  public void testThreeNodeRestart() {
    testThreeNodeRestart(EpidemicFactory);
    testThreeNodeRestart(TreeFactory);
  }

  @Test
  public void testReconnect() {
    NodeConfig config = new NodeConfig
      (new MyConflictResolver(),
       ForeignKeyResolvers.Delete,
       new NodeNetwork(),
       EpidemicFactory);

    Node n1 = new Node(config, 1);
    Node n2 = new Node(config, 2);

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

    flush(config.network);

    assertEquals("one", n1.server.head().query(numbersKey, 1, name));
    assertEquals("one", n2.server.head().query(numbersKey, 1, name));
    
    n1.server.updateView(Collections.<NodeID>emptySet());
    n2.server.updateView(Collections.<NodeID>emptySet());
    
    base = n1.server.head();
    builder = base.builder();
    builder.insert(Throw, numbers, 2, name, "two");
    n1.server.merge(base, builder.commit());

    if (! config.network.messages.isEmpty()) {
      fail("network traffic exists despite disconnection");
    }

    n1.server.updateView(set(n2.id));
    n2.server.updateView(set(n1.id));
    
    flush(config.network);

    // System.out.print("n1: "); n1.server.dump(System.out);
    // System.out.print("n2: "); n2.server.dump(System.out);

    assertEquals("one", n1.server.head().query(numbersKey, 1, name));
    assertEquals("one", n2.server.head().query(numbersKey, 1, name));
    assertEquals("two", n1.server.head().query(numbersKey, 2, name));
    assertEquals("two", n2.server.head().query(numbersKey, 2, name));
  }

  @Test
  public void testDeleteAndInsert() {
    NodeConfig config = new NodeConfig
      (new MyConflictResolver(),
       ForeignKeyResolvers.Delete,
       new NodeNetwork(),
       EpidemicFactory);

    Node n1 = new Node(config, 1);
    Node n2 = new Node(config, 2);

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

    assertEquals("foo", n1.server.head().query(name, key, 1, 2, 1));
    assertNull(n2.server.head().query(name, key, 1, 2, 1));

    flush(config.network);

    assertEquals("foo", n1.server.head().query(name, key, 1, 2, 1));
    assertEquals("foo", n2.server.head().query(name, key, 1, 2, 1));

    base = n2.server.head();
    n2.server.merge
      (base, base.builder().table(table).delete(1, 2, 1).commit());

    base = n1.server.head();

    n1.server.merge
      (base, base.builder().table(table).row(1, 2, 2).update(name, "bar")
       .commit());

    assertEquals("foo", n1.server.head().query(name, key, 1, 2, 1));
    assertNull(n2.server.head().query(name, key, 1, 2, 1));
    assertEquals("bar", n1.server.head().query(name, key, 1, 2, 2));
    assertNull(n2.server.head().query(name, key, 1, 2, 2));

    flush(config.network);

    assertNull(n1.server.head().query(name, key, 1, 2, 1));
    assertNull(n2.server.head().query(name, key, 1, 2, 1));
    assertEquals("bar", n1.server.head().query(name, key, 1, 2, 2));
    assertEquals("bar", n2.server.head().query(name, key, 1, 2, 2));
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

  private static class NodeConfig {
    public final NodeConflictResolver conflictResolver;
    public final ForeignKeyResolver foreignKeyResolver;
    public final NodeNetwork network;
    public final ServerFactory factory;

    public NodeConfig(NodeConflictResolver conflictResolver,
                      ForeignKeyResolver foreignKeyResolver,
                      NodeNetwork network,
                      ServerFactory factory)
    {
      this.conflictResolver = conflictResolver;
      this.foreignKeyResolver = foreignKeyResolver;
      this.network = network;
      this.factory = factory;
    }

    public NetworkServer make(NodeID id) {
      return factory.make(conflictResolver, foreignKeyResolver, network, id);
    }
  }

  private static class Node {
    public final NodeID id;
    private final NodeConfig config;
    public NetworkServer server;

    public Node(NodeConfig config,
                int id)
    {
      this.id = new NodeID(String.valueOf(id));
      this.config = config;

      config.network.nodes.put(this.id, this);
      start();
    }

    public void start() {
      this.server = config.make(id);
    }
  }

  private interface ServerFactory {
    public NetworkServer make(NodeConflictResolver conflictResolver,
                              ForeignKeyResolver foreignKeyResolver,
                              NodeNetwork network,
                              NodeID id);
  }

  private static final ServerFactory EpidemicFactory = new ServerFactory() {
      public NetworkServer make(NodeConflictResolver conflictResolver,
                                ForeignKeyResolver foreignKeyResolver,
                                NodeNetwork network,
                                NodeID id)
      {
        return new EpidemicServer
          (conflictResolver, foreignKeyResolver, network, id);
      }
    };

  private static final ServerFactory TreeFactory = new ServerFactory() {
      public NetworkServer make(NodeConflictResolver conflictResolver,
                                ForeignKeyResolver foreignKeyResolver,
                                NodeNetwork network,
                                NodeID id)
      {
        return new TreeServer
          (conflictResolver, foreignKeyResolver, network, id);
      }
    };

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

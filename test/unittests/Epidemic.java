package unittests;

import org.junit.Test;

import static com.readytalk.oss.dbms.util.Util.cols;
import static com.readytalk.oss.dbms.util.Util.set;
import static com.readytalk.oss.dbms.DuplicateKeyResolution.Throw;
import static com.readytalk.oss.dbms.DuplicateKeyResolution.Overwrite;

import com.readytalk.oss.dbms.Table;
import com.readytalk.oss.dbms.Index;
import com.readytalk.oss.dbms.Column;
import com.readytalk.oss.dbms.Revision;
import com.readytalk.oss.dbms.RevisionBuilder;
import com.readytalk.oss.dbms.ForeignKeyResolver;
import com.readytalk.oss.dbms.ForeignKeyResolvers;
import com.readytalk.oss.dbms.util.BufferOutputStream;
import com.readytalk.oss.dbms.server.EpidemicServer;
import com.readytalk.oss.dbms.server.EpidemicServer.NodeID;
import com.readytalk.oss.dbms.server.EpidemicServer.Network;
import com.readytalk.oss.dbms.server.EpidemicServer.NodeConflictResolver;
import com.readytalk.oss.dbms.server.protocol.ReadContext;
import com.readytalk.oss.dbms.server.protocol.Writable;
import com.readytalk.oss.dbms.server.protocol.Readable;
import com.readytalk.oss.dbms.server.protocol.WriteContext;

import java.io.IOException;
import java.io.ByteArrayInputStream;
import java.util.Arrays;
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

  /*@Test
  public void testTwoNodeNetwork() {
    NodeNetwork network = new NodeNetwork();
    NodeConflictResolver conflictResolver = new MyConflictResolver();
    ForeignKeyResolver foreignKeyResolver = ForeignKeyResolvers.Delete;

    Node n1 = new Node(conflictResolver, foreignKeyResolver, network, 1);
    Node n2 = new Node(conflictResolver, foreignKeyResolver, network, 2);
    Node observer = new Node(conflictResolver, foreignKeyResolver, network, 3);

    n1.server.updateView(set(n2.id));
    n2.server.updateView(set(n1.id));

    Column number = new Column(Integer.class);
    Column name = new Column(String.class);
    Table numbers = new Table(list(number));

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
  }*/
  
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

  private static void flush(NodeNetwork network, NodeID... dontDeliverTo) {
    final int MaxIterations = 100;
    final Set<NodeID> ddt = new HashSet<NodeID>(Arrays.asList(dontDeliverTo));
    int iteration = 0;
    System.out.println("-------flush-------");
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
    System.out.println("-------done-------");
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
    public final EpidemicServer server;

    public Node(NodeConflictResolver conflictResolver,
                ForeignKeyResolver foreignKeyResolver,
                NodeNetwork network,
                int id)
    {
      this.id = new NodeID(String.valueOf(id));
      this.server = new EpidemicServer
        (conflictResolver, foreignKeyResolver, network, this.id);
      network.nodes.put(this.id, this);
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

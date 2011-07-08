package unittests;

import org.junit.Test;
import static org.junit.Assert.assertEquals;

import static com.readytalk.oss.dbms.util.Util.list;
import static com.readytalk.oss.dbms.util.Util.set;
import static com.readytalk.oss.dbms.DuplicateKeyResolution.Throw;

import com.readytalk.oss.dbms.DBMS;
import com.readytalk.oss.dbms.Table;
import com.readytalk.oss.dbms.Index;
import com.readytalk.oss.dbms.Column;
import com.readytalk.oss.dbms.Revision;
import com.readytalk.oss.dbms.RevisionBuilder;
import com.readytalk.oss.dbms.ForeignKey;
import com.readytalk.oss.dbms.ForeignKeyResolver;
import com.readytalk.oss.dbms.ForeignKeyResolvers;
import com.readytalk.oss.dbms.imp.MyDBMS;
import com.readytalk.oss.dbms.util.BufferOutputStream;
import com.readytalk.oss.dbms.server.EpidemicServer;
import com.readytalk.oss.dbms.server.EpidemicServer.Writable;
import com.readytalk.oss.dbms.server.EpidemicServer.Readable;
import com.readytalk.oss.dbms.server.EpidemicServer.NodeID;
import com.readytalk.oss.dbms.server.EpidemicServer.Network;
import com.readytalk.oss.dbms.server.EpidemicServer.NodeConflictResolver;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.io.IOException;
import java.io.ByteArrayInputStream;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;

import junit.framework.TestCase;

public class Epidemic extends TestCase{
  private static void expectEqual(Object actual, Object expected) {
    assertEquals(expected, actual);
  }

  @Test
  public void testTwoNodeNetwork() {
    DBMS dbms = new MyDBMS();
    NodeNetwork network = new NodeNetwork();
    NodeConflictResolver conflictResolver = new MyConflictResolver();
    ForeignKeyResolver foreignKeyResolver = ForeignKeyResolvers.Delete;

    Node n1 = new Node(dbms, conflictResolver, foreignKeyResolver, network, 1);
    Node n2 = new Node(dbms, conflictResolver, foreignKeyResolver, network, 2);

    n1.server.updateView(set(n2.id));
    n2.server.updateView(set(n1.id));

    Column number = new Column(Integer.class);
    Column name = new Column(String.class);
    Table numbers = new Table(list(number));

    Revision base = n1.server.head();
    RevisionBuilder builder = dbms.builder(base);
    
    builder.insert(Throw, numbers, 1, name, "one");

    n1.server.merge(base, builder.commit());

    Index numbersKey = numbers.primaryKey;

    expectEqual(n1.server.head().query(numbersKey, 1, name), "one");

    base = n2.server.head();
    builder = dbms.builder(base);
    
    builder.insert(Throw, numbers, 1, name, "uno");

    n2.server.merge(base, builder.commit());

    expectEqual(n2.server.head().query(numbersKey, 1, name), "uno");

    flush(network);

    expectEqual(n1.server.head().query(numbersKey, 1, name), "one");
    expectEqual(n2.server.head().query(numbersKey, 1, name), "one");
  }

  private static void flush(NodeNetwork network) {
    final int MaxIterations = 100;
    int iteration = 0;
    while (network.messages.size() > 0) {
      if (iteration++ > MaxIterations) {
        throw new RuntimeException("exceeded maximum iteration count");
      }

      List<Message> messages = new ArrayList(network.messages);
      network.messages.clear();

      for (Message m: messages) {
        try {
          Node source = network.nodes.get(m.source);
          Node destination = network.nodes.get(m.destination);

          BufferOutputStream buffer = new BufferOutputStream();
          m.body.writeTo(source.server, buffer);

          Readable result = (Readable) m.body.getClass().newInstance();
          result.readFrom
            (destination.server,
             new ByteArrayInputStream(buffer.getBuffer(), 0, buffer.size()));

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

  private static class Node {
    public final NodeID id;
    public final EpidemicServer server;

    public Node(DBMS dbms,
                NodeConflictResolver conflictResolver,
                ForeignKeyResolver foreignKeyResolver,
                NodeNetwork network,
                int id)
    {
      this.id = new NodeID(String.valueOf(id));
      this.server = new EpidemicServer
        (dbms, conflictResolver, foreignKeyResolver, network, this.id);
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
    public final Map<NodeID, Node> nodes = new HashMap();
    public final List<Message> messages = new ArrayList();

    public void send(NodeID source, NodeID destination, Writable message) {
      messages.add(new Message(source, destination, message));
    }
  }
}

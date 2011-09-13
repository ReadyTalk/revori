package com.readytalk.oss.dbms.server;

import com.readytalk.oss.dbms.Table;
import com.readytalk.oss.dbms.Column;
import com.readytalk.oss.dbms.RevisionBuilder;
import com.readytalk.oss.dbms.Revision;
import com.readytalk.oss.dbms.Revisions;
import com.readytalk.oss.dbms.ConflictResolver;
import com.readytalk.oss.dbms.DiffResult;
import com.readytalk.oss.dbms.DuplicateKeyResolution;
import com.readytalk.oss.dbms.ForeignKeyResolver;
import com.readytalk.oss.dbms.server.protocol.Protocol;
import com.readytalk.oss.dbms.server.protocol.Readable;
import com.readytalk.oss.dbms.server.protocol.ReadContext;
import com.readytalk.oss.dbms.server.protocol.Writable;
import com.readytalk.oss.dbms.server.protocol.WriteContext;
import com.readytalk.oss.dbms.server.protocol.Stringable;
import com.readytalk.oss.dbms.util.BufferOutputStream;

import java.lang.ref.WeakReference;
import java.io.InputStream;
import java.io.IOException;
import java.io.EOFException;
import java.io.ByteArrayInputStream;
import java.util.Map;
import java.util.HashMap;
import java.util.Set;
import java.util.HashSet;
import java.util.List;
import java.util.ArrayList;
import java.util.Iterator;

public class EpidemicServer implements RevisionServer {
  private static final boolean Debug = false;

  private static final int End = 0;
  private static final int Descend = 1;
  private static final int Ascend = 2;
  private static final int Key = 3;
  private static final int Delete = 4;
  private static final int Insert = 5;

  private String id;
  private final Set<Runnable> listeners = new HashSet<Runnable>();
  private final NodeConflictResolver conflictResolver;
  private final ForeignKeyResolver foreignKeyResolver;
  private final Network network;
  private final Object lock = new Object();
  private final Map<NodeID, NodeState> states = new HashMap<NodeID, NodeState>();
  private final Map<NodeID, NodeState> directlyConnectedStates = new HashMap<NodeID, NodeState>();
  private final NodeState localNode;
  private long nextLocalSequenceNumber = 1;

  public EpidemicServer(NodeConflictResolver conflictResolver,
                        ForeignKeyResolver foreignKeyResolver,
                        Network network,
                        NodeID self)
  {
    this.conflictResolver = conflictResolver;
    this.foreignKeyResolver = foreignKeyResolver;
    this.network = network;
    this.localNode = state(self);
  }

  private void debugMessage(String message) {
    if(Debug) {
      System.out.println(id + ": " + (localNode != null ? localNode.id : "(null)") + ": " + message);
    }
  }

  public synchronized void registerListener(Runnable listener) {
    listeners.add(listener);
    listener.run();
  }

  public synchronized void unregisterListener(Runnable listener) {
    listeners.remove(listener);
  }

  public void setId(String id) {
    this.id = id;
  }

  public void updateView(Set<NodeID> directlyConnectedNodes) {
    debugMessage("update view to " + directlyConnectedNodes);

    synchronized (lock) {
      for (Iterator<NodeState> it
             = directlyConnectedStates.values().iterator();
           it.hasNext();)
      {
        NodeState state = it.next();
        if (! directlyConnectedNodes.contains(state.id)) {
          debugMessage("remove directly connected state " + state);
          it.remove();
          state.connectionState = null;
        }
      }

      for (NodeID node: directlyConnectedNodes) {
        NodeState state = state(node);
        if (state.connectionState == null) {
          state.connectionState = new ConnectionState();
          state.connectionState.readyToReceive = true;

          debugMessage("add directly connected state " + node);
          directlyConnectedStates.put(node, state);

          sendNext(state);
        }
      }
    }
  }

  public Revision head() {
    return localNode.head.revision;
  }

  public void merge(Revision base,
                    Revision fork)
  {
    synchronized (lock) {
      Revision head = base.merge
        (localNode.head.revision, fork, conflictResolver(),
         foreignKeyResolver);

      if (head != localNode.head.revision) {
        debugMessage("merging");
        acceptRevision(localNode, nextLocalSequenceNumber++, head);
      }
    }
  }

  public ConflictResolver conflictResolver() {
    return new MyConflictResolver
      (localNode.id, localNode.id, conflictResolver);
  }

  public ForeignKeyResolver foreignKeyResolver() {
    return foreignKeyResolver;
  }

  public void accept(NodeID source, Readable message) {
    synchronized (lock) {
      // debugMessage("accept " + message + " from " + source);
      ((Message) message).deliver(source, this);
    }
  }

  private static void expect(boolean v) {
    if (Debug && ! v) {
      throw new RuntimeException();
    }
  }

  private void send(NodeState state, Writable message) {
    expect(state.connectionState != null);
    expect(state.connectionState.readyToReceive);

    // debugMessage("send " + message + " to " + state.id);

    network.send(localNode.id, state.id, message);
  }

  private NodeState state(NodeID node) {
    NodeState state = states.get(node);
    if (state == null) {
      states.put(node, state = new NodeState(node));

      state.head = new Record(node, Revisions.Empty, 0, null);

      for (NodeState s: states.values()) {
        debugMessage(s.id + " sees " + node + " at 0");
        s.acknowledged.put(state.id, state.head);

        // todo: switch from weak references to reference counting to
        // ensure that we don't follow the chain of records back
        // further than we need to.
        Record tail = s.head;
        Record previous;
        while (tail.previous != null
               && (previous = tail.previous.get()) != null)
        {
          tail = previous;
        }
        
        if (tail.merged != null && tail.merged.node == s.id) {
          tail = tail.merged;
        }
        
        Record rec;
        if (tail.sequenceNumber == 0) {
          rec = tail;
        } else {
          rec = new Record(s.id, Revisions.Empty, 0, null);
          rec.next = tail;
        }

        state.acknowledged.put(s.id, rec);
      }
    }
    return state;
  }

  private boolean readyForDataFromNewNode() {
    for (NodeState s: directlyConnectedStates.values()) {
      if (s.connectionState.sentHello && ! s.connectionState.gotSync) {
        return false;
      }
    }
    return true;
  }

  private void sendNext() {
    for (NodeState state: directlyConnectedStates.values()) {
      sendNext(state);
    }
  }

  private void sendNext(NodeState state) {
    ConnectionState cs = state.connectionState;

    if (! cs.readyToReceive) {
      return;
    }

    if (! cs.sentHello && readyForDataFromNewNode()) {
      state.connectionState.sentHello = true;

      debugMessage("hello to " + state.id);
      send(state, Hello.Instance);
      return;
    }

    if (! cs.gotHello) {
      return;
    }

    for (NodeState other: states.values()) {
      // debugMessage("sendNext: other: " + other.id + ", state: " + state.id+ ", nu: " + needsUpdate(state, other.head));
      if (other != state && needsUpdate(state, other.head)) {
        cs.sentSync = false;
        sendUpdate(state, other.head);
        return;
      }
    }
      
    if (! cs.sentSync) {
      cs.sentSync = true;
      debugMessage("sync to " + state.id);
      send(state, Sync.Instance);
      return;
    }
  }

  private boolean needsUpdate(NodeState state, Record target) {
    Record acknowledged = state.acknowledged.get(target.node);
    // debugMessage("needsUpdate(psn: " + acknowledge.sequenceNumber + ", tsn: " + target.sequenceNumber + ")");
    if (acknowledged.sequenceNumber < target.sequenceNumber) {
      Record lastSent = state.connectionState.lastSent.get(target.node);
      if (lastSent == null) {
        state.connectionState.lastSent.put(target.node, acknowledged);
        return true;
      } else if (lastSent.sequenceNumber < acknowledged.sequenceNumber) {
        lastSent = acknowledged;
        state.connectionState.lastSent.put(target.node, lastSent);
      }

      return lastSent.sequenceNumber < target.sequenceNumber;
    }

    return false;
  }

  private void sendUpdate(NodeState state,
                          Record target)
  {
    while (true) {
      Record lastSent = state.connectionState.lastSent.get(target.node);
      Record record = lastSent.next;

      debugMessage("ls: " + lastSent.hashCode() + ", rec: " + record.hashCode());
            
      if (record.merged != null) {
        if (needsUpdate(state, record.merged)) {
          target = record.merged;
          continue;
        }

        debugMessage("ack to " + state.id + ": " + record.node + " "
                     + record.sequenceNumber + " merged "
                     + record.merged.node + " "
                     + record.merged.sequenceNumber);

        send(state, new Ack
             (record.node, record.sequenceNumber, record.merged.node,
              record.merged.sequenceNumber));
      } else {
        debugMessage("diff to " + state.id + ": " + target.node + " " + lastSent.sequenceNumber + " " + record.sequenceNumber);
        send(state, new Diff
             (target.node, lastSent.sequenceNumber, record.sequenceNumber,
              new RevisionDiffBody(lastSent.revision, record.revision)));
      }

      state.connectionState.lastSent.put(target.node, record);
      break;
    }
  }

  private void acceptSync(NodeID origin) {
    debugMessage("sync from " + origin);
    NodeState state = state(origin);
    
    if (! state.connectionState.gotSync) {
      state.connectionState.gotSync = true;

      sendNext();
    }
  }

  private void acceptHello(NodeID origin) {
    debugMessage("hello from " + origin);
    NodeState state = state(origin);
    
    state.connectionState.gotHello = true;
    sendNext(state);
  }

  private void acceptDiff(NodeID origin,
                          long startSequenceNumber,
                          long endSequenceNumber,
                          DiffBody body)
  {
    debugMessage("accept diff " + origin + " " + startSequenceNumber + " " + endSequenceNumber);
    NodeState state = state(origin);

    if (startSequenceNumber <= state.head.sequenceNumber) {
      Record record = state.head;
      while (record != null
             && endSequenceNumber != record.sequenceNumber
             && startSequenceNumber < record.sequenceNumber)
      {
        record = record.previous.get();
      }

      if (record != null) {
        if (endSequenceNumber == record.sequenceNumber) {
          // do nothing -- we already have this revision
          debugMessage("ignore diff " + origin + " " + startSequenceNumber + " " + endSequenceNumber);
        } else if (startSequenceNumber == record.sequenceNumber) {
          acceptRevision
            (state, endSequenceNumber, body.apply(this, record.revision));
        } else {
          throw new RuntimeException("missed a diff");
        }
      } else {
        throw new RuntimeException("obsolete diff");
      }
    } else {
      throw new RuntimeException("missed a diff");
    }
  }

  private void acceptRevision(NodeState state,
                              long sequenceNumber,
                              Revision revision)
  {
    insertRevision(state, sequenceNumber, revision, null);

    acceptAck
      (localNode.id, nextLocalSequenceNumber++, state.id, sequenceNumber);
  }

  private void insertRevision(NodeState state,
                              long sequenceNumber,
                              Revision revision,
                              Record merged)
  {
    Record record = state.head;
    debugMessage("insertRevision: record: " + record.hashCode());
    while (sequenceNumber < record.sequenceNumber) {
      record = record.previous.get();
    }

    if (sequenceNumber == record.sequenceNumber) {
      throw new RuntimeException("redundant revision");
    }

    Record next = record.next;
    Record newRecord = new Record
      (state.id, revision, sequenceNumber, merged);
    record.next = newRecord;
    newRecord.previous = new WeakReference<Record>(record);
    if (next != null) {
      newRecord.next = next;
      next.previous = new WeakReference<Record>(newRecord);
    } else {
      state.head = newRecord;

      if (state == localNode) {
        debugMessage("notify listeners");
        // tell everyone we have updates!
        // TODO: don't notify people twice for each update.
        for (Runnable listener: listeners) {
          listener.run();
        }
      }
    }

    debugMessage("insertRevision state.id: " + state.id + ", sequenceNo: " + sequenceNumber + ", record: " + newRecord.hashCode());
  }

  private Revision merge(Record base, Record head, Record fork,
                         NodeID headNode, NodeID forkNode)
  {
    MyConflictResolver resolver = new MyConflictResolver
      (headNode, forkNode, conflictResolver);

    Revision result = head.revision;
    Record record = base.next;
    while (base != fork) {
      if (record.merged == null) {
        result = base.revision.merge
          (result, record.revision, resolver, foreignKeyResolver);
      }

      base = record;
      record = record.next;
    }

    return result;
  }

  private void acceptAck(NodeID acknowledger,
                         long acknowledgerSequenceNumber,
                         NodeID diffOrigin,
                         long diffSequenceNumber)
  {
    debugMessage("accept ack " + acknowledger + " " + acknowledgerSequenceNumber + " diff " + diffOrigin + " " + diffSequenceNumber);
    NodeState state = state(acknowledger);

    Record record = state.acknowledged.get(diffOrigin);

    if (record.sequenceNumber < diffSequenceNumber) {
      Record base = record;
      while (record != null
             && record.sequenceNumber < diffSequenceNumber)
      {
        record = record.next;
      }

      if (record != null && record.sequenceNumber == diffSequenceNumber) {
        if (record.merged == null) {
          insertRevision
            (state, acknowledgerSequenceNumber, merge
             (base, state.head, record, acknowledger, diffOrigin), record);
        }

        state.acknowledged.put(diffOrigin, record);
            
        sendNext();

        if (record.merged == null) {
          acceptAck
            (localNode.id, nextLocalSequenceNumber++, acknowledger,
             acknowledgerSequenceNumber);
        }
      } else {
        throw new RuntimeException("missed a diff");
      }
    } else {
      // obsolete ack -- ignore
    }
  }

  private static class NodeState {
    public final NodeID id;
    public Record head;
    public final Map<NodeID, Record> acknowledged
      = new HashMap<NodeID, Record>();
    public ConnectionState connectionState;

    public NodeState(NodeID id) {
      this.id = id;
    }
  }

  private static class ConnectionState {
    public final Map<NodeID, Record> lastSent = new HashMap<NodeID, Record>();
    public boolean readyToReceive;
    public boolean sentHello;
    public boolean gotHello;
    public boolean sentSync;
    public boolean gotSync;
  }

  private static class Record {
    public final NodeID node;
    public final Revision revision;
    public final long sequenceNumber;
    public final Record merged;
    public WeakReference<Record> previous;
    public Record next;

    public Record(NodeID node,
                  Revision revision,
                  long sequenceNumber,
                  Record merged)
    {
      this.node = node;
      this.revision = revision;
      this.sequenceNumber = sequenceNumber;
      this.merged = merged;
    }
  }

  private interface Message extends Writable, Readable {
    public void deliver(NodeID source, EpidemicServer server);
  }

  // public for deserialization
  public static class Ack implements Message {
    private NodeID acknowledger;
    private long acknowledgerSequenceNumber;
    private NodeID diffOrigin;
    private long diffSequenceNumber;

    private Ack(NodeID acknowledger,
               long acknowledgerSequenceNumber,
               NodeID diffOrigin,
               long diffSequenceNumber)
    {
      this.acknowledger = acknowledger;
      this.acknowledgerSequenceNumber = acknowledgerSequenceNumber;
      this.diffOrigin = diffOrigin;
      this.diffSequenceNumber = diffSequenceNumber;
    }

    // for deserialization
    public Ack() { }

    public void writeTo(WriteContext context)
      throws IOException
    {
      StreamUtil.writeString(context.out, acknowledger.id);
      StreamUtil.writeLong(context.out, acknowledgerSequenceNumber);
      StreamUtil.writeString(context.out, diffOrigin.id);
      StreamUtil.writeLong(context.out, diffSequenceNumber);
    }

    public void readFrom(ReadContext context)
      throws IOException
    {
      acknowledger = new NodeID(StreamUtil.readString(context.in));
      acknowledgerSequenceNumber = StreamUtil.readLong(context.in);
      diffOrigin = new NodeID(StreamUtil.readString(context.in));
      diffSequenceNumber = StreamUtil.readLong(context.in);
    }

    public void deliver(NodeID source, EpidemicServer server) {
      server.debugMessage("ack from " + source);
      server.acceptAck(acknowledger, acknowledgerSequenceNumber, diffOrigin,
                       diffSequenceNumber);
    }
  }

  // public for deserialization
  public static class Diff implements Message {
    private NodeID origin;
    private long startSequenceNumber;
    private long endSequenceNumber;
    private DiffBody body;

    private Diff(NodeID origin,
                 long startSequenceNumber,
                 long endSequenceNumber,
                 DiffBody body)
    {
      this.origin = origin;
      this.startSequenceNumber = startSequenceNumber;
      this.endSequenceNumber = endSequenceNumber;
      this.body = body;
    }

    // for deserialization
    public Diff() { }

    public void writeTo(WriteContext context)
      throws IOException
    {
      StreamUtil.writeString(context.out, origin.id);
      StreamUtil.writeLong(context.out, startSequenceNumber);
      StreamUtil.writeLong(context.out, endSequenceNumber);
      ((Writable) body).writeTo(context);
    }

    public void readFrom(ReadContext context)
      throws IOException
    {
      origin = new NodeID(StreamUtil.readString(context.in));
      startSequenceNumber = StreamUtil.readLong(context.in);
      endSequenceNumber = StreamUtil.readLong(context.in);
      BufferDiffBody list = new BufferDiffBody();
      list.readFrom(context);
      body = list;
    }

    public void deliver(NodeID source, EpidemicServer server) {
      server.debugMessage("diff from " + source);
      server.acceptDiff(origin, startSequenceNumber, endSequenceNumber, body);
    }

    public String toString() {
      return "diff[" + body + "]";
    }
  }

  private static abstract class Singleton implements Message {
    public void writeTo(WriteContext context) {
      // ignore
    }

    public void readFrom(ReadContext context) {
      // ignore
    }
  }

  // public for deserialization
  public static class Hello extends Singleton {
    private static final Hello Instance = new Hello();

    public void deliver(NodeID source, EpidemicServer server) {
      server.acceptHello(source);
    }
  }

  // public for deserialization
  public static class Sync extends Singleton {
    private static final Sync Instance = new Sync();

    public void deliver(NodeID source, EpidemicServer server) {
      server.acceptSync(source);
    }
  }

  private static interface DiffBody {
    public Revision apply(EpidemicServer server, Revision base);
  }

  private static class RevisionDiffBody implements DiffBody, Writable {
    public final Revision base;
    public final Revision fork;

    public RevisionDiffBody(Revision base, Revision fork) {
      this.base = base;
      this.fork = fork;
    }

    public Revision apply(EpidemicServer server, Revision base) {
      return fork;
    }

    public void writeTo(WriteContext context)
      throws IOException
    {
      DiffResult result = base.diff(fork, true);
      while (true) {
        DiffResult.Type type = result.next();
        switch (type) {
        case End:
          context.out.write(End);
          return;

        case Descend: {
          context.out.write(Descend);
        } break;

        case Ascend: {
          context.out.write(Ascend);
        } break;

        case Key: {
          Object forkKey = result.fork();
          if (forkKey != null) {
            context.out.write(Key);
            Protocol.write(context, forkKey);
          } else {
            context.out.write(Delete);
            Protocol.write(context, result.base());
            result.skip();
          }
        } break;

        case Value: {
          context.out.write(Insert);
          Protocol.write(context, result.fork());
        } break;

        default:
          throw new RuntimeException("unexpected result type: " + type);
        }
      }
    }

    public String toString() {
      // todo: reduce code duplication between this and the writeTo method

      StringBuilder sb = new StringBuilder();
      // sb.append(" *** from\n").append(base).append(" *** to\n").append(fork)
      //   .append("\n");

      final int MaxDepth = 16;
      Object[] path = new Object[MaxDepth];
      int depth = 0;
      DiffResult result = base.diff(fork, true);
      while (true) {
        DiffResult.Type type = result.next();
        switch (type) {
        case End:
          sb.append("end\n");
          return sb.toString();

        case Descend: {
          sb.append("descend\n");
          ++ depth;
        } break;

        case Ascend: {
          sb.append("ascend\n");
          path[depth--] = null;
        } break;

        case Key: {
          Object forkKey = result.fork();
          if (forkKey != null) {
            sb.append("key ").append(forkKey).append("\n");
            path[depth] = forkKey;
          } else {
            path[depth] = result.base();
            sb.append("delete");
            sb.append(EpidemicServer.toString(path, 0, depth + 1));
            sb.append("\n");
            result.skip();
          }
        } break;

        case Value: {
          path[depth] = result.fork();
          sb.append("insert");
          sb.append(EpidemicServer.toString(path, 0, depth + 1));
          sb.append("\n");
        } break;

        default:
          throw new RuntimeException("unexpected result type: " + type);
        }
      }
    }
  }

  private static String toString(Object[] array, int offset, int length) {
    StringBuilder sb = new StringBuilder();
    sb.append("[");
    for (int i = offset; i < offset + length; ++i) {
      sb.append(array[i]);
      if (i < offset + length - 1) {
        sb.append(" ");
      }
    }
    return sb.append("]").toString();
  }

  private static class BufferDiffBody implements DiffBody, Readable {
    public BufferOutputStream buffer;

    public Revision apply(EpidemicServer server, Revision base) {
      RevisionBuilder builder = base.builder();
      final int MaxDepth = 16;
      Object[] path = new Object[MaxDepth];
      int depth = 0;
      InputStream in = new ByteArrayInputStream
        (buffer.getBuffer(), 0, buffer.size());
      ReadContext readContext = new ReadContext(in);
      boolean visitedColumn = true;

      try {
        while (true) {
          int flag = in.read();
          switch (flag) {
          case End:
            return builder.commit();

          case Descend:
            visitedColumn = true;
            ++ depth;
            break;

          case Ascend:
            if (! visitedColumn) {
              visitedColumn = true;
              builder.insert(DuplicateKeyResolution.Overwrite,
                             path, 0, depth + 1);
            }

            path[depth--] = null;
            break;

          case Key:
            if (! visitedColumn) {
              builder.insert(DuplicateKeyResolution.Overwrite,
                             path, 0, depth + 1);
            } else {
              visitedColumn = false;
            }

            path[depth] = Protocol.read(readContext);
            break;

          case Delete:
            visitedColumn = true;
            path[depth] = Protocol.read(readContext);
            builder.delete(path, 0, depth + 1);
            break;

          case Insert:
            visitedColumn = true;
            path[depth + 1] = Protocol.read(readContext);
            builder.insert(DuplicateKeyResolution.Overwrite,
                           path, 0, depth + 2);
            break;

          default:
            throw new RuntimeException("unexpected flag: " + flag);
          }
        }
      } catch (IOException e) {
        // shouldn't be possible, since we're reading from a byte array
        throw new RuntimeException(e);
      }
    }

    public void readFrom(ReadContext context)
      throws IOException
    {
      buffer = new BufferOutputStream();
      WriteContext writeContext = new WriteContext(buffer);
      while (true) {
        int flag = context.in.read();
        switch (flag) {
        case -1:
          throw new EOFException();

        case End:
          buffer.write(flag);
          return;

        case Descend:          
        case Ascend:
          buffer.write(flag);
          break;

        case Key:
        case Delete:
        case Insert:
          buffer.write(flag);
          Protocol.write(writeContext, Protocol.read(context));
          break;

        default:
          throw new RuntimeException("unexpected flag: " + flag);
        }
      }
    }

    public String toString() {
      // todo: reduce code duplication between this and the apply method
      StringBuilder sb = new StringBuilder();
      final int MaxDepth = 16;
      Object[] path = new Object[MaxDepth];
      int depth = 0;
      InputStream in = new ByteArrayInputStream
        (buffer.getBuffer(), 0, buffer.size());
      ReadContext readContext = new ReadContext(in);
      boolean visitedColumn = true;

      try {
        while (true) {
          int flag = in.read();
          switch (flag) {
          case End:
            return sb.toString();

          case Descend:
            visitedColumn = true;
            ++ depth;
            break;

          case Ascend:
            if (! visitedColumn) {
              visitedColumn = true;
              sb.append("insert");
              sb.append(EpidemicServer.toString(path, 0, depth + 1));
              sb.append("\n");
            }

            path[depth--] = null;
            break;

          case Key:
            if (! visitedColumn) {
              sb.append("insert");
              sb.append(EpidemicServer.toString(path, 0, depth + 1));
              sb.append("\n");
            } else {
              visitedColumn = false;
            }

            path[depth] = Protocol.read(readContext);
            break;

          case Delete:
            visitedColumn = true;
            path[depth] = Protocol.read(readContext);
            sb.append("delete");
            sb.append(EpidemicServer.toString(path, 0, depth + 1));
            sb.append("\n");
            break;

          case Insert:
            visitedColumn = true;
            path[depth + 1] = Protocol.read(readContext);
            sb.append("insert");
            sb.append(EpidemicServer.toString(path, 0, depth + 2));
            sb.append("\n");
            break;

          default:
            throw new RuntimeException("unexpected flag: " + flag);
          }
        }
      } catch (IOException e) {
        // shouldn't be possible, since we're reading from a byte array
        throw new RuntimeException(e);
      }
    }
  }

  public static interface Network {
    public void send(NodeID source, NodeID destination, Writable message);
  }

  public static class NodeID implements Comparable<NodeID>, Stringable {
    public final String id;

    public NodeID(String id) {
      this.id = id;
    }

    public int hashCode() {
      return id.hashCode();
    }

    public boolean equals(Object o) {
      return o instanceof NodeID && id.equals(((NodeID) o).id);
    }

    @Override
    public int compareTo(NodeID o) {
      return id.compareTo(o.id);
    }

    public String toString() {
      return "nodeID[" + id + "]";
    }

    public String asString() {
      return id;
    }
  }

  public static interface NodeConflictResolver {
    public Object resolveConflict(NodeID leftNode,
                                  NodeID rightNode,
                                  Table table,
                                  Column column,
                                  Object[] primaryKeyValues,
                                  Object baseValue,
                                  Object leftValue,
                                  Object rightValue);
  }

  private static class MyConflictResolver implements ConflictResolver {
    private final NodeID leftNode;
    private final NodeID rightNode;
    private final NodeConflictResolver resolver;

    public MyConflictResolver(NodeID leftNode,
                              NodeID rightNode,
                              NodeConflictResolver resolver)
    {
      this.leftNode = leftNode;
      this.rightNode = rightNode;
      this.resolver = resolver;
    }

    public Object resolveConflict(Table table,
                                  Column column,
                                  Object[] primaryKeyValues,
                                  Object baseValue,
                                  Object leftValue,
                                  Object rightValue)
    {
      return resolver.resolveConflict
        (leftNode, rightNode, table, column, primaryKeyValues, baseValue,
         leftValue, rightValue);
    }
  }
}

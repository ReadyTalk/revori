package com.readytalk.oss.dbms.server;

import com.readytalk.oss.dbms.DBMS;
import com.readytalk.oss.dbms.DBMS.PatchContext;
import com.readytalk.oss.dbms.DBMS.Revision;
import com.readytalk.oss.dbms.DBMS.ConflictResolver;
import com.readytalk.oss.dbms.DBMS.DiffResult;
import com.readytalk.oss.dbms.DBMS.DiffResultType;
import com.readytalk.oss.dbms.DBMS.DuplicateKeyResolution;

import java.lang.ref.WeakReference;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.IOException;
import java.io.EOFException;
import java.io.ByteArrayOutputStream;
import java.io.ByteArrayInputStream;
import java.util.Map;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.Set;
import java.util.List;
import java.util.ArrayList;
import java.util.Iterator;

public class EpidemicServer {
  private static final boolean Debug = true;

  private static final Map<Class, Serializer> Serializers = new HashMap();

  static {
    Serializers.put(Integer.class, new Serializer() {
      public void writeTo(OutputStream out, Object v) throws IOException {
        writeInteger(out, (Integer) v);
      }

      public Object readFrom(InputStream in) throws IOException {
        return readInteger(in);
      }
    });

    Serializers.put(String.class, new Serializer() {
      public void writeTo(OutputStream out, Object v) throws IOException {
        writeString(out, (String) v);
      }

      public Object readFrom(InputStream in) throws IOException {
        return readString(in);
      }
    });
  }

  private static final int End = 0;
  private static final int Descend = 1;
  private static final int Ascend = 2;
  private static final int Key = 3;
  private static final int Delete = 4;
  private static final int Insert = 5;
  private static final int ClassDefinition = 6;
  private static final int ClassReference = 7;
  private static final int Reference = 8;

  private final DBMS dbms;
  private final ConflictResolver conflictResolver;
  private final Network network;
  private final Object lock = new Object();
  private final Map<NodeID, NodeState> states = new HashMap();
  private final Map<NodeID, NodeState> directlyConnectedStates = new HashMap();
  private final NodeState localNode;
  private long nextLocalSequenceNumber = 1;

  public EpidemicServer(DBMS dbms,
                        ConflictResolver conflictResolver,
                        Network network,
                        NodeID self)
  {
    this.dbms = dbms;
    this.conflictResolver = conflictResolver;
    this.network = network;
    this.localNode = state(self);
  }

  public void updateView(Set<NodeID> directlyConnectedNodes) {
    synchronized (lock) {
      for (Iterator<NodeState> it
             = directlyConnectedStates.values().iterator();
           it.hasNext();)
      {
        NodeState state = it.next();
        if (! directlyConnectedNodes.contains(state.id)) {
          it.remove();
          state.connectionState = null;
        }
      }

      for (NodeID node: directlyConnectedNodes) {
        NodeState state = state(node);
        if (state.connectionState == null) {
          state.connectionState = new ConnectionState();
          state.connectionState.readyToReceive = true;

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
      acceptRevision
        (localNode,
         nextLocalSequenceNumber++,
         dbms.merge(base, localNode.head.revision, fork, conflictResolver));
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
    expect(state.connectionState.gotHello);

    network.send(state.id, message);
  }

  private NodeState state(NodeID node) {
    NodeState state = states.get(node);
    if (state == null) {
      states.put(node, state = new NodeState(node));

      state.head = new Record(node, dbms.revision(), 0, null);

      for (NodeState s: states.values()) {
        s.pending.put(state.id, state.head);
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

    if (! (cs.readyToReceive && cs.gotHello)) {
      return;
    }

    if (! cs.sentHello && readyForDataFromNewNode()) {
      state.connectionState.sentHello = true;
      send(state, Hello.Instance);
      return;
    }

    for (NodeState other: states.values()) {
      if (other != state && needsUpdate(state, other.head)) {
        cs.sentSync = false;
        sendUpdate(state, other.head);
        return;
      }
    }
      
    if (! cs.sentSync) {
      cs.sentSync = true;
      send(state, Sync.Instance);
      return;
    }
  }

  private boolean needsUpdate(NodeState state, Record target) {
    Record pending = state.pending.get(target.node);
    if (pending.sequenceNumber < target.sequenceNumber) {
      Record lastSent = state.connectionState.lastSent.get(target.node);

      if (lastSent.sequenceNumber < pending.sequenceNumber) {
        lastSent = pending;
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
            
      if (record.merged != null) {
        if (needsUpdate(state, record.merged)) {
          target = record.merged;
          continue;
        }

        send(state, new Ack
             (record.node, record.sequenceNumber, record.merged.node,
              record.merged.sequenceNumber));
      } else {
        send(state, new Diff
             (state.id, lastSent.sequenceNumber, record.sequenceNumber,
              new RevisionDiffBody(dbms, lastSent.revision, record.revision)));
      }

      state.connectionState.lastSent.put(target.node, record);
      break;
    }
  }

  private void acceptSync(NodeID origin) {
    NodeState state = state(origin);
    
    if (! state.connectionState.gotSync) {
      state.connectionState.gotSync = true;

      sendNext();
    }
  }

  private void readyToReceive(NodeID origin) {
    NodeState state = state(origin);
    
    state.connectionState.readyToReceive = true;
    sendNext(state);
  }

  private void acceptHello(NodeID origin) {
    NodeState state = state(origin);
    
    state.connectionState.gotHello = true;
    sendNext(state);
  }

  private void acceptDiff(NodeID origin,
                          long startSequenceNumber,
                          long endSequenceNumber,
                          DiffBody body)
  {
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
        } else if (startSequenceNumber == record.sequenceNumber) {
          acceptRevision
            (state, endSequenceNumber, body.apply(dbms, record.revision));
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
    newRecord.previous = new WeakReference(record);
    if (next != null) {
      newRecord.next = next;
      next.previous = new WeakReference(newRecord);
    }
  }

  private void acceptAck(NodeID acknowledger,
                         long acknowledgerSequenceNumber,
                         NodeID diffOrigin,
                         long diffSequenceNumber)
  {
    NodeState state = state(acknowledger);

    Record record = state.pending.get(diffOrigin);

    if (record.sequenceNumber < diffSequenceNumber) {
      Revision base = record.revision;
      while (record != null
             && record.sequenceNumber < diffSequenceNumber)
      {
        record = record.next;
      }

      if (record != null && record.sequenceNumber == diffSequenceNumber) {
        insertRevision
          (state, acknowledgerSequenceNumber, dbms.merge
           (base, state.head.revision, record.revision, conflictResolver),
           record);

        state.pending.put(diffOrigin, record);
            
        sendNext();
      } else {
        throw new RuntimeException("missed a diff");
      }
    } else {
      // obsolete ack -- ignore
    }
  }

  private static void write(OutputStream out,
                            WriteContext context,
                            Object value)
    throws IOException
  {
    Integer id = context.objectIDs.get(value);
    if (id == null) {
      Class c = value.getClass();
      Integer classID = context.classIDs.get(c);
      if (classID == null) {
        int newClassID = context.nextID++;

        out.write(ClassDefinition);
        writeInteger(out, newClassID);
        writeString(out, c.getName());

        context.objectIDs.put(c, newClassID);
      } else {
        out.write(ClassReference);
        writeInteger(out, classID);
      }

      int newID = context.nextID++;
      writeInteger(out, newID);
      writeObject(out, value);

      context.objectIDs.put(value, newID);
    } else {
      out.write(Reference);
      writeInteger(out, id);
    }
  }

  private static void writeInteger(OutputStream out, int v)
    throws IOException
  {
    if (v == (v & 0x7F)) {
      out.write(v);
    } else {
      out.write((v & 0x7F) | 0x80);
      writeInteger(out, v >>> 7);
    }
  }

  public static void writeString(OutputStream out, String s)
    throws IOException
  {
    byte[] bytes = s.getBytes("UTF-8");
    writeInteger(out, bytes.length);
    out.write(bytes);
  }

  private static void writeObject(OutputStream out, Object v)
    throws IOException
  {
    if (v instanceof Writable) {
      ((Writable) v).writeTo(out);
    } else {
      Serializers.get(v.getClass()).writeTo(out, v);
    }
  }

  private static int readInteger(InputStream in)
    throws IOException
  {
    int b = in.read();
    if (b < 0) {
      throw new EOFException();
    } else if ((b & 0x80) == 0) {
      return b;
    } else {
      return (b & 0x7F) | (readInteger(in) << 7);
    }
  }

  private static String readString(InputStream in)
    throws IOException
  {
    byte[] array = new byte[readInteger(in)];
    if (StreamUtil.readFully(in, array, 0, array.length) != array.length) {
      throw new EOFException();
    }
    return new String(array, "UTF-8");
  }

  private static Object readObject(Class c, InputStream in)
    throws IOException
  {
    if (c.isAssignableFrom(Readable.class)) {
      Readable v;
      try {
        v = (Readable) c.newInstance();
      } catch (InstantiationException e) {
        throw new RuntimeException(e);
      } catch (IllegalAccessException e) {
        throw new RuntimeException(e);
      }
      v.readFrom(in);
      return v;
    } else {
      return Serializers.get(c).readFrom(in);
    }
  }

  private static Object readDefinition(Class c,
                                       InputStream in,
                                       ReadContext context)
    throws IOException
  {
    int id = readInteger(in);
    Object value = readObject(c, in);
    context.objects.put(id, value);
    return value;
  }

  private static Object read(InputStream in,
                             ReadContext context)
    throws IOException
  {
    int flag = in.read();
    switch (flag) {
    case ClassDefinition: {
      int classID = readInteger(in);
      Class c;
      try {
        c = Class.forName(readString(in));
      } catch (ClassNotFoundException e) {
        throw new RuntimeException(e);
      }
      context.classes.put(classID, c);
      return readDefinition(c, in, context);
    }
      
    case ClassReference: {
      return readDefinition
        (context.classes.get(readInteger(in)), in, context);
    }
      
    case Reference: {
      return context.objects.get(readInteger(in));
    }
      
    default:
      throw new RuntimeException("unexpected flag: " + flag);
    }    
  }

  private static class NodeState {
    public final NodeID id;
    public Record head;
    public final Map<NodeID, Record> pending = new HashMap();
    public ConnectionState connectionState;

    public NodeState(NodeID id) {
      this.id = id;
    }
  }

  private static class ConnectionState {
    public final Map<NodeID, Record> lastSent = new HashMap();
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

  private static class Ack implements Writable, Readable {
    public NodeID acknowledger;
    public long acknowledgerSequenceNumber;
    public NodeID diffOrigin;
    public long diffSequenceNumber;

    public Ack(NodeID acknowledger,
               long acknowledgerSequenceNumber,
               NodeID diffOrigin,
               long diffSequenceNumber)
    {
      this.acknowledger = acknowledger;
      this.acknowledgerSequenceNumber = acknowledgerSequenceNumber;
      this.diffOrigin = diffOrigin;
      this.diffSequenceNumber = diffSequenceNumber;
    }

    public void writeTo(OutputStream out) throws IOException {
      StreamUtil.writeString(out, acknowledger.id);
      StreamUtil.writeLong(out, acknowledgerSequenceNumber);
      StreamUtil.writeString(out, diffOrigin.id);
      StreamUtil.writeLong(out, diffSequenceNumber);
    }

    public void readFrom(InputStream in) throws IOException {
      acknowledger = new NodeID(StreamUtil.readString(in));
      acknowledgerSequenceNumber = StreamUtil.readLong(in);
      diffOrigin = new NodeID(StreamUtil.readString(in));
      diffSequenceNumber = StreamUtil.readLong(in);
    }
  }

  private static class Diff implements Writable, Readable {
    public NodeID origin;
    public long startSequenceNumber;
    public long endSequenceNumber;
    public DiffBody body;

    public Diff(NodeID origin,
                long startSequenceNumber,
                long endSequenceNumber,
                DiffBody body)
    {
      this.origin = origin;
      this.startSequenceNumber = startSequenceNumber;
      this.endSequenceNumber = endSequenceNumber;
      this.body = body;
    }

    public void writeTo(OutputStream out) throws IOException {
      StreamUtil.writeString(out, origin.id);
      StreamUtil.writeLong(out, startSequenceNumber);
      StreamUtil.writeLong(out, endSequenceNumber);
      ((Writable) body).writeTo(out);
    }

    public void readFrom(InputStream in) throws IOException {
      origin = new NodeID(StreamUtil.readString(in));
      startSequenceNumber = StreamUtil.readLong(in);
      endSequenceNumber = StreamUtil.readLong(in);
      BufferDiffBody list = new BufferDiffBody();
      list.readFrom(in);
      body = list;
    }
  }

  private static class Singleton implements Writable, Readable {
    public void writeTo(OutputStream out) {
      // ignore
    }

    public void readFrom(InputStream in) throws IOException {
      // ignore
    }
  }

  private static class Hello extends Singleton {
    public static final Hello Instance = new Hello();
  }

  private static class Sync extends Singleton {
    public static final Sync Instance = new Sync();
  }

  private static interface DiffBody {
    public Revision apply(DBMS dbms, Revision base);
  }

  private static class RevisionDiffBody implements DiffBody, Writable {
    public final DBMS dbms;
    public final Revision base;
    public final Revision fork;

    public RevisionDiffBody(DBMS dbms, Revision base, Revision fork) {
      this.dbms = dbms;
      this.base = base;
      this.fork = fork;
    }

    public Revision apply(DBMS dbms, Revision base) {
      return fork;
    }

    public void writeTo(OutputStream out) throws IOException {
      DiffResult result = dbms.diff(base, fork);
      WriteContext writeContext = new WriteContext();
      while (true) {
        DiffResultType type = result.next();
        switch (type) {
        case End:
          out.write(End);
          return;

        case Descend: {
          out.write(Descend);
        } break;

        case Ascend: {
          out.write(Ascend);
        } break;

        case Key: {
          if (result.forkHasKey()) {
            out.write(Key);
            write(out, writeContext, result.get());
          } else {
            out.write(Delete);
            write(out, writeContext, result.get());
            result.skip();
          }
        } break;

        case Value: {
          out.write(Insert);
          write(out, writeContext, result.get());
        } break;

        default:
          throw new RuntimeException("unexpected result type: " + type);
        }
      }
    }
  }

  private static class BufferDiffBody implements DiffBody, Readable {
    public BufferOutputStream buffer;

    public Revision apply(DBMS dbms, Revision base) {
      PatchContext patchContext = dbms.patchContext(base);
      ReadContext readContext = new ReadContext();
      final int MaxDepth = 16;
      Object[] path = new Object[MaxDepth];
      int depth = 0;
      InputStream in = new ByteArrayInputStream
        (buffer.getBuffer(), 0, buffer.size());

      try {
        while (true) {
          int flag = in.read();
          switch (flag) {
          case End:
            return dbms.commit(patchContext);

          case Descend:
            ++ depth;
            break;

          case Ascend:
            -- depth;
            break;

          case Key:
            path[depth] = read(in, readContext);
            break;

          case Delete:
            path[depth] = read(in, readContext);
            dbms.treeDelete(patchContext, path);
            break;

          case Insert:
            path[depth] = read(in, readContext);
            dbms.treeInsert
              (patchContext, DuplicateKeyResolution.Overwrite, path);
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

    public void readFrom(InputStream in) throws IOException {
      buffer = new BufferOutputStream();
      ReadContext readContext = new ReadContext();
      WriteContext writeContext = new WriteContext();
      while (true) {
        int flag = in.read();
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
          write(buffer, writeContext, read(in, readContext));
          break;

        default:
          throw new RuntimeException("unexpected flag: " + flag);
        }
      }
    }
  }

  public static interface Writable {
    public void writeTo(OutputStream out) throws IOException;
  }

  public static interface Readable {
    public void readFrom(InputStream in) throws IOException;
  }

  public static interface Network {
    public void send(NodeID destination, Writable message);
  }

  public static class NodeID {
    public final String id;

    public NodeID(String id) {
      this.id = id;
    }
  }

  private static class BufferOutputStream extends ByteArrayOutputStream {
    public byte[] getBuffer() {
      return buf;
    }
  }

  private static class WriteContext {
    public final Map<Class, Integer> classIDs = new IdentityHashMap();
    public final Map<Object, Integer> objectIDs = new IdentityHashMap();
    public int nextID;
  }

  private static class ReadContext {
    public final Map<Integer, Class> classes = new HashMap();
    public final Map<Integer, Object> objects = new HashMap();
  }

  private interface Serializer {
    public void writeTo(OutputStream out, Object v) throws IOException;
    public Object readFrom(InputStream in) throws IOException;
  }
}

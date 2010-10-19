package com.readytalk.oss.dbms.server;

import com.readytalk.oss.dbms.DBMS;
import com.readytalk.oss.dbms.DBMS.Table;
import com.readytalk.oss.dbms.DBMS.Column;
import com.readytalk.oss.dbms.DBMS.PatchContext;
import com.readytalk.oss.dbms.DBMS.Revision;
import com.readytalk.oss.dbms.DBMS.ConflictResolver;
import com.readytalk.oss.dbms.DBMS.DiffResult;
import com.readytalk.oss.dbms.DBMS.DiffResultType;
import com.readytalk.oss.dbms.DBMS.DuplicateKeyResolution;
import com.readytalk.oss.dbms.imp.BufferOutputStream;

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

  private static volatile Map<Class, Serializer> serializers = new HashMap();
  private static volatile Map<Class, Deserializer> deserializers
    = new HashMap();

  static {
    serializers.put(Integer.class, new Serializer() {
      public void writeTo(WriteContext context, Object v) throws IOException {
        writeInteger(context.out, (Integer) v);
      }
    });

    deserializers.put(Integer.class, new Deserializer() {
      public Object readFrom(ReadContext context, Class c) throws IOException {
        return readInteger(context.in);
      }
    });

    serializers.put(String.class, new Serializer() {
      public void writeTo(WriteContext context, Object v) throws IOException {
        writeString(context.out, (String) v);
      }
    });

    deserializers.put(String.class, new Deserializer() {
      public Object readFrom(ReadContext context, Class c) throws IOException {
        return readString(context.in);
      }
    });

    serializers.put(Class.class, new Serializer() {
      public void writeTo(WriteContext context, Object v) throws IOException {
        write(context, ((Class) v).getName());
      }
    });

    deserializers.put(Class.class, new Deserializer() {
      public Object readFrom(ReadContext context, Class c) throws IOException {
        try {
          return Class.forName((String) read(context));
        } catch (ClassNotFoundException e) {
          throw new RuntimeException(e);
        }
      }
    });

    serializers.put(Writable.class, new Serializer() {
      public void writeTo(WriteContext context, Object v) throws IOException {
        ((Writable) v).writeTo(context.server, context.out);
      }
    });

    deserializers.put(Readable.class, new Deserializer() {
      public Object readFrom(ReadContext context, Class c) throws IOException {
        Readable v;
        try {
          v = (Readable) c.newInstance();
        } catch (InstantiationException e) {
          throw new RuntimeException(e);
        } catch (IllegalAccessException e) {
          throw new RuntimeException(e);
        }
        v.readFrom(context.server, context.in);
        return v;
      }
    });

    serializers.put(Table.class, new Serializer() {
      public void writeTo(WriteContext context, Object v) throws IOException {
        Table t = (Table) v;
        write(context, t.id());
        List<Column> columns = t.primaryKey().columns();
        writeInteger(context.out, columns.size());
        for (Column c: columns) {
          write(context, c);
        }
      }
    });

    deserializers.put(Table.class, new Deserializer() {
      public Object readFrom(ReadContext context, Class c) throws IOException {
        String id = (String) read(context);
        int columnCount = readInteger(context.in);
        List<Column> columns = new ArrayList(columnCount);
        for (int i = 0; i < columnCount; ++i) {
          columns.add((Column) read(context));
        }
        return context.server.dbms.table(id, columns);
      }
    });

    serializers.put(Column.class, new Serializer() {
      public void writeTo(WriteContext context, Object v) throws IOException {
        Column c = (Column) v;
        write(context, c.id());
        write(context, c.type());
      }
    });

    deserializers.put(Column.class, new Deserializer() {
      public Object readFrom(ReadContext context, Class c) throws IOException {
        return context.server.dbms.column
          ((String) read(context), (Class) read(context));
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
  private final NodeConflictResolver conflictResolver;
  private final Network network;
  private final Object lock = new Object();
  private final Map<NodeID, NodeState> states = new HashMap();
  private final Map<NodeID, NodeState> directlyConnectedStates = new HashMap();
  private final NodeState localNode;
  private long nextLocalSequenceNumber = 1;

  public EpidemicServer(DBMS dbms,
                        NodeConflictResolver conflictResolver,
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
         dbms.merge
         (base, localNode.head.revision, fork, new MyConflictResolver
          (localNode.id, localNode.id, conflictResolver)));
    }
  }

  public void accept(NodeID source, Readable message) {
    ((Message) message).deliver(source, this);
  }

  private static void expect(boolean v) {
    if (Debug && ! v) {
      throw new RuntimeException();
    }
  }

  private void send(NodeState state, Writable message) {
    expect(state.connectionState != null);
    expect(state.connectionState.readyToReceive);

    network.send(localNode.id, state.id, message);
  }

  private NodeState state(NodeID node) {
    NodeState state = states.get(node);
    if (state == null) {
      states.put(node, state = new NodeState(node));

      state.head = new Record(node, dbms.revision(), 0, null);

      for (NodeState s: states.values()) {
        s.pending.put(state.id, state.head);
        state.pending.put(s.id, s.head);
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
      send(state, Hello.Instance);
      return;
    }

    if (! cs.gotHello) {
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
      if (lastSent == null) {
        state.connectionState.lastSent.put(target.node, pending);
        return true;
      } else if (lastSent.sequenceNumber < pending.sequenceNumber) {
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
             (target.node, lastSent.sequenceNumber, record.sequenceNumber,
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
    } else {
      state.head = newRecord;
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
           (base, state.head.revision, record.revision, new MyConflictResolver
            (acknowledger, diffOrigin, conflictResolver)), record);

        state.pending.put(diffOrigin, record);
            
        sendNext();
      } else {
        throw new RuntimeException("missed a diff");
      }
    } else {
      // obsolete ack -- ignore
    }
  }

  private static void write(WriteContext context, Object value)
    throws IOException
  {
    Integer id = context.objectIDs.get(value);
    if (id == null) {
      Class c = value.getClass();
      Integer classID = context.classIDs.get(c);
      if (classID == null) {
        int newClassID = context.nextID++;

        context.out.write(ClassDefinition);
        writeInteger(context.out, newClassID);
        writeString(context.out, c.getName());

        context.classIDs.put(c, newClassID);
      } else {
        context.out.write(ClassReference);
        writeInteger(context.out, classID);
      }

      int newID = context.nextID++;
      writeInteger(context.out, newID);
      writeObject(context, value);

      context.objectIDs.put(value, newID);
    } else {
      context.out.write(Reference);
      writeInteger(context.out, id);
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

  private static Class find(Class class_, Map<Class, ?> map) {
    for (Class c = class_; c != Object.class; c = c.getSuperclass()) {
      if (map.containsKey(c)) {
        return c;
      }
    }

    for (Class c: class_.getInterfaces()) {
      if (map.containsKey(c)) {
        return c;
      }
    }

    throw new RuntimeException("no value found for " + class_);
  }

  private static Serializer findSerializer(Class class_) {
    Class c = find(class_, serializers);
    Serializer s = serializers.get(c);
    if (c != class_) {
      synchronized (EpidemicServer.class) {
        Map<Class, Serializer> map = new HashMap(serializers);
        map.put(class_, s);
        serializers = map;
      }
    }
    return s;
  }

  private static Deserializer findDeserializer(Class class_) {
    Class c = find(class_, deserializers);
    Deserializer d = deserializers.get(c);
    if (c != class_) {
      synchronized (EpidemicServer.class) {
        Map<Class, Deserializer> map = new HashMap(deserializers);
        map.put(class_, d);
        deserializers = map;
      }
    }
    return d;
  }

  private static void writeObject(WriteContext context, Object v)
    throws IOException
  {
    findSerializer(v.getClass()).writeTo(context, v);
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

  private static Object readObject(Class c, ReadContext context)
    throws IOException
  {
    return findDeserializer(c).readFrom(context, c);
  }

  private static Object readDefinition(Class c,
                                       ReadContext context)
    throws IOException
  {
    int id = readInteger(context.in);
    Object value = readObject(c, context);
    context.objects.put(id, value);
    return value;
  }

  private static Object read(ReadContext context)
    throws IOException
  {
    InputStream in = context.in;
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
      return readDefinition(c, context);
    }
      
    case ClassReference: {
      int id = readInteger(in);
      Class value = context.classes.get(id);
      if (value == null) {
        throw new NullPointerException();
      }
      return readDefinition(value, context);
    }
      
    case Reference: {
      int id = readInteger(in);
      Object value = context.objects.get(id);
      if (value == null) {
        throw new NullPointerException();
      }
      return value;
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

    public void writeTo(EpidemicServer server, OutputStream out)
      throws IOException
    {
      StreamUtil.writeString(out, acknowledger.id);
      StreamUtil.writeLong(out, acknowledgerSequenceNumber);
      StreamUtil.writeString(out, diffOrigin.id);
      StreamUtil.writeLong(out, diffSequenceNumber);
    }

    public void readFrom(EpidemicServer server, InputStream in)
      throws IOException
    {
      acknowledger = new NodeID(StreamUtil.readString(in));
      acknowledgerSequenceNumber = StreamUtil.readLong(in);
      diffOrigin = new NodeID(StreamUtil.readString(in));
      diffSequenceNumber = StreamUtil.readLong(in);
    }

    public void deliver(NodeID source, EpidemicServer server) {
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

    public void writeTo(EpidemicServer server, OutputStream out)
      throws IOException
    {
      StreamUtil.writeString(out, origin.id);
      StreamUtil.writeLong(out, startSequenceNumber);
      StreamUtil.writeLong(out, endSequenceNumber);
      ((Writable) body).writeTo(server, out);
    }

    public void readFrom(EpidemicServer server, InputStream in)
      throws IOException
    {
      origin = new NodeID(StreamUtil.readString(in));
      startSequenceNumber = StreamUtil.readLong(in);
      endSequenceNumber = StreamUtil.readLong(in);
      BufferDiffBody list = new BufferDiffBody();
      list.readFrom(server, in);
      body = list;
    }

    public void deliver(NodeID source, EpidemicServer server) {
      server.acceptDiff(origin, startSequenceNumber, endSequenceNumber, body);
    }
  }

  private static abstract class Singleton implements Message {
    public void writeTo(EpidemicServer server, OutputStream out) {
      // ignore
    }

    public void readFrom(EpidemicServer server, InputStream in) {
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
    public final DBMS dbms;
    public final Revision base;
    public final Revision fork;

    public RevisionDiffBody(DBMS dbms, Revision base, Revision fork) {
      this.dbms = dbms;
      this.base = base;
      this.fork = fork;
    }

    public Revision apply(EpidemicServer server, Revision base) {
      return fork;
    }

    public void writeTo(EpidemicServer server, OutputStream out)
      throws IOException
    {
      DiffResult result = dbms.diff(base, fork);
      WriteContext writeContext = new WriteContext(out, server);
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
          Object forkKey = result.fork();
          if (forkKey != null) {
            out.write(Key);
            write(writeContext, forkKey);
          } else {
            out.write(Delete);
            write(writeContext, result.base());
            result.skip();
          }
        } break;

        case Value: {
          out.write(Insert);
          write(writeContext, result.fork());
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
      DBMS dbms = server.dbms;
      PatchContext patchContext = dbms.patchContext(base);
      final int MaxDepth = 16;
      Object[] path = new Object[MaxDepth];
      int depth = 0;
      InputStream in = new ByteArrayInputStream
        (buffer.getBuffer(), 0, buffer.size());
      ReadContext readContext = new ReadContext(in, server);

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
            path[depth--] = null;
            break;

          case Key:
            path[depth] = read(readContext);
            break;

          case Delete:
            path[depth] = read(readContext);
            dbms.delete(patchContext, path, 0, depth + 1);
            break;

          case Insert:
            path[depth + 1] = read(readContext);
            dbms.insert(patchContext, DuplicateKeyResolution.Overwrite,
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

    public void readFrom(EpidemicServer server, InputStream in)
      throws IOException
    {
      buffer = new BufferOutputStream();
      ReadContext readContext = new ReadContext(in, server);
      WriteContext writeContext = new WriteContext(buffer, server);
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
          write(writeContext, read(readContext));
          break;

        default:
          throw new RuntimeException("unexpected flag: " + flag);
        }
      }
    }
  }

  public static interface Writable {
    public void writeTo(EpidemicServer server, OutputStream out)
      throws IOException;
  }

  public static interface Readable {
    public void readFrom(EpidemicServer server, InputStream in)
      throws IOException;
  }

  public static interface Network {
    public void send(NodeID source, NodeID destination, Writable message);
  }

  public static class NodeID {
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

  private static class WriteContext {
    public final Map<Class, Integer> classIDs = new IdentityHashMap();
    public final Map<Object, Integer> objectIDs = new IdentityHashMap();
    public final OutputStream out;
    public final EpidemicServer server;
    public int nextID;

    public WriteContext(OutputStream out, EpidemicServer server) {
      this.out = out;
      this.server = server;
    }
  }

  private static class ReadContext {
    public final Map<Integer, Class> classes = new HashMap();
    public final Map<Integer, Object> objects = new HashMap();
    public final InputStream in;
    public final EpidemicServer server;

    public ReadContext(InputStream in, EpidemicServer server) {
      this.in = in;
      this.server = server;
    }
  }

  private interface Serializer {
    public void writeTo(WriteContext context, Object v) throws IOException;
  }

  private interface Deserializer {
    public Object readFrom(ReadContext context, Class c) throws IOException;
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

package com.readytalk.oss.dbms.server;

import com.readytalk.oss.dbms.DBMS;
import com.readytalk.oss.dbms.DBMS.PatchContext;
import com.readytalk.oss.dbms.DBMS.Revision;
import com.readytalk.oss.dbms.DBMS.ConflictResolver;

import java.lang.ref.WeakReference;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.IOException;
import java.util.Map;
import java.util.HashMap;
import java.util.Set;
import java.util.List;
import java.util.ArrayList;
import java.util.Iterator;

public class EpidemicServer {
  private static final boolean Debug = true;

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
              new RevisionDiffBody(lastSent.revision, record.revision)));
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
      ListDiffBody list = new ListDiffBody();
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
    public final Revision base;
    public final Revision fork;

    public RevisionDiffBody(Revision base, Revision fork) {
      this.base = base;
      this.fork = fork;
    }

    public Revision apply(DBMS dbms, Revision base) {
      return fork;
    }

    public void writeTo(OutputStream out) throws IOException {
      // todo
    }
  }

  private static class ListDiffBody implements DiffBody, Readable {
    public final List<Operation> operations = new ArrayList();

    public Revision apply(DBMS dbms, Revision base) {
      PatchContext context = dbms.patchContext(base);
      for (Operation operation: operations) {
        operation.apply(context);
      }
      return dbms.commit(context);
    }

    public void readFrom(InputStream in) throws IOException {
      // todo
    }
  }

  private static interface Operation {
    public void apply(PatchContext context);
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
}
